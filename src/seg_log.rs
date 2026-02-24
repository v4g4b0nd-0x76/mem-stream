use std::{
    alloc::{Layout, alloc_zeroed, dealloc},
    collections::BTreeMap,
    ptr,
};

const SEGMENT_SIZE: usize = 64 * 1024; // 64KB per segment for better cpu cache
const SEGMENT_ALIGN: usize = 4096; // 4Kb for align to os page boundary and better direct mem access
const INITIAL_SEGMENTS: usize = 8; // pre allocate 8 segments 
const MAX_SEGMENTS: usize = 1024 * 16; // max 1GB log size

#[derive(Debug, Clone, Copy)]
struct EntryLoc {
    segment_idx: usize, // segment index in pointer vector
    offset: usize,      // offset in segment
    len: usize,         // total occupied bytes in this entry
}

const HEADER_SIZE: usize = 8 + 8 + 4; // 20 bytes fixed header
pub struct SegLog {
    segs: Vec<*mut u8>, // vector of pointers to segments
    active_seg: usize,
    write_cursor: usize,
    idx: BTreeMap<u64, EntryLoc>, // map from entry id to its location
    next_id: u64,                 // next entry id to assign
    entry_count: usize,           // total number of entries in the log
    seg_layout: Layout,           // layout for segment allocation
}
unsafe impl Send for SegLog {}
unsafe impl Sync for SegLog {}

impl SegLog {
    pub fn new() -> Self {
        let seg_layout = Layout::from_size_align(SEGMENT_SIZE, SEGMENT_ALIGN).unwrap();
        // pre alloc segmetns
        let mut segs = Vec::with_capacity(MAX_SEGMENTS);
        for _ in 0..INITIAL_SEGMENTS {
            let ptr = unsafe { alloc_zeroed(seg_layout) };
            if ptr.is_null() {
                panic!("Failed to allocate memory for segment");
            }
            segs.push(ptr);
        }
        SegLog {
            segs,
            active_seg: 0,
            write_cursor: 0,
            seg_layout,
            idx: BTreeMap::new(),
            next_id: 1,
            entry_count: 0,
        }
    }

    pub fn allocate_seg(&mut self) -> Result<(), LogError> {
        if self.segs.len() >= MAX_SEGMENTS {
            return Err(LogError::MaxSegmentsReached);
        }
        let ptr = unsafe { alloc_zeroed(self.seg_layout) };
        if ptr.is_null() {
            return Err(LogError::AllocationFailed);
        }
        self.segs.push(ptr);
        Ok(())
    }

    pub fn append(&mut self, timestamp: u64, payload: &[u8]) -> Result<u64, LogError> {
        let total_entry_size = HEADER_SIZE + payload.len();
        if total_entry_size > SEGMENT_SIZE {
            return Err(LogError::EntryTooLarge);
        }
        // check if current segment has enough space, and if not mark as full and move to next segment //TODO: use this segment free space later by implementing a page map for each segment
        if self.write_cursor + total_entry_size > SEGMENT_SIZE {
            self.active_seg += 1;
            self.write_cursor = 0;
            if self.active_seg >= self.segs.len() {
                self.allocate_seg()?;
            }
        }
        let entry_id = self.next_id;
        let base: *mut u8 = self.segs[self.active_seg];
        let dst: *mut u8 = unsafe { base.add(self.write_cursor) };
        unsafe {
            let id_bytes = entry_id.to_le_bytes();
            ptr::copy_nonoverlapping(id_bytes.as_ptr(), dst, 8);
            let ts_bytes = timestamp.to_le_bytes();
            ptr::copy_nonoverlapping(ts_bytes.as_ptr(), dst.add(8), 8);
            let len_bytes = (payload.len() as u32).to_le_bytes();
            ptr::copy_nonoverlapping(len_bytes.as_ptr(), dst.add(16), 4);
            if !payload.is_empty() {
                ptr::copy_nonoverlapping(payload.as_ptr(), dst.add(HEADER_SIZE), payload.len());
            }
        }
        self.idx.insert(
            entry_id,
            EntryLoc {
                segment_idx: self.active_seg,
                offset: self.write_cursor,
                len: total_entry_size,
            },
        );
        self.write_cursor += total_entry_size;
        self.next_id += 1;
        self.entry_count += 1;
        Ok(entry_id)
    }

    pub fn read(&self, entry_id: u64) -> Result<(u64, u64, Vec<u8>), LogError> {
        let loc = self.idx.get(&entry_id).ok_or(LogError::EntryNotFound)?;
        let base: *const u8 = self.segs[loc.segment_idx];
        let src: *const u8 = unsafe { base.add(loc.offset) };
        unsafe {
            let mut id_buf = [0u8; 8];
            ptr::copy_nonoverlapping(src, id_buf.as_mut_ptr(), 8);
            let id = u64::from_le_bytes(id_buf);
            let mut ts_buf = [0u8; 8];
            ptr::copy_nonoverlapping(src.add(8), ts_buf.as_mut_ptr(), 8);
            let timestamp = u64::from_le_bytes(ts_buf);
            let mut len_buf = [0u8; 4];
            ptr::copy_nonoverlapping(src.add(16), len_buf.as_mut_ptr(), 4);
            let payload_len = u32::from_le_bytes(len_buf) as usize;
            let mut payload = vec![0u8; payload_len];
            if payload_len > 0 {
                ptr::copy_nonoverlapping(src.add(HEADER_SIZE), payload.as_mut_ptr(), payload_len);
            }

            Ok((id, timestamp, payload))
        }
    }
    pub fn read_range(&self, start: u64, end: u64) -> Vec<(u64, u64, Vec<u8>)> {
        let mut results = Vec::new();

        for (&id, _loc) in self.idx.range(start..=end) {
            if let Ok(entry) = self.read(id) {
                results.push(entry);
            }
        }

        results
    }

    pub fn trim(&mut self, cutoff_id: u64) {
        let to_remove: Vec<u64> = self.idx.range(..cutoff_id).map(|(&id, _)| id).collect();

        for id in &to_remove {
            self.idx.remove(id);
            self.entry_count -= 1;
        }

        if let Some((_first_id, first_loc)) = self.idx.iter().next() {
            let first_live_segment = first_loc.segment_idx;

            for seg_idx in 0..first_live_segment {
                unsafe {
                    ptr::write_bytes(self.segs[seg_idx], 0, SEGMENT_SIZE);
                }
            }
        }
    }

    pub fn get_raw_entry_slice(&self, entry_id: u64) -> Result<&[u8], &'static str> {
        let loc = self.idx.get(&entry_id).ok_or("entry not found")?;
        let base: *const u8 = self.segs[loc.segment_idx];

        unsafe { Ok(std::slice::from_raw_parts(base.add(loc.offset), loc.len)) }
    }
    pub fn total_entries(&self) -> u64 {
        self.entry_count as u64
    }

    pub fn total_segments(&self) -> usize {
        self.segs.len()
    }

    pub fn active_segment_usage(&self) -> f64 {
        // return percentage of active segment used
        (self.write_cursor as f64 / SEGMENT_SIZE as f64) * 100.0
    }
    pub fn next_id(&self) -> u64 {
        self.next_id
    }
}
impl Drop for SegLog {
    fn drop(&mut self) {
        for &ptr in &self.segs {
            if !ptr.is_null() {
                unsafe {
                    dealloc(ptr, self.seg_layout);
                }
            }
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_full_lifecycle() {
        let mut log = SegLog::new();

        let entry_count = 1000u64;
        for i in 0..entry_count {
            let ts = 1700000000000 + i * 50;
            let payload = format!(
                "sensor:temp-{} value:{}.{} unit:c",
                i % 10,
                20 + (i % 15),
                i % 100
            );
            log.append(ts, payload.as_bytes()).unwrap();
        }
        assert_eq!(log.total_entries(), entry_count);

        let samples = [1u64, 50, 250, 500, 750, 999, 1000];
        for &id in &samples {
            let (read_id, read_ts, data) = log.read(id).unwrap();
            assert_eq!(read_id, id);
            let i = id - 1;
            assert_eq!(read_ts, 1700000000000 + i * 50);
            let expected = format!(
                "sensor:temp-{} value:{}.{} unit:c",
                i % 10,
                20 + (i % 15),
                i % 100
            );
            assert_eq!(data, expected.as_bytes(), "payload mismatch at id={}", id);
        }

        let range = log.read_range(100, 109);
        assert_eq!(range.len(), 10);
        for (idx, (id, _, _)) in range.iter().enumerate() {
            assert_eq!(*id, 100 + idx as u64);
        }

        log.trim(501);
        assert_eq!(log.total_entries(), 500);
        assert!(log.read(1).is_err(), "trimmed entry must be gone");
        assert!(log.read(500).is_err(), "trimmed entry must be gone");
        assert!(
            log.read(501).is_ok(),
            "entry at cutoff boundary must survive"
        );

        let new_id = log.append(9999999999999, b"post-trim-event").unwrap();
        assert_eq!(
            new_id,
            entry_count + 1,
            "ID counter must continue, not reset after trim"
        );
        let (_, _, data) = log.read(new_id).unwrap();
        assert_eq!(data, b"post-trim-event");
    }

    #[test]
    fn test_segment_boundary_integrity() {
        let mut log = SegLog::new();

        let payload_size = 1000;
        let count = 500u64;

        for i in 0..count {
            let fill_byte = (i & 0xFF) as u8;
            let payload = vec![fill_byte; payload_size];
            log.append(i, &payload).unwrap();
        }

        assert!(
            log.total_segments() > 1,
            "test must span multiple segments to be meaningful"
        );

        for id in 1..=count {
            let (_, ts, data) = log.read(id).unwrap();
            let i = id - 1;
            assert_eq!(ts, i);
            assert_eq!(data.len(), payload_size);
            let expected_byte = (i & 0xFF) as u8;
            assert!(
                data.iter().all(|&b| b == expected_byte),
                "corruption at id={} (segment boundary?), expected 0x{:02X}, got 0x{:02X} at first mismatch",
                id,
                expected_byte,
                data.iter()
                    .find(|&&b| b != expected_byte)
                    .copied()
                    .unwrap_or(0)
            );
        }
    }

    #[test]
    fn test_wire_format_and_zero_copy() {
        let mut log = SegLog::new();
        let ts: u64 = 0xAABB_CCDD_1122_3344;
        let payload = b"wire-fmt";
        let id = log.append(ts, payload).unwrap();

        let raw = log.get_raw_entry_slice(id).unwrap();
        assert_eq!(raw.len(), HEADER_SIZE + payload.len());

        let raw_id = u64::from_le_bytes(raw[0..8].try_into().unwrap());
        let raw_ts = u64::from_le_bytes(raw[8..16].try_into().unwrap());
        let raw_len = u32::from_le_bytes(raw[16..20].try_into().unwrap()) as usize;
        let raw_payload = &raw[HEADER_SIZE..HEADER_SIZE + raw_len];

        let (s_id, s_ts, s_payload) = log.read(id).unwrap();
        assert_eq!(raw_id, s_id);
        assert_eq!(raw_ts, s_ts);
        assert_eq!(raw_ts, ts, "timestamp must survive LE roundtrip exactly");
        assert_eq!(raw_payload, s_payload.as_slice());
        assert_eq!(raw_payload, payload);
    }

    #[test]
    fn test_edge_cases() {
        let mut log = SegLog::new();

        let id1 = log.append(0, b"").unwrap();
        let (_, _, d) = log.read(id1).unwrap();
        assert!(d.is_empty());

        let max_payload = vec![0xFFu8; SEGMENT_SIZE - HEADER_SIZE];
        let id2 = log.append(u64::MAX, &max_payload).unwrap();
        let (_, ts, d) = log.read(id2).unwrap();
        assert_eq!(ts, u64::MAX);
        assert_eq!(d.len(), SEGMENT_SIZE - HEADER_SIZE);

        let oversized = vec![0u8; SEGMENT_SIZE - HEADER_SIZE + 1];
        assert!(log.append(1, &oversized).is_err());

        assert!(log.read(0).is_err());
        assert!(log.read(9999).is_err());
        assert!(log.get_raw_entry_slice(9999).is_err());

        assert!(log.read_range(5000, 6000).is_empty());
    }

    #[test]
    fn test_drop_safety() {
        drop(SegLog::new());

        let mut log = SegLog::new();
        for _ in 0..300 {
            log.append(1, &vec![0u8; 2000]).unwrap();
        }
        log.trim(200);
        drop(log);
    }
}

#[derive(Debug)]
pub enum LogError {
    EntryTooLarge,
    SegmentLimitReached,
    EntryNotFound,
    AllocationFailed,
    MaxSegmentsReached,
}
impl std::fmt::Display for LogError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LogError::EntryTooLarge => write!(f, "entry exceeds segment capacity"),
            LogError::SegmentLimitReached => write!(f, "max segment count reached"),
            LogError::EntryNotFound => write!(f, "entry not found"),
            LogError::AllocationFailed => write!(f, "failed to allocate memory for segment"),
            LogError::MaxSegmentsReached => write!(f, "maximum segments reached"),
        }
    }
}
