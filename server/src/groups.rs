use crate::seg_log::{LogError, SegLog};
use std::collections::HashMap;
#[derive(Debug)]
pub enum GroupError {
    GroupNotFound(String),
    GroupAlreadyExists(String),
    LogError(LogError),
}
impl std::fmt::Display for GroupError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GroupError::GroupNotFound(name) => write!(f, "group '{}' not found", name),
            GroupError::GroupAlreadyExists(name) => write!(f, "group '{}' already exists", name),
            GroupError::LogError(e) => write!(f, "log error: {}", e),
        }
    }
}
impl From<LogError> for GroupError {
    fn from(e: LogError) -> Self {
        GroupError::LogError(e)
    }
}
pub struct GroupStats {
    pub total_entries: u64,
    pub total_segments: usize,
    pub next_id: u64,
}
pub struct GroupManager {
    groups: HashMap<String, SegLog>,
}

impl GroupManager {
    pub fn new() -> Self {
        GroupManager {
            groups: HashMap::new(),
        }
    }
    pub fn create_group(&mut self, name: &str) -> Result<(), GroupError> {
        if self.groups.contains_key(name) {
            return Err(GroupError::GroupAlreadyExists(name.to_string()));
        }
        let log = SegLog::new();
        self.groups.insert(name.to_string(), log);
        Ok(())
    }
    pub fn drop_group(&mut self, name: &str) -> Result<(), GroupError> {
        if self.groups.remove(name).is_none() {
            return Err(GroupError::GroupNotFound(name.to_string()));
        }
        Ok(())
    }
    pub fn add(&mut self, group: &str, timestamp: u64, payload: &[u8]) -> Result<u64, GroupError> {
        let log = self
            .groups
            .get_mut(group)
            .ok_or_else(|| GroupError::GroupNotFound(group.to_owned()))?;
        Ok(log.append(timestamp, payload)?)
    }

    pub fn add_range(
        &mut self,
        group: &str,
        entries: &[(u64, &[u8])], // (timestamp, payload)
    ) -> Result<(u64, u64), GroupError> {
        let log = self
            .groups
            .get_mut(group)
            .ok_or_else(|| GroupError::GroupNotFound(group.to_owned()))?;

        if entries.is_empty() {
            return Err(GroupError::LogError(LogError::EntryNotFound));
        }

        let first_id = log.append(entries[0].0, entries[0].1)?;
        let mut last_id = first_id;
        for &(ts, payload) in &entries[1..] {
            last_id = log.append(ts, payload)?;
        }
        Ok((first_id, last_id))
    }
    pub fn read(&self, group: &str, id: u64) -> Result<(u64, u64, Vec<u8>), GroupError> {
        let log = self
            .groups
            .get(group)
            .ok_or_else(|| GroupError::GroupNotFound(group.to_owned()))?;
        Ok(log.read(id)?)
    }

    pub fn read_range(
        &self,
        group: &str,
        start: u64,
        end: u64,
    ) -> Result<Vec<(u64, u64, Vec<u8>)>, GroupError> {
        let log = self
            .groups
            .get(group)
            .ok_or_else(|| GroupError::GroupNotFound(group.to_owned()))?;
        Ok(log.read_range(start, end))
    }

    pub fn remove(&mut self, group: &str, up_to_id: u64) -> Result<(), GroupError> {
        let log = self
            .groups
            .get_mut(group)
            .ok_or_else(|| GroupError::GroupNotFound(group.to_owned()))?;
        log.trim(up_to_id);
        Ok(())
    }

    pub fn list_groups(&self) -> Vec<&str> {
        self.groups.keys().map(|s| s.as_str()).collect()
    }

    pub fn group_stats(&self, group: &str) -> Result<GroupStats, GroupError> {
        let log = self
            .groups
            .get(group)
            .ok_or_else(|| GroupError::GroupNotFound(group.to_owned()))?;
        Ok(GroupStats {
            total_entries: log.total_entries(),
            total_segments: log.total_segments(),
            next_id: log.next_id(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_full_cycle() {
        let mut m = GroupManager::new();

        assert!(m.create_group("g1").is_ok());
        assert!(m.create_group("g2").is_ok());
        assert!(m.create_group("g1").is_err());

        let a1 = m.add("g1", 100, b"aaa").unwrap();
        let a2 = m.add("g1", 200, b"bbb").unwrap();
        let a3 = m.add("g1", 300, b"ccc").unwrap();
        assert_eq!(a1, 1);
        assert_eq!(a2, 2);
        assert_eq!(a3, 3);

        let (rid, rts, rdata) = m.read("g1", a2).unwrap();
        assert_eq!(rid, 2);
        assert_eq!(rts, 200);
        assert_eq!(rdata, b"bbb");

        assert!(m.read("g1", 999).is_err());
        assert!(m.add("ghost", 0, b"x").is_err());
        assert!(m.read("ghost", 1).is_err());

        let (first, last) = m
            .add_range("g1", &[(400, b"ddd"), (500, b"eee"), (600, b"fff")])
            .unwrap();
        assert_eq!(first, 4);
        assert_eq!(last, 6);

        let range = m.read_range("g1", 2, 5).unwrap();
        assert_eq!(range.len(), 4);
        assert_eq!(range[0].0, 2);
        assert_eq!(range[3].0, 5);

        assert!(m.read_range("g1", 900, 999).unwrap().is_empty());

        let b1 = m.add("g2", 10, b"isolated").unwrap();
        assert_eq!(b1, 1);
        let (_, _, d) = m.read("g2", 1).unwrap();
        assert_eq!(d, b"isolated");

        m.remove("g1", 4).unwrap();
        assert!(m.read("g1", 1).is_err());
        assert!(m.read("g1", 2).is_err());
        assert!(m.read("g1", 3).is_err());
        assert!(m.read("g1", 4).is_ok());
        assert!(m.read("g1", 6).is_ok());

        let (_, _, kept) = m.read("g2", 1).unwrap();
        assert_eq!(kept, b"isolated");

        let stats = m.group_stats("g1").unwrap();
        assert_eq!(stats.total_entries, 3);
        assert_eq!(stats.next_id, 7);

        let mut groups = m.list_groups();
        groups.sort();
        assert_eq!(groups, vec!["g1", "g2"]);

        m.drop_group("g2").unwrap();
        assert!(m.drop_group("g2").is_err());
        assert!(m.read("g2", 1).is_err());
        assert_eq!(m.list_groups().len(), 1);

        let post = m.add("g1", 700, b"after-trim").unwrap();
        assert_eq!(post, 7);
        let (_, _, pd) = m.read("g1", 7).unwrap();
        assert_eq!(pd, b"after-trim");
    }
}
