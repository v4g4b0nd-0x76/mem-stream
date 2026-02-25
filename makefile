run_server:
	@cargo run --bin segmented-memory-storage-server --manifest-path server/Cargo.toml
run_client:
	@cargo run --bin segmented-memory-storage-client --manifest-path client/Cargo.toml