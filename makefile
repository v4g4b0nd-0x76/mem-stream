.PHONY:  server client clean run_server run_client
SERVER_PID = /tmp/segmented-memory-storage-server.pid
CLIENT_PID = /tmp/segmented-memory-storage-client.pid
run_server:
	@cargo run --bin segmented-memory-storage-server --manifest-path server/Cargo.toml
run_client:
	@cargo run --bin segmented-memory-storage-client --manifest-path client/Cargo.toml

	
server:
	cargo build --release --manifest-path server/Cargo.toml --target-dir=bins
	./bins/release/segmented-memory-storage-server &
	echo $$! > $(SERVER_PID)
	sleep 1

client:
	cargo build --release --manifest-path client/Cargo.toml --target-dir=bins
	SERVER_ADDR=127.0.0.1:9090 HTTP_ADDR=127.0.0.1:8000 POOL_SIZE=16 \
		./bins/release/segmented-memory-storage-client &
	echo $$! > $(CLIENT_PID)
	sleep 1

clean:
	-kill $$(cat $(SERVER_PID) 2>/dev/null) 2>/dev/null || true
	-kill $$(cat $(CLIENT_PID) 2>/dev/null) 2>/dev/null || true
	-rm -f $(SERVER_PID) $(CLIENT_PID)
