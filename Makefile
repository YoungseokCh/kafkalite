SERVER_DIR := rust/server
CLIENT_DIR := rust/client

.PHONY: test test-server test-client test-python test-differential

test: test-server test-client

test-server:
	cargo test --manifest-path $(SERVER_DIR)/Cargo.toml

test-client:
	cargo test --manifest-path $(CLIENT_DIR)/Cargo.toml

test-python:
	bash scripts/run-python-compat.sh

test-differential:
	bash scripts/run-differential.sh
