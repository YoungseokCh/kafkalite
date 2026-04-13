SERVER_DIR := rust/server

.PHONY: test test-server test-python test-differential fmt clippy verify publish-dry-run publish-dry-run-dirty bench bench-runtime bench-compare

test: test-server

test-server:
	cargo test --manifest-path $(SERVER_DIR)/Cargo.toml

fmt:
	cargo fmt --manifest-path $(SERVER_DIR)/Cargo.toml --check

clippy:
	cargo clippy --manifest-path $(SERVER_DIR)/Cargo.toml --all-targets --all-features -- -D warnings

verify: fmt clippy test

publish-dry-run:
	cargo publish --manifest-path $(SERVER_DIR)/Cargo.toml --dry-run

publish-dry-run-dirty:
	cargo publish --manifest-path $(SERVER_DIR)/Cargo.toml --dry-run --allow-dirty

test-python:
	bash scripts/run-python-compat.sh

test-differential:
	bash scripts/run-differential.sh

bench:
	bash scripts/run-bench.sh full "$(LABEL)"

bench-runtime:
	bash scripts/run-bench.sh runtime "$(LABEL)"

bench-compare:
	bash scripts/compare-bench.sh "$(BASE)" "$(NEW)" "$(OUTPUT)"
