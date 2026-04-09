SERVER_DIR := rust/server

.PHONY: test test-server test-python test-differential bench bench-quick bench-size bench-runtime bench-memory bench-storage bench-baseline bench-compare

test: test-server

test-server:
	cargo test --manifest-path $(SERVER_DIR)/Cargo.toml

test-python:
	bash scripts/run-python-compat.sh

test-differential:
	bash scripts/run-differential.sh

bench:
	bash scripts/run-bench.sh full

bench-quick:
	bash scripts/run-bench.sh quick

bench-size:
	bash scripts/run-bench.sh size

bench-runtime:
	bash scripts/run-bench.sh runtime

bench-memory:
	bash scripts/run-bench.sh memory

bench-storage:
	bash scripts/run-bench.sh storage

bench-baseline:
	bash scripts/run-bench.sh baseline

bench-compare:
	bash scripts/compare-bench.sh $(BASE) $(NEW)
