.PHONY: all test build run unit-test fmt lint coverage check-llvm-cov

all: run

check-llvm-cov:
	@command -v cargo-llvm-cov >/dev/null 2>&1 || { \
		echo "cargo-llvm-cov is required for 'make test'"; \
		echo "install it with: cargo install cargo-llvm-cov"; \
		exit 1; \
	}

test: check-llvm-cov
	cargo llvm-cov --summary-only

build:
	cargo build

run:
	cargo run --release

unit-test:
	cargo test

fmt:
	cargo fmt --all

lint:
	cargo clippy --all-targets -- -D warnings

coverage: check-llvm-cov
	cargo llvm-cov --summary-only
