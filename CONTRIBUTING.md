# Contributing

Thanks for your interest in contributing.

## Development

Requirements:

- Rust (stable)

Common commands:

```bash
cargo fmt
cargo test
cargo clippy --all-targets --all-features -- -D warnings
```

## Pull Requests

- Keep changes focused and minimal.
- Add tests for behavior changes and regressions.
- Ensure CI passes (`cargo fmt`, `cargo clippy`, `cargo test`, `cargo audit`).
- Prefer clear error messages and predictable CLI behavior.
