# Contributing to mzani

Thank you for your interest in contributing.

## Development setup

1. Install Rust 1.85 or newer (`rustup update stable`).
2. Clone the repository and run:

```bash
cargo fmt
cargo clippy --all-targets -- -D warnings
cargo test
```

## Guidelines

- The library crate must remain **stdlib-only** (no new dependencies in `[dependencies]`).
- No `unsafe` code.
- Document all public items.
- Add tests for behavior changes.

## Pull requests

- Keep changes focused.
- Update `CHANGELOG.md` under `Unreleased` for user-visible changes.
- Ensure CI passes before requesting review.
