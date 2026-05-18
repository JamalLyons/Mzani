default:
    just --list

setup:
    rustup override set nightly
    cargo build

run:
    cargo run -- 127.0.0.1:5000

run-mock-test:
    #!/usr/bin/env bash
    bun run mock/server.ts 3333 & PID1=$!
    bun run mock/server.ts 3334 & PID2=$!

    trap "kill $PID1 $PID2" EXIT

    sleep 0.5

    bun run mock/script.ts

build:
    cargo build --release

serve:
    cargo run --release -- 127.0.0.1:5000

delete-logs:
    rm -f logs/*.log

fmt:
    cargo fmt

fmt-check:
    cargo fmt --check

lint:
    cargo clippy --all-targets -- -D warnings

test:
    cargo test

doc:
    cargo doc --no-deps

check: fmt-check lint test
    cargo build --all-targets
