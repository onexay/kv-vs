# Repository Guidelines

## Project Structure & Module Organization
The repository tracks experiments comparing key-value storage engines in Rust. Place the shared abstractions and core orchestration modules in `src/kv_vs/`, splitting submodules by domain (`engine`, `cli`, `metrics`). Each backend implementation receives its own folder under `engines/<backend_name>/` with a local README describing dependencies and configuration knobs. Integration resources (fixtures, benchmark configs) belong in `data/` and `benchmarks/`, while long-form design notes live in `docs/`.

## Build, Test, and Development Commands
Use `cargo check` for quick validation before opening a PR. Run `cargo fmt` to apply the repository style, then `cargo clippy --all-targets --all-features` and treat warnings as failures. Execute `cargo test --all-features` to run unit and integration suites; add `-- --nocapture` when debugging. Benchmark runs use `cargo bench` or the CLI: `cargo run --bin kvvs-cli -- --engine rocksdb --dataset data/smoke.csv`.

## Coding Style & Naming Conventions
Follow default Rustfmt settings (4-space indent, trailing commas where allowed). Modules, files, and crates use `snake_case`; types and traits stay in `UpperCamelCase`; constants in `SCREAMING_SNAKE_CASE`. Favor explicit `#[derive]` implementations over manual trait impls to keep parity between engines. Keep public APIs narrowly scoped and document any unsafe blocks.

## Testing Guidelines
Write unit tests alongside source files under `#[cfg(test)]` modules for fast feedback. Place cross-engine integration tests in `tests/` with filenames mirroring the scenario, e.g., `tests/compaction_smoke.rs`. Stretch goals or slow benchmarks belong under `benches/` with Criterion harnesses. Aim for coverage on all persistence paths; add reproduction cases for regressions before fixing them.

## Commit & Pull Request Guidelines
Use Conventional Commits (`feat:`, `fix:`, `perf:`, `chore:`) to keep the history searchable; prefix backend-specific work with the backend name (`feat(rocksdb):`). Squash commits that are purely fixup before merging. PRs should include a concise summary, linked issue or ticket, and test evidence (`cargo test`, `cargo bench` excerpt). Include configuration notes or benchmark diffs when altering storage defaults.
