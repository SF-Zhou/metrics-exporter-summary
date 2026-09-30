# Releasing the workspace

The seven crates use one version in `[workspace.package]`; update the matching
versions in `[workspace.dependencies]` together when preparing a release. Library
consumers require Rust 1.95 or newer. Use the current stable Cargo for workspace
packaging and publishing.

Application-to-collector requests and ACKs use the defined MessagePack wire
version 1 layout, with `/v1/batches` for HTTP and the `MXS1` TCP handshake.
Verify that deployed sender and collector builds use matching protocol definitions.

Start from the reviewed commit with a clean working tree. `Cargo.lock` is not
tracked; generate it locally, then keep that same resolution for all checks and
the publish command. Retain a copy with the release artifacts so dependency
versions can be reproduced later.

```sh
cargo +1.95.0 generate-lockfile
cargo fmt --all -- --check
cargo test --workspace --all-features --locked
cargo +1.95.0 test --workspace --all-features --locked
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
RUSTDOCFLAGS="-Dwarnings -Dmissing_docs" cargo doc --workspace --all-features --no-deps --locked
cargo test --workspace --all-features --doc --locked
cargo +stable package --workspace --all-features --locked --registry crates-io
cargo +stable publish --workspace --all-features --locked --registry crates-io --dry-run
```

Run the optional transport test matrix from `.github/workflows/ci.yml` as well.
Those tests include actual TOML configuration and TCP service startup, rather
than only checking that feature combinations compile. For storage or dashboard
changes, run the disposable ClickHouse and Grafana checks described in their
deployment guides. CI verifies the workspace archives by unpacking and building
each crate; a file listing alone does not provide that check.

All seven packages configure docs.rs to build every feature. The remote and
collector API pages annotate HTTP/TCP requirements using `doc(cfg)`; this
documentation-only setting uses docs.rs's nightly toolchain. CI also checks that
build mode. Using the lockfile generated above, reproduce it locally with:

```sh
RUSTDOCFLAGS="--cfg docsrs -Dwarnings -Dmissing_docs" cargo +nightly doc --workspace --all-features --no-deps --locked
```

The examples compile as doctests; in-memory examples run without external
services, while network examples use `no_run`. Inspect the generated remote
`Endpoint` and collector `serve_http`/`serve_tcp` pages for feature badges before
publishing. The docs.rs service builds the uploaded crate versions; changing
local documentation alone does not update the public site.

Inspect the generated `target/package/*.crate` files for licenses, README,
configuration metadata and the expected VCS revision. A local registry mirror
can interfere with resolving workspace packages that have not been published
yet; use the official registry without source replacement for release checks.

After review and an explicit decision to publish, authenticate with crates.io
and use the same workspace command without `--dry-run`:

```sh
cargo +stable publish --workspace --all-features --locked --registry crates-io
```

Cargo handles the dependency ordering. Record the release version and commit,
and verify the published versions and their documentation before announcing
the release. See the official [package command](https://doc.rust-lang.org/cargo/commands/cargo-package.html)
and [publish command](https://doc.rust-lang.org/cargo/commands/cargo-publish.html)
for the archive checks and dry-run behavior.
