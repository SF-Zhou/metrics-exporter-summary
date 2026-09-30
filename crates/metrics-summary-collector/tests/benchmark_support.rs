#[test]
fn packaged_capacity_workloads_match_when_the_workspace_is_present() {
    // Each package contains its own helper so independently published tarballs
    // can compile every target. In the workspace their benchmark logic must stay
    // byte-identical; changing only one copy would invalidate backend comparisons.
    let peer = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../metrics-exporter-summary/benches/support/capacity_workload.rs");
    if let Ok(peer) = std::fs::read_to_string(peer) {
        assert_eq!(
            peer,
            include_str!("../benches/support/capacity_workload.rs")
        );
    }
}
