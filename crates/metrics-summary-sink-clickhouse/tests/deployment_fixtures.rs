//! Published crate fixtures remain usable without the workspace deployment tree.
#[test]
fn packaged_sql_matches_workspace_deployment_templates() {
    for (name, packaged) in [
        ("schema.sql", include_str!("fixtures/schema.sql")),
        ("queries.sql", include_str!("fixtures/queries.sql")),
        ("retention.sql", include_str!("fixtures/retention.sql")),
    ] {
        let deployed = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../deploy/clickhouse")
            .join(name);
        if deployed.is_file() {
            assert_eq!(
                packaged,
                std::fs::read_to_string(deployed).unwrap(),
                "fixture drift: {name}"
            );
        }
    }
}
