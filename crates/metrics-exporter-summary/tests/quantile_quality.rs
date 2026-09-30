//! Empirical acceptance checks, not a distribution-independent t-digest guarantee.
//! The oracle uses the empirical CDF interval at the estimate: ties cover a rank
//! interval instead of incorrectly requiring one particular tie-breaking rank.

use metrics_exporter_summary::{metrics, Builder, Config, MetricValue};
use metrics_summary_sink_memory::{MemorySink, Retention};
use std::{thread, time::Duration};

fn datasets() -> Vec<(&'static str, Vec<f64>)> {
    let mut seed = 0x5eed_u64;
    let random = (0..10_000)
        .map(|_| {
            seed = seed
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (seed >> 11) as f64 / ((1_u64 << 53) as f64)
        })
        .collect();
    vec![
        ("constant", vec![17.0; 10_000]),
        ("repeated", (0..10_000).map(|i| (i % 23) as f64).collect()),
        ("monotonic", (0..10_000).map(|i| i as f64).collect()),
        ("random", random),
        (
            "bimodal",
            (0..10_000)
                .map(|i| {
                    if i % 2 == 0 {
                        -10.0 + (i % 101) as f64 / 100.0
                    } else {
                        10.0 + (i % 101) as f64 / 100.0
                    }
                })
                .collect(),
        ),
        (
            "long_tail",
            (1..=10_000)
                .map(|i| -((i as f64) / 10_001.0).ln())
                .collect(),
        ),
        (
            "extreme_outliers",
            (0..10_000)
                .map(|i| if i < 9990 { 0.0 } else { 1e12 })
                .collect(),
        ),
        ("one_sample", vec![-7.0]),
        ("two_samples", vec![-3.0, 99.0]),
        (
            "eleven_samples",
            vec![
                -3.0, -1.0, 0.0, 0.0, 1.0, 2.0, 4.0, 6.0, 10.0, 100.0, 1000.0,
            ],
        ),
    ]
}

fn rank_error(sorted: &[f64], estimate: f64, quantile: f64) -> f64 {
    let less = sorted.partition_point(|value| *value < estimate) as f64 / sorted.len() as f64;
    let at_most = sorted.partition_point(|value| *value <= estimate) as f64 / sorted.len() as f64;
    if quantile < less {
        less - quantile
    } else if quantile > at_most {
        quantile - at_most
    } else {
        0.0
    }
}

fn collect(samples: &[f64], threads: usize) -> MetricValue {
    let (sink, reader) = MemorySink::new(Retention::default()).unwrap();
    let (recorder, control) = Builder::for_service("quality", "test")
        .unwrap()
        .config(Config {
            collect_interval: None,
            digest_compression: 100,
            buffer_capacity: 32,
            ..Default::default()
        })
        .build(sink)
        .unwrap();
    let histogram = metrics::with_local_recorder(&recorder, || metrics::histogram!("observations"));
    thread::scope(|scope| {
        for lane in 0..threads {
            let histogram = histogram.clone();
            scope.spawn(move || {
                for sample in samples.iter().skip(lane).step_by(threads) {
                    histogram.record(*sample);
                }
            });
        }
    });
    let report = control.shutdown(Duration::from_secs(10)).unwrap();
    assert!(report.is_success(), "{report:?}");
    let batch = reader.get(report.target).unwrap();
    assert_eq!(batch.rows.len(), 1);
    batch.rows[0].value.clone()
}

#[test]
fn fixed_cdf_oracle_covers_ties_tails_low_counts_and_thread_merges() {
    for (name, samples) in datasets() {
        let mut sorted = samples.clone();
        sorted.sort_by(f64::total_cmp);
        let reference_sum: f64 = samples.iter().sum();
        for threads in [1, 8] {
            let value = collect(&samples, threads);
            let MetricValue::HistogramSummary {
                count,
                sum,
                min,
                p50,
                p90,
                p95,
                p99,
                max,
            } = value
            else {
                panic!("wrong instrument")
            };
            assert_eq!(count, samples.len() as u64, "{name}, {threads} threads");
            assert_eq!(min, sorted[0], "{name}, {threads} threads");
            assert_eq!(max, *sorted.last().unwrap(), "{name}, {threads} threads");
            assert!(
                (sum - reference_sum).abs() <= reference_sum.abs().max(1.0) * 1e-12,
                "sum {name}, {threads}: {sum} != {reference_sum}"
            );
            for (q, estimate) in [(0.5, p50), (0.9, p90), (0.95, p95), (0.99, p99)] {
                check_quantile(
                    name,
                    &format!("recorder_{threads}_threads"),
                    &sorted,
                    q,
                    estimate,
                );
            }
        }
    }
}

fn check_quantile(name: &str, profile: &str, sorted: &[f64], q: f64, estimate: f64) {
    let exact = sorted[((sorted.len() as f64 * q).ceil() as usize).saturating_sub(1)];
    let error = rank_error(sorted, estimate, q);
    let absolute = (estimate - exact).abs();
    // A continuous estimate near an integer can cross one full 1/23 CDF
    // atom. This specific quantized dataset additionally requires rounding to
    // recover the exact value; exported estimates themselves are never rounded.
    let tolerance = if name == "repeated" {
        assert!(
            absolute < 0.5,
            "{name}, {profile}, q={q}: {estimate} vs {exact}"
        );
        assert_eq!(estimate.round(), exact);
        1.0 / 23.0 + 1.0 / sorted.len() as f64
    } else {
        0.015 + 1.0 / sorted.len() as f64
    };
    assert!(
        error <= tolerance,
        "{name}, {profile}, q={q}: value={estimate}, rank_error={error}, tolerance={tolerance}"
    );
    assert!(estimate >= sorted[0] && estimate <= *sorted.last().unwrap());
    if name == "constant" || name == "one_sample" {
        assert_eq!(estimate, sorted[0]);
    }
    if std::env::var_os("SUMMARY_QUANTILE_REPORT").is_some() {
        println!(
            "{}",
            serde_json::json!({
                "dataset": name, "profile": profile, "samples": sorted.len(),
                "quantile": q, "exact_empirical_quantile": exact, "estimate": estimate,
                "absolute_error": absolute,
                "relative_error": if exact == 0.0 { None } else { Some(absolute / exact.abs()) },
                "rank_error": error, "rank_tolerance": tolerance,
            })
        );
    }
}

#[test]
fn explicit_digest_merge_orders_obey_the_same_empirical_oracle() {
    // The recorder's registry iteration order is unspecified. Independently
    // exercise its pinned sketch's merge operation with explicit permutations.
    for (name, samples) in datasets() {
        let mut sorted = samples.clone();
        sorted.sort_by(f64::total_cmp);
        let digests: Vec<_> = (0..8)
            .map(|lane| {
                let values: Vec<_> = samples.iter().skip(lane).step_by(8).copied().collect();
                let mut digest = tdigest::TDigest::new_with_size(100);
                for chunk in values.chunks(32) {
                    digest = digest.merge_unsorted(chunk.to_vec());
                }
                digest
            })
            .collect();
        for (profile, order) in [
            ("digest_forward", [0, 1, 2, 3, 4, 5, 6, 7]),
            ("digest_reverse", [7, 6, 5, 4, 3, 2, 1, 0]),
            ("digest_permuted", [3, 0, 6, 1, 7, 4, 2, 5]),
        ] {
            let mut merged = tdigest::TDigest::merge_digests(
                order.into_iter().map(|i| digests[i].clone()).collect(),
            );
            merged.flush();
            assert_eq!(merged.count(), samples.len() as f64);
            assert_eq!(merged.max(), sorted.last().copied());
            for q in [0.5, 0.9, 0.95, 0.99] {
                check_quantile(
                    name,
                    profile,
                    &sorted,
                    q,
                    merged.estimate_quantile(q).unwrap(),
                );
            }
        }
    }
}

#[test]
fn weighted_record_many_matches_explicit_observations() {
    let (sink, reader) = MemorySink::new(Retention::default()).unwrap();
    let (recorder, control) = Builder::for_service("quality", "weighted")
        .unwrap()
        .config(Config {
            collect_interval: None,
            buffer_capacity: 16,
            ..Default::default()
        })
        .build(sink)
        .unwrap();
    metrics::with_local_recorder(&recorder, || {
        let weighted = metrics::histogram!("weighted");
        let explicit = metrics::histogram!("explicit");
        for (value, count) in [(-5.0, 7), (0.0, 0), (2.0, 99), (100.0, 1000), (10_000.0, 1)] {
            weighted.record_many(value, count);
            for _ in 0..count {
                explicit.record(value);
            }
        }
    });
    let report = control.shutdown(Duration::from_secs(10)).unwrap();
    assert!(report.is_success());
    let batch = reader.get(report.target).unwrap();
    assert_eq!(batch.rows.len(), 2);
    assert_eq!(batch.rows[0].value, batch.rows[1].value);
}
