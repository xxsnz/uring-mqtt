//! Public-seam tests for `bench::record::Recorder` (task 2 of the
//! benchmark-harness feature): a delivered PUBLISH payload turns into
//! delivered/duplicate/foreign/over-range/anomaly counts, a per-second
//! timeline, and window-only latency (AC-3..AC-8). `parse_arm_line` is
//! covered as a regression guard alongside `Stamp`'s round trip.

use bench::payload::{monotonic_now_ns, Stamp, PAYLOAD_BYTES, TOPIC};
use bench::record::{
    parse_arm_line, Recorder, Timeline, Window, WorkerLoad, ARM_ORIGIN_SKEW_MAX_NS,
    CONNECTIONS_MAX, LATENCY_BINS_BUDGET, LATENCY_BUCKET_MAX_NS, LATENCY_HISTOGRAM_BINS,
    LATENCY_MAX_NS, LATENCY_SIGFIG, SEQ_CHUNK_SEQS, WINDOW_MAX_SPAN_NS,
};
use hdrhistogram::Histogram;

/// Shared armed-recorder fixture: 2 connections, origin 9 s, window [10 s, 20 s).
const CONNECTIONS: u32 = 2;
const ORIGIN_NS: u64 = 9_000_000_000;
const WINDOW_START_NS: u64 = 10_000_000_000;
const WINDOW_END_NS: u64 = 20_000_000_000;

fn armed_recorder() -> Recorder {
    let rec = Recorder::new(CONNECTIONS);
    rec.arm_at(
        ORIGIN_NS,
        Window {
            start_ns: WINDOW_START_NS,
            end_ns: WINDOW_END_NS,
        },
        ORIGIN_NS,
    )
    .expect("recorder arms once from a fresh instance");
    rec
}

/// Record `conn_id`'s deliveries for every `seq` in `start..=end`, each
/// scheduled at `scheduled_ns` and delivered `latency_ns` later.
fn record_seq_range(
    rec: &Recorder,
    conn_id: u32,
    start: u64,
    end: u64,
    scheduled_ns: u64,
    latency_ns: u64,
) {
    for seq in start..=end {
        let payload = Stamp {
            conn_id,
            seq,
            scheduled_ns,
        }
        .encode();
        rec.record(&payload, scheduled_ns + latency_ns);
    }
}

#[test]
fn stamp_round_trips_through_its_64_byte_encoding() {
    let stamp = Stamp {
        conn_id: 7,
        seq: 42,
        scheduled_ns: 123_456_789,
    };

    let encoded = stamp.encode();

    assert_eq!(encoded.len(), PAYLOAD_BYTES);
    assert_eq!(Stamp::decode(&encoded), Some(stamp));
}

#[test]
fn recorder_counts_a_new_stamp_once_in_its_delivery_second() {
    let rec = armed_recorder();
    let scheduled_ns = 11_000_000_000;
    let now_ns = 12_500_000_000;
    let payload = Stamp {
        conn_id: 0,
        seq: 0,
        scheduled_ns,
    }
    .encode();

    rec.record(&payload, now_ns);
    let report = rec.report();

    assert_eq!(report.delivered, 1);
    assert_eq!(report.timeline.sum_secs(12, 13), 1);
    assert_eq!(report.timeline.sum_secs(9, 12), 0);
}

#[test]
fn recorder_counts_a_repeated_sequence_as_duplicate_not_delivery() {
    let rec = armed_recorder();
    let payload = Stamp {
        conn_id: 1,
        seq: 5,
        scheduled_ns: 11_000_000_000,
    }
    .encode();

    rec.record(&payload, 12_000_000_000);
    rec.record(&payload, 12_000_000_000);
    let report = rec.report();

    assert_eq!(report.delivered, 1);
    assert_eq!(report.duplicates, 1);
}

#[test]
fn recorder_counts_bad_payloads_unknown_connections_and_unarmed_records_as_foreign() {
    // A payload one byte short of PAYLOAD_BYTES fails to decode.
    let rec = armed_recorder();
    let short_payload = vec![0u8; PAYLOAD_BYTES - 1];
    rec.record(&short_payload, WINDOW_START_NS);
    let report = rec.report();
    assert_eq!(report.foreign, 1);
    assert_eq!(report.delivered, 0);

    // A well-formed stamp whose padding has been corrupted also fails to decode.
    let rec = armed_recorder();
    let mut payload = Stamp {
        conn_id: 0,
        seq: 0,
        scheduled_ns: WINDOW_START_NS,
    }
    .encode()
    .to_vec();
    assert_eq!(payload[20], 0, "padding byte must start zero for this corruption to take effect");
    payload[20] = 0xFF;
    rec.record(&payload, WINDOW_START_NS);
    let report = rec.report();
    assert_eq!(report.foreign, 1);
    assert_eq!(report.delivered, 0);

    // A connection id at or beyond the armed connection count is unknown.
    let rec = armed_recorder();
    let payload = Stamp {
        conn_id: CONNECTIONS,
        seq: 0,
        scheduled_ns: WINDOW_START_NS,
    }
    .encode();
    rec.record(&payload, WINDOW_START_NS);
    let report = rec.report();
    assert_eq!(report.foreign, 1);
    assert_eq!(report.delivered, 0);

    // A valid stamp on a recorder that was never armed is also foreign.
    let rec = Recorder::new(CONNECTIONS);
    let payload = Stamp {
        conn_id: 0,
        seq: 0,
        scheduled_ns: WINDOW_START_NS,
    }
    .encode();
    rec.record(&payload, WINDOW_START_NS);
    let report = rec.report();
    assert_eq!(report.foreign, 1);
    assert_eq!(report.delivered, 0);
}

#[test]
fn recorder_keeps_latency_only_for_stamps_scheduled_in_the_window() {
    let rec = armed_recorder();
    let latency_ns = 1_000_000;
    let before_window_scheduled_ns = 9_500_000_000;
    let in_window_scheduled_ns = 15_000_000_000;

    let outside = Stamp {
        conn_id: 0,
        seq: 0,
        scheduled_ns: before_window_scheduled_ns,
    }
    .encode();
    rec.record(&outside, before_window_scheduled_ns + latency_ns);

    let inside = Stamp {
        conn_id: 1,
        seq: 0,
        scheduled_ns: in_window_scheduled_ns,
    }
    .encode();
    rec.record(&inside, in_window_scheduled_ns + latency_ns);

    let report = rec.report();
    let total_latency_samples: u64 = report.latency.iter().map(|&(_, count)| count).sum();
    assert_eq!(total_latency_samples, 1);

    let (value, _) = report.latency[0];
    let tolerance_ns = latency_ns / 1000; // within 0.1 %
    assert!(value.abs_diff(latency_ns) <= tolerance_ns);
}

#[test]
fn recorder_counts_over_range_latency_instead_of_clamping() {
    let rec = armed_recorder();
    let scheduled_ns = 10_000_000_000;
    let now_ns = 71_000_000_000;
    assert!(now_ns - scheduled_ns > LATENCY_MAX_NS);
    let payload = Stamp {
        conn_id: 0,
        seq: 0,
        scheduled_ns,
    }
    .encode();

    rec.record(&payload, now_ns);
    let report = rec.report();

    assert_eq!(report.latency_over_range, 1);
    assert!(report.latency.is_empty());
}

#[test]
fn recorder_counts_a_stamp_scheduled_after_its_receipt() {
    let rec = armed_recorder();
    let scheduled_ns = 15_000_000_000;
    let now_ns = 14_000_000_000;
    let payload = Stamp {
        conn_id: 0,
        seq: 0,
        scheduled_ns,
    }
    .encode();

    rec.record(&payload, now_ns);
    let report = rec.report();

    assert_eq!(report.clock_anomalies, 1);
    assert!(report.latency.is_empty());
}

#[test]
fn stamp_encodes_the_documented_byte_layout() {
    // Fixed fixture so a field moving in both encoder and decoder still fails.
    let stamp = Stamp {
        conn_id: 0x0403_0201,
        seq: 0x0807_0605,
        scheduled_ns: 0x0C0B_0A09,
    };

    let encoded = stamp.encode();

    assert_eq!(&encoded[0..4], &[0x01, 0x02, 0x03, 0x04]);
    assert_eq!(&encoded[4..12], &[0x05, 0x06, 0x07, 0x08, 0, 0, 0, 0]);
    assert_eq!(&encoded[12..20], &[0x09, 0x0A, 0x0B, 0x0C, 0, 0, 0, 0]);
    assert!(encoded[20..PAYLOAD_BYTES].iter().all(|&b| b == 0));
}

#[test]
fn recorder_tracks_sequences_across_chunk_boundaries_without_aliasing() {
    // A client-chosen `seq` must never size the duplicate bitset, yet every
    // well-formed stamp is still its own delivery (AC-3) and its repeat is
    // still a duplicate (AC-4). The fixture straddles the word boundary, both
    // chunk boundaries and the top of the `u64` range, so chunk or word
    // arithmetic that folds two distinct sequences together fails here.
    let seqs = [
        0,
        63,
        64,
        SEQ_CHUNK_SEQS - 1,
        SEQ_CHUNK_SEQS,
        SEQ_CHUNK_SEQS * 3 + 7,
        u64::MAX / SEQ_CHUNK_SEQS * SEQ_CHUNK_SEQS,
        u64::MAX,
    ];
    let rec = armed_recorder();
    for seq in seqs {
        let payload = Stamp {
            conn_id: 0,
            seq,
            scheduled_ns: WINDOW_START_NS,
        }
        .encode();
        rec.record(&payload, WINDOW_START_NS);
        rec.record(&payload, WINDOW_START_NS);
    }

    let report = rec.report();

    let count = u64::try_from(seqs.len()).expect("fixture length fits in u64");
    assert_eq!(report.delivered, count);
    assert_eq!(report.duplicates, count);
    assert_eq!(report.foreign, 0);
    assert_eq!(report.timeline.sum_secs(10, 11), count);
}

#[test]
fn recorder_keeps_a_delivery_before_the_origin_in_its_own_second() {
    let rec = armed_recorder();
    let before_origin_ns = 8_500_000_000;
    let in_window_ns = 12_500_000_000;
    let before = Stamp {
        conn_id: 0,
        seq: 0,
        scheduled_ns: WINDOW_START_NS,
    }
    .encode();
    let after = Stamp {
        conn_id: 1,
        seq: 0,
        scheduled_ns: WINDOW_START_NS,
    }
    .encode();

    rec.record(&before, before_origin_ns);
    rec.record(&after, in_window_ns);
    let report = rec.report();

    assert_eq!(report.delivered, 2);
    assert_eq!(report.timeline.origin_sec, 8);
    assert_eq!(report.timeline.sum_secs(8, 9), 1);
    assert_eq!(report.timeline.sum_secs(9, 12), 0);
    assert_eq!(report.timeline.sum_secs(12, 13), 1);
}

#[test]
fn arm_line_parses_and_rejects_malformed_input() {
    let (origin_ns, window) = parse_arm_line("ARM 9000000000 10000000000 20000000000")
        .expect("well-formed ARM line parses");
    assert_eq!(origin_ns, 9_000_000_000);
    assert_eq!(
        window,
        Window {
            start_ns: 10_000_000_000,
            end_ns: 20_000_000_000,
        }
    );

    assert!(parse_arm_line("ARM 1 2").is_err());
    assert!(parse_arm_line("ARM 9000000000 20000000000 10000000000").is_err());
    assert!(parse_arm_line("ARM 9000000000 10000000001 20000000000").is_err());
}

#[test]
fn recorder_counts_a_publish_on_another_topic_as_foreign() {
    // The broker invokes the callback for every ingested PUBLISH, so a valid
    // stamp on an unrelated topic must not enter the benchmark totals. The
    // benchmark topic through the same entry point still must.
    let rec = armed_recorder();
    let payload = Stamp {
        conn_id: 0,
        seq: 0,
        scheduled_ns: WINDOW_START_NS,
    }
    .encode();

    rec.record_on_topic("other/topic", &payload, 12_000_000_000);
    let report = rec.report();

    assert_eq!(report.foreign, 1);
    assert_eq!(report.delivered, 0);
    assert_eq!(report.duplicates, 0);
    assert_eq!(report.timeline.sum_secs(0, 20), 0);

    rec.record_on_topic(TOPIC, &payload, 12_000_000_000);
    let report = rec.report();

    assert_eq!(report.foreign, 1);
    assert_eq!(report.delivered, 1);
    assert_eq!(report.timeline.sum_secs(12, 13), 1);
}

#[test]
fn arm_rejects_an_origin_that_cannot_size_the_timeline() {
    // An `ARM` line is untrusted input: an origin far from the recorder's clock
    // would make the first delivery allocate one bin per intervening second.
    let far_origin_ns = ORIGIN_NS + ARM_ORIGIN_SKEW_MAX_NS + 1;
    let window = Window {
        start_ns: WINDOW_START_NS,
        end_ns: WINDOW_END_NS,
    };
    let rec = Recorder::new(CONNECTIONS);
    assert!(rec.arm_at(far_origin_ns, window, ORIGIN_NS).is_err());
    assert!(rec.arm_at(u64::MAX, window, ORIGIN_NS).is_err());
    assert!(rec
        .arm_at(
            ORIGIN_NS,
            Window {
                start_ns: 0,
                end_ns: WINDOW_END_NS,
            },
            ORIGIN_NS,
        )
        .is_err());
    assert!(rec
        .arm_at(
            ORIGIN_NS,
            Window {
                start_ns: WINDOW_START_NS,
                end_ns: ORIGIN_NS + WINDOW_MAX_SPAN_NS + 1_000_000_000,
            },
            ORIGIN_NS,
        )
        .is_err());

    // A rejected origin leaves the recorder unarmed, so a later valid arm works
    // and a delivery before it is still foreign.
    let payload = Stamp {
        conn_id: 0,
        seq: 0,
        scheduled_ns: WINDOW_START_NS,
    }
    .encode();
    rec.record(&payload, ORIGIN_NS);
    assert_eq!(rec.report().foreign, 1);
    rec.arm_at(ORIGIN_NS, window, ORIGIN_NS)
        .expect("a rejected arm does not consume the single arming");
}

#[test]
fn arm_reads_the_monotonic_clock_and_applies_the_same_skew_bound() {
    // The runner calls `arm` without a clock reading of its own, so the bound
    // must hold against the clock `arm` reads for itself.
    let now_ns = monotonic_now_ns();
    let rec = Recorder::new(CONNECTIONS);
    let far_origin_ns = now_ns + ARM_ORIGIN_SKEW_MAX_NS + 1_000_000_000;
    assert!(rec
        .arm(
            far_origin_ns,
            Window {
                start_ns: far_origin_ns,
                end_ns: far_origin_ns + 1_000_000_000,
            },
        )
        .is_err());

    assert!(rec
        .arm(
            now_ns,
            Window {
                start_ns: now_ns,
                end_ns: now_ns + 1_000_000_000,
            },
        )
        .is_ok());
}

#[test]
fn summing_a_range_wider_than_the_stored_bins_returns_their_total() {
    // A persisted record can name an extreme range; the sum must walk the
    // stored bins instead of every requested second.
    let timeline = Timeline {
        origin_sec: 10,
        counts: vec![1, 2, 3],
    };

    assert_eq!(timeline.sum_secs(0, u64::MAX), 6);
    assert_eq!(timeline.sum_secs(11, u64::MAX), 5);
    assert_eq!(timeline.sum_secs(0, 10), 0);
    assert_eq!(timeline.sum_secs(13, u64::MAX), 0);
    assert_eq!(timeline.sum_secs(12, 11), 0);
}

/// `CONNECTIONS_MAX` is a memory budget divided by the bins one latency
/// histogram holds, so the documented bin count must match what the pinned
/// hdrhistogram actually allocates. A change there would silently move the
/// worst-case memory of a run.
#[test]
fn one_latency_histogram_holds_the_documented_number_of_bins() {
    let histogram: Histogram<u64> = Histogram::new_with_bounds(1, LATENCY_MAX_NS, LATENCY_SIGFIG)
        .expect("hdrhistogram bounds are valid");

    assert_eq!(
        histogram.distinct_values(),
        usize::try_from(LATENCY_HISTOGRAM_BINS).expect("bin count fits in usize")
    );
    assert_eq!(CONNECTIONS_MAX, LATENCY_BINS_BUDGET / LATENCY_HISTOGRAM_BINS);
}

/// `analysis::histogram_total` rejects a persisted bucket value outside
/// `1..=LATENCY_BUCKET_MAX_NS`, so the bound must be the largest value the
/// pinned hdrhistogram can report for a sample at the recorder's cap. It is
/// above `LATENCY_MAX_NS` because `iter_recorded` reports bucket tops.
#[test]
fn the_latency_bucket_bound_is_the_top_of_the_histogram_cap_bucket() {
    let histogram: Histogram<u64> = Histogram::new_with_bounds(1, LATENCY_MAX_NS, LATENCY_SIGFIG)
        .expect("hdrhistogram bounds are valid");

    assert_eq!(
        histogram.highest_equivalent(LATENCY_MAX_NS),
        LATENCY_BUCKET_MAX_NS
    );
}

#[test]
fn recorder_worker_loads_attribute_each_connection_to_the_thread_that_delivered_it_first() {
    let rec = Recorder::new(5);
    rec.arm_at(
        ORIGIN_NS,
        Window {
            start_ns: WINDOW_START_NS,
            end_ns: WINDOW_END_NS,
        },
        ORIGIN_NS,
    )
    .expect("recorder arms once from a fresh instance");

    let scheduled_ns = 15_000_000_000;
    let latency_ns = 1_000_000;

    let rec_c = rec.clone();
    let thread_c = std::thread::spawn(move || {
        record_seq_range(&rec_c, 0, 0, 0, scheduled_ns, latency_ns);
    });

    let rec_b = rec.clone();
    let thread_b = std::thread::spawn(move || {
        record_seq_range(&rec_b, 1, 0, 0, scheduled_ns, latency_ns);
        record_seq_range(&rec_b, 2, 0, 0, scheduled_ns, latency_ns);
    });

    let rec_a = rec.clone();
    let thread_a = std::thread::spawn(move || {
        record_seq_range(&rec_a, 3, 0, 1, scheduled_ns, latency_ns);
        record_seq_range(&rec_a, 4, 0, 1, scheduled_ns, latency_ns);
    });

    thread_c.join().expect("thread C joins");
    thread_b.join().expect("thread B joins");
    thread_a.join().expect("thread A joins");

    // A late delivery on a fourth thread must not move conn 3's group: its
    // first delivery already ran on thread A.
    record_seq_range(&rec, 3, 2, 2, scheduled_ns, latency_ns);

    let loads = rec.worker_loads(3);

    assert_eq!(loads.len(), 3);
    let tolerance_ns = latency_ns / 1000; // within 0.1 %

    assert_eq!(loads[0].connections, 2);
    assert_eq!(loads[0].delivered, 5);
    assert!(loads[0]
        .latency_p99_ns
        .expect("group has in-window samples")
        .abs_diff(latency_ns)
        <= tolerance_ns);

    assert_eq!(loads[1].connections, 2);
    assert_eq!(loads[1].delivered, 2);
    assert!(loads[1]
        .latency_p99_ns
        .expect("group has in-window samples")
        .abs_diff(latency_ns)
        <= tolerance_ns);

    assert_eq!(loads[2].connections, 1);
    assert_eq!(loads[2].delivered, 1);
    assert!(loads[2]
        .latency_p99_ns
        .expect("group has in-window samples")
        .abs_diff(latency_ns)
        <= tolerance_ns);
}

#[test]
fn recorder_worker_loads_pad_with_idle_workers() {
    let rec = Recorder::new(1);
    rec.arm_at(
        ORIGIN_NS,
        Window {
            start_ns: WINDOW_START_NS,
            end_ns: WINDOW_END_NS,
        },
        ORIGIN_NS,
    )
    .expect("recorder arms once from a fresh instance");

    let before_window_scheduled_ns = 9_500_000_000;
    let late_latency_ns = 5_000_000_000; // scheduled 9.5 s, recorded 14.5 s

    let rec_thread = rec.clone();
    std::thread::spawn(move || {
        record_seq_range(
            &rec_thread,
            0,
            0,
            0,
            before_window_scheduled_ns,
            late_latency_ns,
        );
    })
    .join()
    .expect("spawned thread joins");

    let loads = rec.worker_loads(4);

    assert_eq!(
        loads,
        vec![
            WorkerLoad {
                connections: 1,
                delivered: 1,
                latency_p99_ns: None,
            },
            WorkerLoad {
                connections: 0,
                delivered: 0,
                latency_p99_ns: None,
            },
            WorkerLoad {
                connections: 0,
                delivered: 0,
                latency_p99_ns: None,
            },
            WorkerLoad {
                connections: 0,
                delivered: 0,
                latency_p99_ns: None,
            },
        ]
    );
}

/// A connections run delivers no benchmark publish, and a foreign payload
/// never reaches a shard, so no thread is attributed and every entry is the
/// zero padding entry.
#[test]
fn recorder_worker_loads_are_all_zero_when_no_connection_delivered() {
    let rec = armed_recorder();

    let rec_thread = rec.clone();
    std::thread::spawn(move || {
        let short_payload = vec![0u8; PAYLOAD_BYTES - 1];
        rec_thread.record(&short_payload, WINDOW_START_NS);
    })
    .join()
    .expect("spawned thread joins");

    let report = rec.report();
    assert_eq!(report.foreign, 1);
    assert_eq!(report.delivered, 0);

    assert_eq!(
        rec.worker_loads(4),
        vec![
            WorkerLoad {
                connections: 0,
                delivered: 0,
                latency_p99_ns: None,
            };
            4
        ]
    );
}

/// A duplicate is not a delivery: it must not inflate its worker's
/// `delivered`, and a duplicate recorded on a second thread must not move the
/// connection to that thread or open a second group.
#[test]
fn recorder_worker_loads_count_duplicates_on_neither_worker() {
    let rec = armed_recorder();
    let scheduled_ns = 15_000_000_000;
    let latency_ns = 1_000_000;

    let rec_a = rec.clone();
    std::thread::spawn(move || {
        record_seq_range(&rec_a, 0, 0, 0, scheduled_ns, latency_ns);
        record_seq_range(&rec_a, 0, 0, 0, scheduled_ns, latency_ns);
    })
    .join()
    .expect("thread A joins");

    let rec_b = rec.clone();
    std::thread::spawn(move || {
        record_seq_range(&rec_b, 0, 0, 0, scheduled_ns, latency_ns);
    })
    .join()
    .expect("thread B joins");

    let report = rec.report();
    assert_eq!(report.delivered, 1);
    assert_eq!(report.duplicates, 2);

    let loads = rec.worker_loads(2);
    assert_eq!(loads.len(), 2);
    assert_eq!(loads[0].connections, 1);
    assert_eq!(loads[0].delivered, 1);
    assert!(loads[0]
        .latency_p99_ns
        .expect("group has in-window samples")
        .abs_diff(latency_ns)
        <= latency_ns / 1000);
    assert_eq!(
        loads[1],
        WorkerLoad {
            connections: 0,
            delivered: 0,
            latency_p99_ns: None,
        }
    );

    let total_delivered: u64 = loads.iter().map(|l| l.delivered).sum();
    assert_eq!(total_delivered, report.delivered);
}

#[test]
fn recorder_worker_loads_report_the_p99_of_each_workers_merged_window_samples() {
    let rec = Recorder::new(5);
    rec.arm_at(
        ORIGIN_NS,
        Window {
            start_ns: WINDOW_START_NS,
            end_ns: WINDOW_END_NS,
        },
        ORIGIN_NS,
    )
    .expect("recorder arms once from a fresh instance");

    let in_window_scheduled_ns = 15_000_000_000;
    let fast_latency_ns = 1_000_000;
    let slow_latency_ns = 100_000_000;
    let before_window_scheduled_ns = 9_500_000_000;
    let late_latency_ns = 5_000_000_000;

    let rec_a = rec.clone();
    let thread_a = std::thread::spawn(move || {
        record_seq_range(&rec_a, 0, 0, 49, in_window_scheduled_ns, fast_latency_ns);
        record_seq_range(&rec_a, 1, 0, 48, in_window_scheduled_ns, fast_latency_ns);
        record_seq_range(&rec_a, 1, 49, 49, in_window_scheduled_ns, slow_latency_ns);
        record_seq_range(&rec_a, 0, 50, 50, before_window_scheduled_ns, late_latency_ns);
    });

    let rec_b = rec.clone();
    let thread_b = std::thread::spawn(move || {
        record_seq_range(&rec_b, 2, 0, 0, in_window_scheduled_ns, slow_latency_ns);
    });

    let rec_c = rec.clone();
    let thread_c = std::thread::spawn(move || {
        record_seq_range(&rec_c, 3, 0, 49, in_window_scheduled_ns, fast_latency_ns);
        record_seq_range(&rec_c, 4, 0, 47, in_window_scheduled_ns, fast_latency_ns);
        record_seq_range(&rec_c, 4, 48, 49, in_window_scheduled_ns, slow_latency_ns);
    });

    thread_a.join().expect("thread A joins");
    thread_b.join().expect("thread B joins");
    thread_c.join().expect("thread C joins");

    let loads = rec.worker_loads(3);

    assert_eq!(loads.len(), 3);
    let tolerance = |expected: u64| expected / 1000; // within 0.1 %

    assert_eq!(loads[0].connections, 2);
    assert_eq!(loads[0].delivered, 101);
    let p99_a = loads[0]
        .latency_p99_ns
        .expect("group A has in-window samples");
    assert!(p99_a.abs_diff(fast_latency_ns) <= tolerance(fast_latency_ns));

    assert_eq!(loads[1].connections, 2);
    assert_eq!(loads[1].delivered, 100);
    let p99_c = loads[1]
        .latency_p99_ns
        .expect("group C has in-window samples");
    assert!(p99_c.abs_diff(slow_latency_ns) <= tolerance(slow_latency_ns));

    assert_eq!(loads[2].connections, 1);
    assert_eq!(loads[2].delivered, 1);
    let p99_b = loads[2]
        .latency_p99_ns
        .expect("group B has in-window samples");
    assert!(p99_b.abs_diff(slow_latency_ns) <= tolerance(slow_latency_ns));
}
