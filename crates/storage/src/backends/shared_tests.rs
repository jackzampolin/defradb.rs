use super::*;

#[test]
fn detects_write_to_committed_read_prefix() {
    let tracker = Arc::new(ConflictTracker::new());
    let first_snapshot = tracker.begin_snapshot();
    let second_snapshot = tracker.begin_snapshot();

    let mut first_reads = ReadSet::default();
    first_reads.record_iter_options(&IterOptions::new().with_prefix(b"d/i/books/".to_vec()));
    let first_writes = [b"d/d/publishers/website".to_vec()];
    tracker
        .check_and_record(first_snapshot.version(), first_writes.iter(), &first_reads)
        .unwrap();
    drop(first_snapshot);

    let second_writes = [b"d/i/books/online-book".to_vec()];
    let err = tracker
        .check_and_record(
            second_snapshot.version(),
            second_writes.iter(),
            &ReadSet::default(),
        )
        .unwrap_err();

    assert!(matches!(err, crate::corekv::Error::TxnConflict));
}

#[test]
fn ignores_document_collection_scan_prefixes() {
    let tracker = Arc::new(ConflictTracker::new());
    let first_snapshot = tracker.begin_snapshot();
    let second_snapshot = tracker.begin_snapshot();

    let mut first_reads = ReadSet::default();
    first_reads.record_iter_options(&IterOptions::new().with_prefix(b"d/d/books/".to_vec()));
    let first_writes = [b"d/d/publishers/website".to_vec()];
    tracker
        .check_and_record(first_snapshot.version(), first_writes.iter(), &first_reads)
        .unwrap();
    drop(first_snapshot);

    let second_writes = [b"d/d/books/online-book".to_vec()];
    tracker
        .check_and_record(
            second_snapshot.version(),
            second_writes.iter(),
            &ReadSet::default(),
        )
        .unwrap();
}

fn collection_transition_reads() -> ReadSet {
    let mut reads = ReadSet::default();
    reads.record_iter_options(
        &IterOptions::new()
            .with_prefix(b"h/c/7/".to_vec())
            .with_commutative_set(),
    );
    reads
}

#[test]
fn commutative_set_transitions_do_not_conflict() {
    let tracker = Arc::new(ConflictTracker::new());
    let first_snapshot = tracker.begin_snapshot();
    let second_snapshot = tracker.begin_snapshot();
    let reads = collection_transition_reads();

    let first_writes = [b"h/c/7/old".to_vec(), b"h/c/7/first".to_vec()];
    tracker
        .check_and_record(first_snapshot.version(), first_writes.iter(), &reads)
        .unwrap();
    drop(first_snapshot);

    let second_writes = [b"h/c/7/old".to_vec(), b"h/c/7/second".to_vec()];
    tracker
        .check_and_record(second_snapshot.version(), second_writes.iter(), &reads)
        .unwrap();
}

#[test]
fn commutative_set_transition_conflicts_with_ordinary_scan_in_either_order() {
    for ordinary_first in [false, true] {
        let tracker = Arc::new(ConflictTracker::new());
        let first_snapshot = tracker.begin_snapshot();
        let second_snapshot = tracker.begin_snapshot();
        let commutative_reads = collection_transition_reads();
        let mut ordinary_reads = ReadSet::default();
        ordinary_reads.record_iter_options(&IterOptions::new().with_prefix(b"h/c/7/".to_vec()));

        let (first_reads, second_reads) = if ordinary_first {
            (&ordinary_reads, &commutative_reads)
        } else {
            (&commutative_reads, &ordinary_reads)
        };
        let first_writes = [b"h/c/7/old".to_vec(), b"h/c/7/first".to_vec()];
        tracker
            .check_and_record(first_snapshot.version(), first_writes.iter(), first_reads)
            .unwrap();
        drop(first_snapshot);

        let second_writes = [b"h/c/7/old".to_vec(), b"h/c/7/second".to_vec()];
        let error = tracker
            .check_and_record(
                second_snapshot.version(),
                second_writes.iter(),
                second_reads,
            )
            .unwrap_err();
        assert!(matches!(error, crate::corekv::Error::TxnConflict));
    }
}

#[test]
fn commutative_set_transition_does_not_hide_document_head_conflicts() {
    let tracker = Arc::new(ConflictTracker::new());
    let first_snapshot = tracker.begin_snapshot();
    let second_snapshot = tracker.begin_snapshot();
    let reads = collection_transition_reads();

    let first_writes = [
        b"h/c/7/old".to_vec(),
        b"h/c/7/first".to_vec(),
        b"h/d/1/C/old".to_vec(),
    ];
    tracker
        .check_and_record(first_snapshot.version(), first_writes.iter(), &reads)
        .unwrap();
    drop(first_snapshot);

    let second_writes = [
        b"h/c/7/old".to_vec(),
        b"h/c/7/second".to_vec(),
        b"h/d/1/C/old".to_vec(),
    ];
    let error = tracker
        .check_and_record(second_snapshot.version(), second_writes.iter(), &reads)
        .unwrap_err();
    assert!(matches!(error, crate::corekv::Error::TxnConflict));
}

#[test]
fn detects_read_of_committed_write_key() {
    let tracker = Arc::new(ConflictTracker::new());
    let first_snapshot = tracker.begin_snapshot();
    let second_snapshot = tracker.begin_snapshot();

    let first_writes = [b"d/d/books/website-book".to_vec()];
    tracker
        .check_and_record(
            first_snapshot.version(),
            first_writes.iter(),
            &ReadSet::default(),
        )
        .unwrap();
    drop(first_snapshot);

    let mut second_reads = ReadSet::default();
    second_reads.record_key(b"d/d/books/website-book");
    let second_writes = [b"d/d/publishers/online".to_vec()];
    let err = tracker
        .check_and_record(
            second_snapshot.version(),
            second_writes.iter(),
            &second_reads,
        )
        .unwrap_err();

    assert!(matches!(err, crate::corekv::Error::TxnConflict));
}

fn cid_bytes(fill: u8) -> Vec<u8> {
    // Valid CIDv1: version 1, dag-cbor codec, sha2-256 multihash.
    let mut bytes = vec![0x01, 0x71, 0x12, 0x20];
    bytes.extend(std::iter::repeat_n(fill, 32));
    assert!(cid::Cid::try_from(bytes.as_slice()).is_ok());
    bytes
}

fn block_key(fill: u8) -> Vec<u8> {
    let mut key = vec![b'b'];
    key.extend(cid_bytes(fill));
    key
}

#[test]
fn identical_block_writes_do_not_conflict() {
    let tracker = Arc::new(ConflictTracker::new());
    let first_snapshot = tracker.begin_snapshot();
    let second_snapshot = tracker.begin_snapshot();

    // Two overlapping transactions write the same content-addressed block
    // (same CID => byte-identical value). Both must commit (#1194).
    let writes = [block_key(0xaa)];
    tracker
        .check_and_record(first_snapshot.version(), writes.iter(), &ReadSet::default())
        .unwrap();
    drop(first_snapshot);

    tracker
        .check_and_record(
            second_snapshot.version(),
            writes.iter(),
            &ReadSet::default(),
        )
        .unwrap();
}

#[test]
fn merge_index_writes_still_conflict() {
    let tracker = Arc::new(ConflictTracker::new());
    let first_snapshot = tracker.begin_snapshot();
    let second_snapshot = tracker.begin_snapshot();

    // Blockstore merge-tracking keys ('b' + 'm' + CID) are mutable state,
    // not content-addressed data: write-write stays a conflict even though
    // the payload after the merge prefix is a valid CID.
    let mut merge_key = vec![b'b', b'm'];
    merge_key.extend(cid_bytes(0xaa));
    let writes = [merge_key];
    tracker
        .check_and_record(first_snapshot.version(), writes.iter(), &ReadSet::default())
        .unwrap();
    drop(first_snapshot);

    let err = tracker
        .check_and_record(
            second_snapshot.version(),
            writes.iter(),
            &ReadSet::default(),
        )
        .unwrap_err();
    assert!(matches!(err, crate::corekv::Error::TxnConflict));
}

#[test]
fn non_cid_blockstore_writes_still_conflict() {
    let tracker = Arc::new(ConflictTracker::new());
    let first_snapshot = tracker.begin_snapshot();
    let second_snapshot = tracker.begin_snapshot();

    // A blockstore-namespace key whose payload is not a valid CID is not
    // content-addressed: write-write stays a conflict.
    let mut bogus_key = vec![b'b', 0x01, 0x71];
    bogus_key.extend(std::iter::repeat_n(0xaa, 34));
    assert!(cid::Cid::try_from(&bogus_key[1..]).is_err());
    let writes = [bogus_key];
    tracker
        .check_and_record(first_snapshot.version(), writes.iter(), &ReadSet::default())
        .unwrap();
    drop(first_snapshot);

    let err = tracker
        .check_and_record(
            second_snapshot.version(),
            writes.iter(),
            &ReadSet::default(),
        )
        .unwrap_err();
    assert!(matches!(err, crate::corekv::Error::TxnConflict));
}

#[test]
fn cid_with_trailing_bytes_still_conflicts() {
    let tracker = Arc::new(ConflictTracker::new());
    let first_snapshot = tracker.begin_snapshot();
    let second_snapshot = tracker.begin_snapshot();

    // CID parsing accepts a valid prefix, so require the entire payload to
    // be one CID before treating the key as immutable content-addressed data.
    let mut key = block_key(0xaa);
    key.push(0xff);
    assert!(cid::Cid::try_from(&key[1..]).is_ok());
    let writes = [key];
    tracker
        .check_and_record(first_snapshot.version(), writes.iter(), &ReadSet::default())
        .unwrap();
    drop(first_snapshot);

    let err = tracker
        .check_and_record(
            second_snapshot.version(),
            writes.iter(),
            &ReadSet::default(),
        )
        .unwrap_err();
    assert!(matches!(err, crate::corekv::Error::TxnConflict));
}

#[test]
fn block_write_vs_committed_read_still_conflicts() {
    let tracker = Arc::new(ConflictTracker::new());
    let first_snapshot = tracker.begin_snapshot();
    let second_snapshot = tracker.begin_snapshot();

    // A committed transaction READ the block key; a later write to it is
    // still an anti-dependency and must conflict.
    let block = block_key(0xbb);
    let mut first_reads = ReadSet::default();
    first_reads.record_key(&block);
    let first_writes = [b"d/d/books/other".to_vec()];
    tracker
        .check_and_record(first_snapshot.version(), first_writes.iter(), &first_reads)
        .unwrap();
    drop(first_snapshot);

    let second_writes = [block];
    let err = tracker
        .check_and_record(
            second_snapshot.version(),
            second_writes.iter(),
            &ReadSet::default(),
        )
        .unwrap_err();
    assert!(matches!(err, crate::corekv::Error::TxnConflict));
}

#[test]
fn skips_retained_prefix_for_recent_snapshots() {
    const RETAINED_PREFIX: usize = 64;

    let tracker = Arc::new(ConflictTracker::new());
    let old_snapshot = tracker.begin_snapshot();
    let no_reads = ReadSet::default();

    for index in 0..RETAINED_PREFIX {
        let snapshot = tracker.begin_snapshot();
        let writes = [format!("history/{index}").into_bytes()];
        tracker
            .check_and_record(snapshot.version(), writes.iter(), &no_reads)
            .unwrap();
    }

    let recent_snapshot = tracker.begin_snapshot();
    let suffix_snapshot = tracker.begin_snapshot();
    let suffix_writes = [b"suffix/conflict".to_vec()];
    tracker
        .check_and_record(suffix_snapshot.version(), suffix_writes.iter(), &no_reads)
        .unwrap();
    drop(suffix_snapshot);

    {
        let state = tracker.state.lock();
        assert_eq!(state.committed.len(), RETAINED_PREFIX + 1);
        assert_eq!(state.committed_after(recent_snapshot.version()).len(), 1);
    }

    let old_writes = [b"history/0".to_vec()];
    let old_err = tracker
        .check_and_record(old_snapshot.version(), old_writes.iter(), &no_reads)
        .unwrap_err();
    assert!(matches!(old_err, crate::corekv::Error::TxnConflict));

    let recent_err = tracker
        .check_and_record(recent_snapshot.version(), suffix_writes.iter(), &no_reads)
        .unwrap_err();
    assert!(matches!(recent_err, crate::corekv::Error::TxnConflict));
}

#[test]
fn pending_reservation_blocks_conflicting_writer_before_publication() {
    let tracker = Arc::new(ConflictTracker::new());
    let first_snapshot = tracker.begin_snapshot();
    let second_snapshot = tracker.begin_snapshot();
    let writes = [b"reserved/key".to_vec()];

    let reservation = tracker
        .reserve(first_snapshot.version(), writes.iter(), &ReadSet::default())
        .unwrap();
    assert_eq!(tracker.current_version(), 0);

    let error = tracker
        .reserve(
            second_snapshot.version(),
            writes.iter(),
            &ReadSet::default(),
        )
        .err()
        .expect("pending write must conflict");
    assert!(matches!(error, crate::corekv::Error::TxnConflict));

    assert_eq!(reservation.publish(), 1);
    assert_eq!(tracker.current_version(), 1);
}

#[test]
fn dropped_reservation_releases_conflicting_writer() {
    let tracker = Arc::new(ConflictTracker::new());
    let first_snapshot = tracker.begin_snapshot();
    let second_snapshot = tracker.begin_snapshot();
    let writes = [b"cancelled/key".to_vec()];

    let reservation = tracker
        .reserve(first_snapshot.version(), writes.iter(), &ReadSet::default())
        .unwrap();
    drop(reservation);

    let replacement = tracker
        .reserve(
            second_snapshot.version(),
            writes.iter(),
            &ReadSet::default(),
        )
        .unwrap();
    assert_eq!(replacement.publish(), 1);
}

#[test]
fn disjoint_reservations_publish_in_physical_completion_order() {
    let tracker = Arc::new(ConflictTracker::new());
    let first_snapshot = tracker.begin_snapshot();
    let second_snapshot = tracker.begin_snapshot();
    let first_writes = [b"first/key".to_vec()];
    let second_writes = [b"second/key".to_vec()];

    let first = tracker
        .reserve(
            first_snapshot.version(),
            first_writes.iter(),
            &ReadSet::default(),
        )
        .unwrap();
    let second = tracker
        .reserve(
            second_snapshot.version(),
            second_writes.iter(),
            &ReadSet::default(),
        )
        .unwrap();
    assert_eq!(tracker.current_version(), 0);

    assert_eq!(second.publish(), 1);
    assert_eq!(first.publish(), 2);
}

#[test]
fn indexed_conflict_checks_match_linear_history_scan() {
    let mut state = ConflictTrackerState::default();

    let mut point_reads = ReadSet::default();
    point_reads.record_key(b"point/read");
    state.record(1, HashSet::from([b"alpha/1".to_vec()]), point_reads);

    let mut prefix_reads = ReadSet::default();
    prefix_reads.record_iter_options(&IterOptions::new().with_prefix(b"prefix/".to_vec()));
    state.record(2, HashSet::from([b"beta/2".to_vec()]), prefix_reads);

    let mut range_reads = ReadSet::default();
    range_reads.record_iter_options(
        &IterOptions::new()
            .with_start(b"range/b".to_vec())
            .with_end(b"range/f".to_vec()),
    );
    state.record(3, HashSet::from([b"gamma/3".to_vec()]), range_reads);

    let commutative_reads = collection_transition_reads();
    state.record(
        4,
        HashSet::from([b"h/c/7/shared".to_vec()]),
        commutative_reads,
    );
    state.record(5, HashSet::from([block_key(0xcc)]), ReadSet::default());

    let write_sets = [
        vec![],
        vec![b"alpha/1".to_vec()],
        vec![b"point/read".to_vec()],
        vec![b"prefix/new".to_vec()],
        vec![b"range/d".to_vec()],
        vec![b"h/c/7/shared".to_vec()],
        vec![block_key(0xcc)],
        vec![b"unrelated".to_vec(), b"prefix/second".to_vec()],
    ];

    let mut candidate_point = ReadSet::default();
    candidate_point.record_key(b"beta/2");
    let mut candidate_prefix = ReadSet::default();
    candidate_prefix.record_iter_options(&IterOptions::new().with_prefix(b"gamma/".to_vec()));
    let mut candidate_range = ReadSet::default();
    candidate_range.record_iter_options(
        &IterOptions::new()
            .with_start(b"alpha/".to_vec())
            .with_end(b"delta/".to_vec()),
    );
    let mut candidate_empty_range = ReadSet::default();
    candidate_empty_range.record_iter_options(
        &IterOptions::new()
            .with_start(b"zulu/".to_vec())
            .with_end(b"alpha/".to_vec()),
    );
    let candidate_commutative = collection_transition_reads();
    let read_sets = [
        ReadSet::default(),
        candidate_point,
        candidate_prefix,
        candidate_range,
        candidate_empty_range,
        candidate_commutative,
    ];

    for read_version in 0..=5 {
        for writes in &write_sets {
            let write_refs: Vec<&Vec<u8>> = writes.iter().collect();
            for reads in &read_sets {
                let linear = state.committed_after(read_version).iter().any(
                    |(_, other_writes, other_reads)| {
                        transaction_conflicts(&write_refs, reads, other_writes, other_reads)
                    },
                );
                let indexed = state.conflicts_committed(read_version, &write_refs, reads);
                assert_eq!(
                    indexed, linear,
                    "mismatch at version {read_version} for writes {writes:?} and reads {reads:?}"
                );
            }
        }
    }
}
