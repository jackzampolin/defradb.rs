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
fn unrecord_removes_phantom_record() {
    let tracker = Arc::new(ConflictTracker::new());
    let first_snapshot = tracker.begin_snapshot();
    let second_snapshot = tracker.begin_snapshot();

    let writes = [b"d/i/books/failed-write".to_vec()];
    let version = tracker
        .check_and_record(first_snapshot.version(), writes.iter(), &ReadSet::default())
        .unwrap();
    drop(first_snapshot);

    // Simulate the physical write failing: without unrecord the second
    // transaction would hit a phantom conflict against data that never
    // landed.
    tracker.unrecord(version);

    tracker
        .check_and_record(
            second_snapshot.version(),
            writes.iter(),
            &ReadSet::default(),
        )
        .unwrap();
}

#[test]
fn unrecord_tolerates_pruned_and_empty_versions() {
    let tracker = Arc::new(ConflictTracker::new());

    // Version 0 marks "nothing recorded" (empty write set).
    let empty_version = tracker
        .check_and_record(0, [].iter(), &ReadSet::default())
        .unwrap();
    assert_eq!(empty_version, 0);
    tracker.unrecord(empty_version);

    // A record pruned before unrecord (no active snapshots pin it) is
    // silently gone; unrecord must not panic or disturb later commits.
    let snapshot = tracker.begin_snapshot();
    let writes = [b"d/i/books/pruned".to_vec()];
    let version = tracker
        .check_and_record(snapshot.version(), writes.iter(), &ReadSet::default())
        .unwrap();
    drop(snapshot);
    tracker.unrecord(version);
    tracker.unrecord(version);

    let survivor = tracker.begin_snapshot();
    tracker
        .check_and_record(survivor.version(), writes.iter(), &ReadSet::default())
        .unwrap();
}

#[test]
fn record_guard_unrecords_on_drop() {
    let tracker = Arc::new(ConflictTracker::new());
    let loser_snapshot = tracker.begin_snapshot();
    let writes = [b"d/i/books/guarded".to_vec()];

    let version = tracker
        .check_and_record(
            tracker.current_version(),
            writes.iter(),
            &ReadSet::default(),
        )
        .unwrap();
    drop(RecordGuard::new(&tracker, version));

    // The dropped (armed) guard removed the record: no phantom conflict.
    tracker
        .check_and_record(loser_snapshot.version(), writes.iter(), &ReadSet::default())
        .unwrap();
}

#[test]
fn record_guard_defuse_keeps_record() {
    let tracker = Arc::new(ConflictTracker::new());
    let loser_snapshot = tracker.begin_snapshot();
    let writes = [b"d/i/books/kept".to_vec()];

    let version = tracker
        .check_and_record(
            tracker.current_version(),
            writes.iter(),
            &ReadSet::default(),
        )
        .unwrap();
    RecordGuard::new(&tracker, version).defuse();

    let err = tracker
        .check_and_record(loser_snapshot.version(), writes.iter(), &ReadSet::default())
        .unwrap_err();
    assert!(matches!(err, crate::corekv::Error::TxnConflict));
}

#[test]
fn record_guard_unrecords_on_panic() {
    let tracker = Arc::new(ConflictTracker::new());
    let loser_snapshot = tracker.begin_snapshot();
    let writes = [b"d/i/books/panicked".to_vec()];

    let version = tracker
        .check_and_record(
            tracker.current_version(),
            writes.iter(),
            &ReadSet::default(),
        )
        .unwrap();
    let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _guard = RecordGuard::new(&tracker, version);
        panic!("physical write panicked");
    }));
    assert!(unwound.is_err());

    tracker
        .check_and_record(loser_snapshot.version(), writes.iter(), &ReadSet::default())
        .unwrap();
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
