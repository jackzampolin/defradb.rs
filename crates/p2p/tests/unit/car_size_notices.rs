use super::*;
use ciborium::Value;
use multihash_codetable::{Code, MultihashDigest};

fn cid(data: &[u8]) -> Cid {
    Cid::new_v1(0x71, Code::Sha2_256.digest(data))
}

#[test]
fn legacy_decoder_keeps_useful_blocks_and_ignores_size_notices() {
    let root = cid(b"oversized");
    let small = cid(b"small");
    let notices = [(root, CAR_MAX_BYTES + 1)];
    let car = encode_car_response(&[root], &[(&small, b"small")], &notices).unwrap();
    assert_eq!(decode_car_oversized(&car).unwrap(), notices);
    assert_eq!(
        decode_car(&car).unwrap(),
        (vec![root], vec![(small, b"small".to_vec())])
    );

    let empty = encode_car_response(&[root], &[], &notices).unwrap();
    assert!(!car_has_any_block(&empty));
    assert_eq!(decode_car_oversized(&empty).unwrap(), notices);
    assert!(decode_car_oversized(&encode_car(&[root], &[]).unwrap())
        .unwrap()
        .is_empty());
}

#[test]
fn malformed_size_notices_are_rejected() {
    let root = cid(b"root");
    let notice = |size: i64| {
        Value::Array(vec![
            Value::Bytes(root.to_bytes()),
            Value::Integer(size.into()),
        ])
    };
    for value in [
        Value::Bool(true),
        Value::Array(vec![Value::Bool(true)]),
        Value::Array(vec![notice(-1)]),
        Value::Array(vec![notice(CAR_MAX_BYTES as i64)]),
        Value::Array(vec![Value::Array(vec![
            Value::Bytes(vec![0]),
            Value::Integer((CAR_MAX_BYTES as u64 + 1).into()),
        ])]),
        Value::Array(vec![notice(CAR_MAX_BYTES as i64 + 1); CAR_MAX_BLOCKS + 1]),
    ] {
        let mut header = Vec::new();
        ciborium::into_writer(
            &Value::Map(vec![(Value::Text("oversized".into()), value)]),
            &mut header,
        )
        .unwrap();
        let mut car = Vec::new();
        write_varint_prefixed(&mut car, &header);
        assert!(decode_car_oversized(&car).is_err());
    }
    assert!(decode_car_oversized(&[]).is_err());
    assert!(decode_car_oversized(&[127, 0]).is_err());
}
