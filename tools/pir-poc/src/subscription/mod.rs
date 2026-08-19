mod batch_benchmark;
mod batch_eval;
mod benchmark;
mod demo;

use std::collections::HashMap;

use anyhow::{bail, Context, Result};
use fss_rs::dpf::{Dpf, DpfImpl};
use fss_rs::group::byte::ByteGroup;
use fss_rs::prg::Aes128MatyasMeyerOseasPrg;
use fss_rs::{Cw, PointFn, Share};
use rand::{CryptoRng, RngCore};

// This is a computational two-party DPF: privacy relies on the selected
// construction and the AES-based PRG, in addition to non-collusion. It is not
// an information-theoretic FSS implementation.

pub use batch_benchmark::{run as benchmark_batches, LiveBatchBenchmarkReport};
pub use benchmark::{run as benchmark, SubscriptionBenchmarkReport};
pub use demo::{run as demo, SubscriptionDemoReport};

const INPUT_BYTES: usize = 4;
const OUTPUT_BYTES: usize = 16;
const KEY_MAGIC: &[u8; 4] = b"DPF1";
const KEY_HEADER_BYTES: usize = 16;
const CORRECTION_WORD_BYTES: usize = OUTPUT_BYTES + 1;
const MATCH_VALUE: [u8; OUTPUT_BYTES] = [1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];

// Public, domain-separated AES keys for the PRG. DPF secrecy comes from the
// independently random roots generated for each subscription, not these keys.
const PRG_KEYS: [[u8; 16]; 2] = [*b"Defra-DPF-PRG-A!", *b"Defra-DPF-PRG-B!"];

type OutputGroup = ByteGroup<OUTPUT_BYTES>;
type Prg = Aes128MatyasMeyerOseasPrg<OUTPUT_BYTES, 1, 2>;
type Engine = DpfImpl<INPUT_BYTES, OUTPUT_BYTES, Prg>;
type InnerKey = Share<OUTPUT_BYTES, OutputGroup>;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct SubscriptionId([u8; 16]);

impl SubscriptionId {
    pub fn random<R: RngCore + CryptoRng>(rng: &mut R) -> Self {
        let mut value = [0u8; 16];
        rng.fill_bytes(&mut value);
        Self(value)
    }
}

impl std::fmt::Display for SubscriptionId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}", hex::encode(self.0))
    }
}

pub struct CompactRegistration {
    pub id: SubscriptionId,
    pub bucket_count: usize,
    pub server_keys: [Vec<u8>; 2],
}

pub fn compact_registration<R: RngCore + CryptoRng>(
    target_bucket: usize,
    bucket_count: usize,
    rng: &mut R,
) -> Result<CompactRegistration> {
    let depth = validate_domain(target_bucket, bucket_count)?;
    let engine = engine(depth);
    let alpha = encode_bucket(target_bucket, depth);
    let point = PointFn {
        alpha,
        beta: MATCH_VALUE.into(),
    };
    let mut roots = [[0u8; OUTPUT_BYTES]; 2];
    rng.fill_bytes(&mut roots[0]);
    rng.fill_bytes(&mut roots[1]);
    let generated = engine.gen(&point, [&roots[0], &roots[1]]);

    let mut left = generated.clone();
    left.s0s = vec![left.s0s[0]];
    let mut right = generated;
    right.s0s = vec![right.s0s[1]];

    Ok(CompactRegistration {
        id: SubscriptionId::random(rng),
        bucket_count,
        server_keys: [
            encode_server_key(false, bucket_count, &left)?,
            encode_server_key(true, bucket_count, &right)?,
        ],
    })
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NotificationShare {
    pub subscription_id: SubscriptionId,
    party: bool,
    value: [u8; OUTPUT_BYTES],
}

pub fn combine_compact(shares: &[NotificationShare]) -> Result<bool> {
    if shares.len() != 2 {
        bail!("Compact DPF subscriptions require exactly two result shares");
    }
    if shares[0].subscription_id != shares[1].subscription_id {
        bail!("subscription result IDs differ");
    }
    if shares[0].party == shares[1].party {
        bail!("Compact DPF results came from the same server party");
    }
    let mut combined = shares[0].value;
    xor_array(&mut combined, &shares[1].value);
    if combined == [0; OUTPUT_BYTES] {
        Ok(false)
    } else if combined == MATCH_VALUE {
        Ok(true)
    } else {
        bail!("Compact DPF servers returned an invalid combined value")
    }
}

struct CompactServerKey {
    inner: InnerKey,
}

pub struct CompactSubscriptionServer {
    party: bool,
    bucket_count: usize,
    depth: usize,
    engine: Engine,
    subscriptions: HashMap<SubscriptionId, CompactServerKey>,
}

impl CompactSubscriptionServer {
    pub fn new(party_index: usize, bucket_count: usize) -> Result<Self> {
        if party_index > 1 {
            bail!("Compact DPF supports server indexes 0 and 1 only");
        }
        let depth = validate_domain(0, bucket_count)?;
        Ok(Self {
            party: party_index == 1,
            bucket_count,
            depth,
            engine: engine(depth),
            subscriptions: HashMap::new(),
        })
    }

    pub fn register(&mut self, id: SubscriptionId, encoded_key: &[u8]) -> Result<()> {
        if self.subscriptions.contains_key(&id) {
            bail!("subscription ID is already registered");
        }
        let key = decode_server_key(encoded_key, self.party, self.bucket_count)?;
        self.subscriptions
            .insert(id, CompactServerKey { inner: key });
        Ok(())
    }

    pub fn unregister(&mut self, id: SubscriptionId) -> bool {
        self.subscriptions.remove(&id).is_some()
    }

    pub fn subscription_count(&self) -> usize {
        self.subscriptions.len()
    }

    pub fn evaluate_event(&self, event_bucket: usize) -> Result<Vec<NotificationShare>> {
        if event_bucket >= self.bucket_count {
            bail!("event bucket is outside the subscription domain");
        }
        let encoded_bucket = encode_bucket(event_bucket, self.depth);
        Ok(self
            .subscriptions
            .iter()
            .map(|(id, key)| {
                let mut output = ByteGroup([0; OUTPUT_BYTES]);
                self.engine
                    .eval_point(self.party, &key.inner, &encoded_bucket, &mut output);
                NotificationShare {
                    subscription_id: *id,
                    party: self.party,
                    value: output.0,
                }
            })
            .collect())
    }

    pub fn evaluate_one(
        &self,
        id: SubscriptionId,
        event_bucket: usize,
    ) -> Result<NotificationShare> {
        if event_bucket >= self.bucket_count {
            bail!("event bucket is outside the subscription domain");
        }
        let key = self
            .subscriptions
            .get(&id)
            .context("subscription is not registered")?;
        let mut output = ByteGroup([0; OUTPUT_BYTES]);
        self.engine.eval_point(
            self.party,
            &key.inner,
            &encode_bucket(event_bucket, self.depth),
            &mut output,
        );
        Ok(NotificationShare {
            subscription_id: id,
            party: self.party,
            value: output.0,
        })
    }
}

pub struct DenseRegistration {
    pub id: SubscriptionId,
    pub bucket_count: usize,
    pub server_keys: Vec<Vec<u8>>,
}

pub fn dense_registration<R: RngCore + CryptoRng>(
    target_bucket: usize,
    bucket_count: usize,
    server_count: usize,
    rng: &mut R,
) -> Result<DenseRegistration> {
    Ok(DenseRegistration {
        id: SubscriptionId::random(rng),
        bucket_count,
        server_keys: crate::dense::query_shares(target_bucket, bucket_count, server_count, rng)?,
    })
}

pub fn evaluate_dense(key: &[u8], event_bucket: usize, bucket_count: usize) -> Result<u8> {
    if key.len() != crate::dense::query_size(bucket_count) {
        bail!("dense subscription key has the wrong length");
    }
    if event_bucket >= bucket_count {
        bail!("event bucket is outside the subscription domain");
    }
    Ok((key[event_bucket / 8] >> (event_bucket % 8)) & 1)
}

pub fn combine_dense(shares: &[u8]) -> Result<bool> {
    if shares.len() < 2 {
        bail!("Dense subscriptions require at least two result shares");
    }
    let value = shares
        .iter()
        .copied()
        .fold(0u8, |combined, share| combined ^ share);
    match value {
        0 => Ok(false),
        1 => Ok(true),
        _ => bail!("Dense subscription servers returned a non-bit value"),
    }
}

fn engine(depth: usize) -> Engine {
    let key_refs = [&PRG_KEYS[0], &PRG_KEYS[1]];
    Engine::new_with_filter(Prg::new(&key_refs), depth)
}

fn validate_domain(bucket: usize, bucket_count: usize) -> Result<usize> {
    if !bucket_count.is_power_of_two()
        || !(4..=u32::MAX as u64 + 1).contains(&(bucket_count as u64))
    {
        bail!("subscription bucket count must be a power of two between 4 and 2^32");
    }
    if bucket >= bucket_count {
        bail!("subscription bucket is outside the domain");
    }
    Ok(bucket_count.trailing_zeros() as usize)
}

fn encode_bucket(bucket: usize, depth: usize) -> [u8; INPUT_BYTES] {
    let shift = u32::BITS as usize - depth;
    (u32::try_from(bucket).expect("validated DPF bucket") << shift).to_be_bytes()
}

fn encode_server_key(party: bool, bucket_count: usize, key: &InnerKey) -> Result<Vec<u8>> {
    let depth = validate_domain(0, bucket_count)?;
    if key.s0s.len() != 1 || key.cws.len() != depth {
        bail!("Compact DPF library returned an unexpected key shape");
    }
    let capacity = KEY_HEADER_BYTES + OUTPUT_BYTES + depth * CORRECTION_WORD_BYTES + OUTPUT_BYTES;
    let mut output = Vec::with_capacity(capacity);
    output.extend_from_slice(KEY_MAGIC);
    output.push(u8::from(party));
    output.push(u8::try_from(depth).context("DPF depth does not fit in wire format")?);
    output.extend_from_slice(&[0, 0]);
    output.extend_from_slice(&(bucket_count as u64).to_be_bytes());
    output.extend_from_slice(&key.s0s[0]);
    for correction in &key.cws {
        if correction.v.0 != [0; OUTPUT_BYTES] {
            bail!("Compact DPF library returned an unsupported value correction");
        }
        output.extend_from_slice(&correction.s);
        output.push(u8::from(correction.tl) | (u8::from(correction.tr) << 1));
    }
    output.extend_from_slice(&key.cw_np1.0);
    debug_assert_eq!(output.len(), capacity);
    Ok(output)
}

fn decode_server_key(
    encoded: &[u8],
    expected_party: bool,
    expected_bucket_count: usize,
) -> Result<InnerKey> {
    if encoded.len() < KEY_HEADER_BYTES || &encoded[..4] != KEY_MAGIC {
        bail!("invalid Compact DPF key header");
    }
    let party = match encoded[4] {
        0 => false,
        1 => true,
        _ => bail!("invalid Compact DPF party index"),
    };
    if party != expected_party {
        bail!("Compact DPF key was sent to the wrong server");
    }
    let depth = encoded[5] as usize;
    if encoded[6..8] != [0, 0] {
        bail!("unsupported Compact DPF key flags");
    }
    let bucket_count = u64::from_be_bytes(encoded[8..16].try_into().expect("fixed header"));
    if bucket_count != expected_bucket_count as u64
        || validate_domain(0, expected_bucket_count)? != depth
    {
        bail!("Compact DPF key uses a different bucket domain");
    }
    let expected_len =
        KEY_HEADER_BYTES + OUTPUT_BYTES + depth * CORRECTION_WORD_BYTES + OUTPUT_BYTES;
    if encoded.len() != expected_len {
        bail!(
            "Compact DPF key has {} bytes, expected {expected_len}",
            encoded.len()
        );
    }

    let mut cursor = KEY_HEADER_BYTES;
    let root = encoded[cursor..cursor + OUTPUT_BYTES]
        .try_into()
        .expect("validated root length");
    cursor += OUTPUT_BYTES;
    let mut corrections = Vec::with_capacity(depth);
    for _ in 0..depth {
        let seed = encoded[cursor..cursor + OUTPUT_BYTES]
            .try_into()
            .expect("validated correction length");
        cursor += OUTPUT_BYTES;
        let flags = encoded[cursor];
        cursor += 1;
        if flags & !0b11 != 0 {
            bail!("invalid Compact DPF correction flags");
        }
        corrections.push(Cw {
            s: seed,
            // The selected two-party DPF construction never reads `v` during
            // point evaluation and its generator initializes it to zero.  Do
            // not put a redundant 16 zero bytes per tree level on the wire.
            v: ByteGroup([0; OUTPUT_BYTES]),
            tl: flags & 1 != 0,
            tr: flags & 2 != 0,
        });
    }
    let final_correction = encoded[cursor..cursor + OUTPUT_BYTES]
        .try_into()
        .expect("validated final correction length");
    Ok(Share {
        s0s: vec![root],
        cws: corrections,
        cw_np1: ByteGroup(final_correction),
    })
}

fn xor_array(target: &mut [u8; OUTPUT_BYTES], other: &[u8; OUTPUT_BYTES]) {
    for (target, other) in target.iter_mut().zip(other) {
        *target ^= other;
    }
}

#[cfg(test)]
mod tests {
    use rand::{rngs::StdRng, SeedableRng};

    use super::*;

    #[test]
    fn compact_subscription_matches_only_the_target_bucket_after_wire_roundtrip() {
        let bucket_count = 1 << 20;
        let target = 456_789;
        let registration =
            compact_registration(target, bucket_count, &mut StdRng::seed_from_u64(7)).unwrap();
        let mut servers = [
            CompactSubscriptionServer::new(0, bucket_count).unwrap(),
            CompactSubscriptionServer::new(1, bucket_count).unwrap(),
        ];
        for (server, key) in servers.iter_mut().zip(&registration.server_keys) {
            server.register(registration.id, key).unwrap();
        }
        for bucket in [0, target - 1, target, target + 1, bucket_count - 1] {
            let shares = servers
                .iter()
                .map(|server| server.evaluate_one(registration.id, bucket).unwrap())
                .collect::<Vec<_>>();
            assert_eq!(combine_compact(&shares).unwrap(), bucket == target);
        }
    }

    #[test]
    fn compact_server_rejects_the_other_partys_key() {
        let registration = compact_registration(9, 64, &mut StdRng::seed_from_u64(9)).unwrap();
        let mut server = CompactSubscriptionServer::new(0, 64).unwrap();
        assert!(server
            .register(registration.id, &registration.server_keys[1])
            .is_err());
    }

    #[test]
    fn compact_server_rejects_duplicate_ids_without_replacing_the_key() {
        let first = compact_registration(9, 64, &mut StdRng::seed_from_u64(12)).unwrap();
        let second = compact_registration(17, 64, &mut StdRng::seed_from_u64(13)).unwrap();
        let mut server = CompactSubscriptionServer::new(0, 64).unwrap();
        server.register(first.id, &first.server_keys[0]).unwrap();
        let before = server.evaluate_one(first.id, 9).unwrap();

        let error = server
            .register(first.id, &second.server_keys[0])
            .unwrap_err();

        assert!(error.to_string().contains("already registered"));
        assert_eq!(server.subscription_count(), 1);
        assert_eq!(server.evaluate_one(first.id, 9).unwrap(), before);
    }

    #[test]
    fn compact_combiner_rejects_two_results_from_the_same_party() {
        let registration = compact_registration(9, 64, &mut StdRng::seed_from_u64(10)).unwrap();
        let mut server = CompactSubscriptionServer::new(0, 64).unwrap();
        server
            .register(registration.id, &registration.server_keys[0])
            .unwrap();
        let share = server.evaluate_one(registration.id, 9).unwrap();
        assert!(combine_compact(&[share.clone(), share]).is_err());
    }

    #[test]
    fn dense_subscription_extends_to_three_or_more_servers() {
        let mut rng = StdRng::seed_from_u64(11);
        for server_count in 2..=6 {
            let registration = dense_registration(19, 64, server_count, &mut rng).unwrap();
            for bucket in [0, 18, 19, 20, 63] {
                let shares = registration
                    .server_keys
                    .iter()
                    .map(|key| evaluate_dense(key, bucket, 64).unwrap())
                    .collect::<Vec<_>>();
                assert_eq!(combine_dense(&shares).unwrap(), bucket == 19);
            }
        }
    }
}
