//! Coverage matrix over the registry.
//!
//! The "diff" the harness defends: every family the README claims is *Modeled*
//! must appear in `registry::PROPERTIES` with at least one binding tier. A model
//! that lands without a conformance hook fails `every_modeled_family_is_bound`.

use crate::registry::{Property, Tier, PROPERTIES};

/// The Modeled families from `proofs/README.md`. Kept here so the assertion is
/// self-contained; if the README grows a family, this list and `PROPERTIES`
/// must both grow or the test fails.
pub const MODELED_FAMILIES: &[&str] = &[
    "B3 filtered replication",
    "DAG convergence (partition / eviction / restart)",
    "CRDT merge laws",
    "Replicator lifecycle (no-loss / resume)",
    "Multi-instance claim",
    "Block integrity / signatures",
    "KMS key distribution",
    "Management-channel auth (NAC gate)",
    "ACP soundness + revocation + dual-path commits",
    "Storage SSI serializability (point + range/scan carve-out)",
    "P2P explicit-replay capability gate",
    "NAC lifecycle privilege-escalation",
    "Transaction & merge-queue concurrency",
    "Document materialization status convergence",
    "JWT issuer / algorithm binding",
    "CID content-addressing determinism + Block canonicalization",
    "Deferred-ACP overlay consistency",
    "Index-maintenance consistency",
    "Order-preserving key encoding",
];

pub fn properties_for(family: &'static str) -> impl Iterator<Item = &'static Property> {
    PROPERTIES.iter().filter(move |p| p.family == family)
}

pub fn count(tier: Tier) -> usize {
    PROPERTIES
        .iter()
        .filter(|p| p.tiers.contains(&tier))
        .count()
}

/// A `family -> bound?` summary line for each modeled family.
pub fn summary() -> Vec<(&'static str, bool)> {
    MODELED_FAMILIES
        .iter()
        .map(|&fam| (fam, properties_for(fam).next().is_some()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_modeled_family_is_bound() {
        let unbound: Vec<_> = MODELED_FAMILIES
            .iter()
            .filter(|&&fam| properties_for(fam).next().is_none())
            .collect();
        assert!(
            unbound.is_empty(),
            "modeled families with no conformance binding in registry::PROPERTIES: {unbound:?}"
        );
    }

    #[test]
    fn every_property_has_a_tier() {
        let untiered: Vec<_> = PROPERTIES.iter().filter(|p| p.tiers.is_empty()).collect();
        assert!(
            untiered.is_empty(),
            "properties with no tier: {:?}",
            untiered.iter().map(|p| p.name).collect::<Vec<_>>()
        );
    }

    #[test]
    fn registry_covers_every_modeled_family() {
        // No stray registry family that isn't a declared modeled family.
        let stray: Vec<_> = PROPERTIES
            .iter()
            .filter(|p| !MODELED_FAMILIES.contains(&p.family))
            .map(|p| p.family)
            .collect();
        assert!(
            stray.is_empty(),
            "registry families not in MODELED_FAMILIES: {stray:?}"
        );
    }
}
