use super::Profile;

pub(super) fn sample_count(profile: Profile, snapshot_bytes: usize) -> usize {
    match profile {
        Profile::Quick if snapshot_bytes >= 64 * 1024 * 1024 => 3,
        Profile::Quick => 7,
        Profile::Full => 11,
    }
}
