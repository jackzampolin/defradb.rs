# Iroh Integration Guide

This guide covers the application-side changes needed to consume the iroh transport updates released in `v0.9.7`.

## Who Should Read This

- Apps embedding Defra via `embedded`
- Services using `defra-node`
- CLI-based deployments configuring iroh transport
- FFI/mobile clients such as Amy

## What Changed

- Defra now uses `iroh 0.97`, `iroh-gossip 0.97`, and `iroh-blobs 0.99`.
- Relay configuration is now explicit: `default`, `disabled`, or `custom`.
- Discovery configuration is now explicit: `n0`, `disabled`, or custom DNS + pkarr relay.
- Public iroh peer addresses now support endpoint tickets and a connectable `host:port/p2p/<endpoint-id>` form.
- Mobile/networked clients can now notify the transport when network conditions change.
- Bind behavior is stricter and more useful for multi-homed hosts.

## Upgrade Checklist

1. Update both peers together when using iroh.
2. Stop assuming the only usable peer address is `iroh://<endpoint-id>`.
3. Enable relay for mobile or NATed peers unless you have a specific reason not to.
4. Set `bind_addr` on multi-interface hosts if you want to avoid advertising the wrong LAN.
5. Call `network_change()` after sleep/wake or network transitions on mobile clients.
6. Re-run the iroh integration tests before rolling out.

## Address Formats

Defra now accepts all of these for iroh peer dialing:

- endpoint ticket strings: `endpoint...`
- direct address with peer id: `<host>:<port>/p2p/<endpoint-id>`
- legacy direct form: `<endpoint-id>@<host>:<port>`
- discovery-only forms: `<endpoint-id>` and `iroh://<endpoint-id>`

Important:

- `listen_addresses()` and `/p2p/info` now prefer a directly dialable `host:port/p2p/<endpoint-id>` as the first address when one is available.
- An endpoint ticket is also exposed when available.
- The bare endpoint id is still included as an identity-only fallback.
- Do not dial only the bare endpoint id if discovery is disabled and you do not already have relay/direct hints.

## Recommended Defaults

### Mobile or NATed Clients

- Use `relay_mode = default` or a custom relay set.
- Use `discovery = n0` unless you operate your own DNS/pkarr infrastructure.
- Store the peer's stable endpoint id separately from its latest dialable address.
- After network transitions, call `network_change()` and then reconnect or resync.

### Multi-Homed Servers

- Set `bind_addr` to the specific interface you want advertised.
- For Tailscale-only connectivity, bind to the Tailscale IP.
- Do not rely on interface auto-selection if the host has bridge, Thunderbolt, link-local, Wi-Fi, and mesh interfaces all present.

### Dedicated Iroh Infrastructure

- If you operate your own discovery stack, configure custom discovery with:
  - `origin_domain`
  - `pkarr_relay_url`
- If you operate custom relays, configure `relay_mode = custom([...])`.
- Prefer more than one relay URL for production redundancy.

## Surface-Specific Changes

### `p2p::iroh::IrohEndpointConfig`

- `relay_url` has been replaced by `relay_mode`.
- `discovery: bool` has been replaced by `IrohDiscoveryConfig`.
- `bind_addr` works even when the port is ephemeral.

### `defra-node::P2PConfig`

Use:

- `bind_addr`
- `relay_mode`
- `discovery`
- `secret_key_path`

Recommended:

- Keep `secret_key_path` stable across restarts.
- Keep `load_persisted_collections = true` when you want subscriptions and replicator state to survive restarts.

### CLI Config

New iroh fields:

- `iroh_relay_mode`
- `iroh_relay_urls`
- `iroh_discovery_origin_domain`
- `iroh_pkarr_relay_url`

Legacy compatibility fields are still accepted:

- `iroh_relay_url`
- `iroh_discovery`

### Embedded

`embedded::IrohConfig` now uses:

- `bind_addr`
- `bind_port`
- `relay_mode`
- `discovery`
- `secret_key_path`

`P2POperations` also exposes:

- `notify_network_change()`

### FFI / Mobile

New `NodeInitOptions` fields:

- `iroh_relay_mode`
- `iroh_relay_urls_json`
- `iroh_discovery_origin_domain`
- `iroh_pkarr_relay_url`

New FFI entry points:

- `p2p_notify_network_change(...)`
- `defra_mobile_notify_network_change(...)`

Mobile guidance:

- Call the notify function when the app resumes, the radio changes, the interface changes, or the OS reports a path transition.
- After notifying, reconnect or resync explicitly if your app flow requires an active session.

## Rollout Advice

### Update Both Sides Together

The iroh sync path now includes selective block fetch behavior introduced after `v0.9.6`.

- Treat mixed-version iroh peers as unsupported for rollout purposes.
- Upgrade server and client together.
- If you have multiple app repos, stage the Defra upgrade first, then roll the client reconnect logic and config updates immediately after.

### Preserve Peer Identity

- Keep the iroh secret key on a persistent path outside disposable data directories.
- If you wipe app data but keep the key, peers continue to recognize the same node identity.

## App-Level Pitfalls To Avoid

- Do not disable relay on phones unless you control both network reachability and wake behavior.
- Do not use bare endpoint ids as the only stored address when discovery is disabled.
- Do not ignore `bind_addr` on multi-interface hosts.
- Do not assume the first iroh address will always be a bare endpoint id.
- Do not skip `network_change()` on mobile after sleep/wake or network handoff.
- Do not roll out only one side of an iroh deployment when selective-fetch behavior matters.

## Suggested Validation

Run at least these checks before shipping:

```bash
cargo test -p p2p --features iroh-transport --lib
cargo test -p embedded --features iroh --test iroh_smoke
cargo test -p integration-test --test p2p_iroh -- sync::
cargo test -p integration-test --test p2p_iroh -- replication::
```

For app integrations, also verify:

- `p2p_info` returns a connectable first iroh address
- reconnect after sleep/wake succeeds
- reconnect after Wi-Fi <-> cellular or Wi-Fi <-> Tailscale transition succeeds
- relay-assisted dialing works when direct UDP is unavailable
- multi-interface hosts advertise the intended interface only

## Known Test Harness Issue

The broader `connection::` iroh integration slice still has an existing harness problem where some spawned nodes are built without the iroh feature and fail with:

`invalid transport type: iroh transport not enabled`

That issue is outside the transport changes in this release.
