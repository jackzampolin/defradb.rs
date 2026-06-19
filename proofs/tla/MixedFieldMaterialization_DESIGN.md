# MixedFieldMaterialization — Design & Anchors

Models the #1048 mixed-field materialization hazard: the same document carries an
LWW field and a counter field, and a merge of one field must not re-materialize
the whole document from a stale snapshot that clobbers the other field.

## What It Abstracts

| Model element | Code / behavior |
|---|---|
| `name` | materialized LWW field value (`crates/crdt/src/lww.rs`) |
| `views` | materialized counter field value (`crates/crdt/src/counter.rs`) |
| `RemoteSnap` | a merge reads the materialized document before applying one field delta |
| `MergeMode = "WholeDoc"` | buggy design: commit the whole stale snapshot with one field changed |
| `MergeMode = "Componentwise"` | required design: merge updates only the target field component, preserving the rest of the current document |
| `expectedName`, `expectedViews` | independent oracle for the componentwise product proven in `DefraConvergence.MixedField` |

## Verdicts

| Config | Meaning | Expected |
|---|---|---|
| `MC_MixedFieldMaterialization_Red_WholeDoc.cfg` | stale whole-document commit can clobber the other field | RED |
| `MC_MixedFieldMaterialization_Green.cfg` | componentwise materialization preserves both fields | GREEN |

## Behavioral Binding

The model is bound to Rust by:

- `partition::convergence_concurrent_mixed_lww_and_counter_fields_merge`
- `partition::convergence_restart_mixed_lww_and_counter_fields_merge`
- `partition::convergence_mixed_lww_and_counter_3node_full_mesh`

The 3-node test is the strongest live behavioral witness: node0 writes the LWW
field (`name=alice`) while node1 and node2 each increment the counter
(`views += 10` and `views += 7`). All three replicas must converge to the exact
mixed product state (`name=alice, views=17`) after their commit DAGs match — a
concurrent LWW write plus two concurrent counter writes folding to one document.
