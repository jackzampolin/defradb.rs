//! v0.15 document-storage migration to the v0.16 short-ID layout.

use std::collections::{HashMap, HashSet, VecDeque};

use acp::{RelationTuple, Relationship, Subject};
use cid::Cid;
use datastore::NamespaceView;
use defra_core::{Block, CrdtDelta};
use document::{DocID, Document};
use storage::corekv::{IterOptions, Key, Store};
use storage::keys::doc_id_index::encode_doc_short_id;
use storage::keys::{
    DataStoreKey, HeadstoreDocKey, HeadstorePriorityKey, InstanceType, PrimaryDataStoreKey,
};

use crate::collection::Collection;
use crate::database::DB;
use crate::error::{Error, Result};
use crate::index_manager::IndexManager;

const DOC_SHORT_ID_MIGRATION_MARKER: &[u8] = b"/migration/doc-short-id/v1";

#[derive(Debug)]
struct LegacyDocument {
    collection: Collection,
    old_doc_id: String,
    blob: Vec<u8>,
    version: Option<Vec<u8>>,
    deleted: Option<Vec<u8>>,
    canonical_doc_id: String,
    doc_short_id: u64,
    owned_block_cids: HashSet<Cid>,
}

impl<S: Store> DB<S> {
    /// Upgrade pre-v0.16 document storage to the genesis-CID/short-ID layout.
    ///
    /// Every key rewrite, identity mapping, ACP rewrite, and index rebuild is
    /// committed in one store transaction. Any malformed or incomplete legacy
    /// document aborts the open without publishing a partial migration.
    pub(crate) async fn maybe_migrate_v015_document_storage(&self) -> Result<()> {
        let collections = self
            .list_collections()?
            .into_iter()
            .map(|name| {
                self.get_collection(&name)?.ok_or_else(|| {
                    Error::Other(format!(
                        "collection '{name}' disappeared while preparing store migration"
                    ))
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let txn = self.new_txn(false).await?;
        let migrated_count = {
            let datastore = txn.datastore()?;
            let systemstore = txn.systemstore()?;
            if systemstore
                .has(DOC_SHORT_ID_MIGRATION_MARKER)
                .await
                .map_err(Error::Storage)?
            {
                0
            } else {
                let mut legacy = collect_legacy_documents(&datastore, &collections).await?;
                if legacy.is_empty() {
                    0
                } else {
                    let headstore = txn.headstore()?;
                    let blockstore = txn.blockstore()?;
                    let acpstore = txn.acpstore()?;
                    let peerstore = txn.peerstore()?;

                    resolve_legacy_identities(&systemstore, &headstore, &blockstore, &mut legacy)
                        .await?;
                    migrate_document_keys(&datastore, &headstore, &legacy).await?;
                    migrate_acp_keys(&acpstore, &legacy).await?;
                    migrate_auxiliary_doc_id_keys(&datastore, &systemstore, &peerstore, &legacy)
                        .await?;
                    rebuild_indexes(&datastore, &systemstore, &collections).await?;

                    systemstore
                        .set(DOC_SHORT_ID_MIGRATION_MARKER, b"complete")
                        .await
                        .map_err(Error::Storage)?;
                    legacy.len()
                }
            }
        };

        txn.commit().await?;
        if migrated_count > 0 {
            tracing::info!(
                migrated_documents = migrated_count,
                "migrated legacy document storage to doc short IDs"
            );
        }
        Ok(())
    }
}

async fn collect_legacy_documents(
    datastore: &NamespaceView,
    collections: &[Collection],
) -> Result<Vec<LegacyDocument>> {
    let mut result = Vec::new();
    for collection in collections {
        let prefix = legacy_doc_collection_prefix(collection.collection_id());
        let mut iter = datastore
            .iterator(IterOptions::new().with_prefix(prefix.clone()))
            .await
            .map_err(Error::Storage)?;
        while let Some(pair) = iter.next().await.map_err(Error::Storage)? {
            let suffix = &pair.key[prefix.len()..];
            if suffix.ends_with(b"/v") || suffix.contains(&b'/') {
                continue;
            }
            let Ok(old_doc_id) = std::str::from_utf8(suffix) else {
                continue;
            };
            if old_doc_id.parse::<DocID>().is_err() {
                continue;
            }

            let version = datastore
                .get(&legacy_version_key(collection.collection_id(), old_doc_id))
                .await
                .map_err(Error::Storage)?;
            let deleted = datastore
                .get(&legacy_deleted_key(collection.collection_id(), old_doc_id))
                .await
                .map_err(Error::Storage)?;
            Document::from_cbor(&pair.value)
                .map_err(|error| Error::document_at_key(&pair.key, error))?;

            result.push(LegacyDocument {
                collection: collection.clone(),
                old_doc_id: old_doc_id.to_string(),
                blob: pair.value,
                version,
                deleted,
                canonical_doc_id: String::new(),
                doc_short_id: 0,
                owned_block_cids: HashSet::new(),
            });
        }
        iter.close().await.map_err(Error::Storage)?;
    }
    Ok(result)
}

async fn resolve_legacy_identities(
    systemstore: &NamespaceView,
    headstore: &NamespaceView,
    blockstore: &NamespaceView,
    documents: &mut [LegacyDocument],
) -> Result<()> {
    for document in documents {
        let (canonical_doc_id, owned_block_cids) =
            resolve_legacy_block_graph(headstore, blockstore, &document.old_doc_id).await?;
        let collection_short_id = document.collection.resolved_root_id();

        let existing_old =
            crate::doc_id_map::get_doc_ref(systemstore, &document.old_doc_id).await?;
        let existing_canonical =
            crate::doc_id_map::get_doc_ref(systemstore, &canonical_doc_id).await?;
        let doc_short_id = match (existing_old, existing_canonical) {
            (Some(old_ref), Some(canonical_ref)) if old_ref != canonical_ref => {
                return Err(Error::Other(format!(
                    "legacy document '{}' and canonical document '{}' resolve to different identities",
                    document.old_doc_id, canonical_doc_id
                )))
            }
            (Some(doc_ref), _) | (_, Some(doc_ref)) => {
                if doc_ref.collection_short_id != collection_short_id {
                    return Err(Error::Other(format!(
                        "document '{}' identity belongs to collection short ID {}, expected {}",
                        document.old_doc_id, doc_ref.collection_short_id, collection_short_id
                    )));
                }
                doc_ref.doc_short_id
            }
            (None, None) => crate::doc_id_map::next_doc_short_id(systemstore).await?,
        };

        if let Some(existing_doc_id) =
            crate::doc_id_map::get_doc_id(systemstore, doc_short_id).await?
        {
            if existing_doc_id != canonical_doc_id && existing_doc_id != document.old_doc_id {
                return Err(Error::Other(format!(
                    "document short ID {doc_short_id} already belongs to '{existing_doc_id}', not legacy document '{}'",
                    document.old_doc_id
                )));
            }
        }
        crate::doc_id_map::ensure_doc_short_id_sequence_at_least(systemstore, doc_short_id).await?;

        crate::doc_id_map::set_doc_id_mapping(
            systemstore,
            collection_short_id,
            doc_short_id,
            &canonical_doc_id,
        )
        .await?;
        if document.old_doc_id != canonical_doc_id {
            crate::doc_id_map::set_doc_id_alias(
                systemstore,
                collection_short_id,
                doc_short_id,
                &document.old_doc_id,
            )
            .await?;
        }
        for cid in &owned_block_cids {
            crate::doc_id_map::set_block_doc_id_mapping(
                systemstore,
                &cid.to_string(),
                &canonical_doc_id,
            )
            .await?;
        }

        document.canonical_doc_id = canonical_doc_id;
        document.doc_short_id = doc_short_id;
        document.owned_block_cids = owned_block_cids;
    }
    Ok(())
}

async fn resolve_legacy_block_graph(
    headstore: &NamespaceView,
    blockstore: &NamespaceView,
    old_doc_id: &str,
) -> Result<(String, HashSet<Cid>)> {
    let prefix = legacy_head_field_prefix(old_doc_id, "C");
    let mut iter = headstore
        .iterator(IterOptions::new().with_prefix(prefix.clone()))
        .await
        .map_err(Error::Storage)?;
    let mut queue = VecDeque::new();
    while let Some(pair) = iter.next().await.map_err(Error::Storage)? {
        let cid = String::from_utf8(pair.key[prefix.len()..].to_vec())
            .map_err(|error| Error::text_decode("legacy composite head CID", error))?
            .parse::<Cid>()
            .map_err(|error| Error::Serialization(format!("legacy composite head CID: {error}")))?;
        queue.push_back(cid);
    }
    iter.close().await.map_err(Error::Storage)?;
    if queue.is_empty() {
        return Err(Error::Other(format!(
            "legacy document '{old_doc_id}' has no composite heads"
        )));
    }

    let mut visited = HashSet::new();
    let mut genesis = HashSet::new();
    while let Some(cid) = queue.pop_front() {
        if !visited.insert(cid) {
            continue;
        }
        let Some(bytes) = blockstore
            .get(&cid.to_bytes())
            .await
            .map_err(Error::Storage)?
        else {
            return Err(Error::Other(format!(
                "legacy document '{old_doc_id}' references missing block {cid}"
            )));
        };
        let block = Block::from_dag_cbor(&bytes)
            .map_err(|error| Error::Serialization(format!("decode legacy block {cid}: {error}")))?;
        if matches!(block.delta, CrdtDelta::Composite(_))
            && block.heads.as_deref().is_none_or(<[Cid]>::is_empty)
        {
            genesis.insert(cid);
        }
        if let Some(heads) = block.heads {
            queue.extend(heads);
        }
        if let Some(links) = block.links {
            queue.extend(links.into_iter().map(|link| link.link));
        }
        if let Some(cid) = block.encryption {
            visited.insert(cid);
        }
        if let Some(cid) = block.signature {
            visited.insert(cid);
        }
    }

    if genesis.len() != 1 {
        return Err(Error::Other(format!(
            "legacy document '{old_doc_id}' reaches {} genesis composite blocks; expected exactly one",
            genesis.len()
        )));
    }
    let genesis_cid = *genesis.iter().next().expect("length checked");
    Ok((db_blocks::derive_doc_id(&genesis_cid), visited))
}

async fn migrate_document_keys(
    datastore: &NamespaceView,
    headstore: &NamespaceView,
    documents: &[LegacyDocument],
) -> Result<()> {
    for document in documents {
        let collection_id = document.collection.collection_id();
        move_exact(
            datastore,
            &legacy_doc_key(collection_id, &document.old_doc_id),
            &storage::keys::doc_key(collection_id, document.doc_short_id),
            Some(&document.blob),
        )
        .await?;
        move_optional_exact(
            datastore,
            &legacy_version_key(collection_id, &document.old_doc_id),
            &document.collection.version_key(document.doc_short_id),
            document.version.as_deref(),
        )
        .await?;
        move_optional_exact(
            datastore,
            &legacy_deleted_key(collection_id, &document.old_doc_id),
            &storage::keys::deleted_doc_key(collection_id, document.doc_short_id),
            document.deleted.as_deref(),
        )
        .await?;

        for instance in [
            InstanceType::Value,
            InstanceType::Priority,
            InstanceType::Deleted,
        ] {
            let mut old_prefix = DataStoreKey::collection_instance_prefix(
                document.collection.resolved_root_id(),
                instance,
            );
            old_prefix.extend_from_slice(document.old_doc_id.as_bytes());
            old_prefix.push(b'/');
            let new_prefix = DataStoreKey::document_prefix(
                document.collection.resolved_root_id(),
                instance,
                document.doc_short_id,
            );
            move_prefix(datastore, &old_prefix, &new_prefix).await?;
        }

        let old_primary = format!(
            "/{}/pk/{}",
            document.collection.resolved_root_id(),
            document.old_doc_id
        );
        move_optional_exact(
            datastore,
            old_primary.as_bytes(),
            &PrimaryDataStoreKey::new(
                document.collection.resolved_root_id(),
                document.doc_short_id,
            )
            .bytes(),
            None,
        )
        .await?;

        move_prefix(
            headstore,
            &legacy_head_document_prefix(&document.old_doc_id),
            &HeadstoreDocKey::document_prefix(document.doc_short_id),
        )
        .await?;
        move_prefix(
            headstore,
            &legacy_priority_document_prefix(&document.old_doc_id),
            &HeadstorePriorityKey::document_prefix(document.doc_short_id),
        )
        .await?;
    }
    move_crdt_keys(datastore, documents).await?;
    Ok(())
}

async fn move_crdt_keys(datastore: &NamespaceView, documents: &[LegacyDocument]) -> Result<()> {
    let replacements = documents
        .iter()
        .map(|document| {
            (
                document.old_doc_id.as_bytes().to_vec(),
                encode_doc_short_id(document.doc_short_id),
            )
        })
        .collect::<HashMap<_, _>>();
    let mut iter = datastore
        .iterator(IterOptions::new().with_prefix(b"/data/".to_vec()))
        .await
        .map_err(Error::Storage)?;
    let mut moves = Vec::new();
    while let Some(pair) = iter.next().await.map_err(Error::Storage)? {
        let Some((doc_id_start, doc_id_end)) = crdt_doc_id_range(&pair.key) else {
            continue;
        };
        let Some(replacement) = replacements.get(&pair.key[doc_id_start..doc_id_end]) else {
            continue;
        };
        let mut new_key = pair.key[..doc_id_start].to_vec();
        new_key.extend_from_slice(replacement);
        new_key.extend_from_slice(&pair.key[doc_id_end..]);
        moves.push((pair.key, new_key, pair.value));
    }
    iter.close().await.map_err(Error::Storage)?;
    apply_moves(datastore, moves).await
}

fn crdt_doc_id_range(key: &[u8]) -> Option<(usize, usize)> {
    let suffix = key.strip_prefix(b"/data/")?;
    let schema_end = suffix.iter().position(|byte| *byte == b'/')?;
    let doc_id_start = b"/data/".len() + schema_end + 1;
    let doc_id_len = key[doc_id_start..].iter().position(|byte| *byte == b'/')?;
    Some((doc_id_start, doc_id_start + doc_id_len))
}

async fn rebuild_indexes(
    datastore: &NamespaceView,
    systemstore: &NamespaceView,
    collections: &[Collection],
) -> Result<()> {
    for collection in collections {
        let manager =
            IndexManager::from_collection(collection.resolved_root_id(), collection.schema())?;
        let index_names = manager
            .get_indexes()
            .into_iter()
            .map(|index| index.name.clone())
            .collect::<Vec<_>>();
        for name in &index_names {
            if let Some(index) = manager.get_index(name) {
                index
                    .remove_all(&mut datastore.clone())
                    .await
                    .map_err(Error::Storage)?;
            }
        }

        let documents = collection
            .get_all_with_datastore_short_ids(datastore, systemstore, false)
            .await?;
        for (doc_short_id, doc, _) in documents {
            manager
                .on_document_create_merge(
                    datastore,
                    systemstore,
                    &doc,
                    doc_short_id,
                    collection.schema(),
                )
                .await?;
        }
    }
    Ok(())
}

async fn migrate_acp_keys(acpstore: &NamespaceView, documents: &[LegacyDocument]) -> Result<()> {
    let replacements = documents
        .iter()
        .map(|document| {
            (
                document.old_doc_id.clone(),
                document.canonical_doc_id.clone(),
            )
        })
        .collect::<HashMap<_, _>>();
    migrate_local_acp(acpstore, &replacements).await?;
    migrate_zanzibar_acp(acpstore, &replacements).await
}

async fn migrate_auxiliary_doc_id_keys(
    datastore: &NamespaceView,
    systemstore: &NamespaceView,
    peerstore: &NamespaceView,
    documents: &[LegacyDocument],
) -> Result<()> {
    let replacements = documents
        .iter()
        .map(|document| {
            (
                document.old_doc_id.clone(),
                document.canonical_doc_id.clone(),
            )
        })
        .collect::<HashMap<_, _>>();

    move_terminal_doc_ids(datastore, b"/se/", &replacements, false).await?;
    move_terminal_doc_ids(systemstore, b"/p2p/document/", &replacements, false).await?;
    move_terminal_doc_ids(peerstore, b"/rep/retry/doc/", &replacements, true).await?;
    move_terminal_doc_ids(peerstore, b"/se-retry/", &replacements, false).await
}

async fn move_terminal_doc_ids(
    store: &NamespaceView,
    prefix: &[u8],
    replacements: &HashMap<String, String>,
    rewrite_push_retry: bool,
) -> Result<()> {
    let mut iter = store
        .iterator(IterOptions::new().with_prefix(prefix.to_vec()))
        .await
        .map_err(Error::Storage)?;
    let mut moves = Vec::new();
    while let Some(pair) = iter.next().await.map_err(Error::Storage)? {
        let Some(segment_start) = pair.key.iter().rposition(|byte| *byte == b'/') else {
            continue;
        };
        let Ok(old_doc_id) = std::str::from_utf8(&pair.key[segment_start + 1..]) else {
            continue;
        };
        let Some(canonical) = replacements.get(old_doc_id) else {
            continue;
        };

        let mut new_key = pair.key[..segment_start + 1].to_vec();
        new_key.extend_from_slice(canonical.as_bytes());
        let mut value = pair.value;
        if rewrite_push_retry {
            if let Ok(mut retry) = storage::stores::PersistedPushRetry::from_bytes(&value) {
                if retry.doc_id == old_doc_id {
                    retry.doc_id = canonical.clone();
                    value = retry.to_bytes().map_err(Error::Serialization)?;
                }
            }
        }
        moves.push((pair.key, new_key, value));
    }
    iter.close().await.map_err(Error::Storage)?;
    apply_moves(store, moves).await
}

async fn migrate_local_acp(
    acpstore: &NamespaceView,
    replacements: &HashMap<String, String>,
) -> Result<()> {
    let mut iter = acpstore
        .iterator(IterOptions::new().with_prefix(b"/acp/".to_vec()))
        .await
        .map_err(Error::Storage)?;
    let mut moves = Vec::new();
    while let Some(pair) = iter.next().await.map_err(Error::Storage)? {
        let tuple: RelationTuple = serde_json::from_slice(&pair.value)
            .map_err(|error| Error::Serialization(format!("decode legacy ACP tuple: {error}")))?;
        let Some(canonical) = replacements.get(tuple.doc_id()) else {
            continue;
        };
        let migrated = RelationTuple::try_new(
            tuple.subject().clone(),
            tuple.relation(),
            tuple.collection_id(),
            canonical,
        )
        .map_err(|error| Error::Other(format!("migrate ACP tuple: {error}")))?;
        moves.push((
            pair.key,
            migrated.storage_key().into_bytes(),
            serde_json::to_vec(&migrated).map_err(|error| {
                Error::Serialization(format!("encode migrated ACP tuple: {error}"))
            })?,
        ));
    }
    iter.close().await.map_err(Error::Storage)?;
    apply_moves(acpstore, moves).await?;

    let mut sentinels = acpstore
        .iterator(IterOptions::new().with_prefix(b"/acp-reg/".to_vec()))
        .await
        .map_err(Error::Storage)?;
    let mut moves = Vec::new();
    while let Some(pair) = sentinels.next().await.map_err(Error::Storage)? {
        let mut new_key = pair.key.clone();
        for (old, canonical) in replacements {
            if let Some(offset) = find_path_segment(&new_key, old.as_bytes()) {
                new_key.splice(offset..offset + old.len(), canonical.bytes());
                break;
            }
        }
        if new_key != pair.key {
            moves.push((pair.key, new_key, pair.value));
        }
    }
    sentinels.close().await.map_err(Error::Storage)?;
    apply_moves(acpstore, moves).await
}

async fn migrate_zanzibar_acp(
    acpstore: &NamespaceView,
    replacements: &HashMap<String, String>,
) -> Result<()> {
    let mut iter = acpstore
        .iterator(IterOptions::new().with_prefix(b"/zanzibar/".to_vec()))
        .await
        .map_err(Error::Storage)?;
    let mut moves = Vec::new();
    while let Some(pair) = iter.next().await.map_err(Error::Storage)? {
        let Some(rel_offset) = find_subslice(&pair.key, b"/rel/") else {
            continue;
        };
        let mut relationship: Relationship =
            serde_json::from_slice(&pair.value).map_err(|error| {
                Error::Serialization(format!("decode legacy Zanzibar relationship: {error}"))
            })?;
        let mut changed = false;
        if let Some(canonical) = replacements.get(&relationship.object_id) {
            relationship.object_id = canonical.clone();
            changed = true;
        }
        if let Subject::EntitySet { object_id, .. } = &mut relationship.subject {
            if let Some(canonical) = replacements.get(object_id) {
                *object_id = canonical.clone();
                changed = true;
            }
        }
        if !changed {
            continue;
        }
        let prefix = &pair.key[..rel_offset];
        let new_key = [prefix, relationship.storage_key().as_bytes()].concat();
        moves.push((
            pair.key,
            new_key,
            serde_json::to_vec(&relationship).map_err(|error| {
                Error::Serialization(format!("encode migrated Zanzibar relationship: {error}"))
            })?,
        ));
    }
    iter.close().await.map_err(Error::Storage)?;
    apply_moves(acpstore, moves).await
}

async fn move_prefix(store: &NamespaceView, old_prefix: &[u8], new_prefix: &[u8]) -> Result<()> {
    let mut iter = store
        .iterator(IterOptions::new().with_prefix(old_prefix.to_vec()))
        .await
        .map_err(Error::Storage)?;
    let mut moves = Vec::new();
    while let Some(pair) = iter.next().await.map_err(Error::Storage)? {
        let mut new_key = new_prefix.to_vec();
        new_key.extend_from_slice(&pair.key[old_prefix.len()..]);
        moves.push((pair.key, new_key, pair.value));
    }
    iter.close().await.map_err(Error::Storage)?;
    apply_moves(store, moves).await
}

async fn move_exact(
    store: &NamespaceView,
    old_key: &[u8],
    new_key: &[u8],
    known_value: Option<&[u8]>,
) -> Result<()> {
    let value = match known_value {
        Some(value) => Some(value.to_vec()),
        None => store.get(old_key).await.map_err(Error::Storage)?,
    };
    if let Some(value) = value {
        apply_moves(store, vec![(old_key.to_vec(), new_key.to_vec(), value)]).await?;
    }
    Ok(())
}

async fn move_optional_exact(
    store: &NamespaceView,
    old_key: &[u8],
    new_key: &[u8],
    known_value: Option<&[u8]>,
) -> Result<()> {
    move_exact(store, old_key, new_key, known_value).await
}

async fn apply_moves(store: &NamespaceView, moves: Vec<(Vec<u8>, Vec<u8>, Vec<u8>)>) -> Result<()> {
    for (old_key, new_key, value) in moves {
        if old_key == new_key {
            continue;
        }
        if let Some(existing) = store.get(&new_key).await.map_err(Error::Storage)? {
            if existing != value {
                return Err(Error::Other(format!(
                    "store migration key collision at {:?}",
                    new_key
                )));
            }
        } else {
            store.set(&new_key, &value).await.map_err(Error::Storage)?;
        }
        store.delete(&old_key).await.map_err(Error::Storage)?;
    }
    Ok(())
}

fn legacy_doc_collection_prefix(collection_id: &str) -> Vec<u8> {
    format!("/d/{collection_id}/").into_bytes()
}

fn legacy_doc_key(collection_id: &str, doc_id: &str) -> Vec<u8> {
    format!("/d/{collection_id}/{doc_id}").into_bytes()
}

fn legacy_version_key(collection_id: &str, doc_id: &str) -> Vec<u8> {
    format!("/d/{collection_id}/{doc_id}/v").into_bytes()
}

fn legacy_deleted_key(collection_id: &str, doc_id: &str) -> Vec<u8> {
    format!("/del/{collection_id}/{doc_id}").into_bytes()
}

fn legacy_head_document_prefix(doc_id: &str) -> Vec<u8> {
    format!("/d/{doc_id}/").into_bytes()
}

fn legacy_head_field_prefix(doc_id: &str, field: &str) -> Vec<u8> {
    format!("/d/{doc_id}/{field}/").into_bytes()
}

fn legacy_priority_document_prefix(doc_id: &str) -> Vec<u8> {
    format!("/p/{doc_id}/").into_bytes()
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() {
        return Some(0);
    }
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn find_path_segment(key: &[u8], segment: &[u8]) -> Option<usize> {
    let needle = [b"/".as_slice(), segment, b"/"].concat();
    find_subslice(key, &needle)
        .map(|offset| offset + 1)
        .or_else(|| {
            let suffix = [b"/".as_slice(), segment].concat();
            key.ends_with(&suffix).then_some(key.len() - segment.len())
        })
}
