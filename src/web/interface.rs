//! Defines all the structs needed to interact with the Rojo Serve API. This is
//! useful for tests to be able to use the same data structures as the
//! implementation.

use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
};

use rbx_dom_weak::{
    types::{Ref, Variant, VariantType},
    Ustr, UstrMap,
};
use serde::{Deserialize, Serialize};
use strum::Display;

use crate::{
    session_id::SessionId,
    snapshot::{
        AppliedPatchSet, InstanceMetadata as RojoInstanceMetadata, InstanceWithMeta, RojoTree,
    },
};

/// Server version to report over the API, not exposed outside this crate.
pub(crate) const SERVER_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Current protocol version, which is required to match.
pub const PROTOCOL_VERSION: u64 = 5;

/// Message returned by Rojo API when a change has occurred.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SubscribeMessage<'a> {
    pub removed: Vec<Ref>,
    pub added: HashMap<Ref, Instance<'a>>,
    pub updated: Vec<InstanceUpdate>,
}

impl<'a> SubscribeMessage<'a> {
    pub(crate) fn from_patch_update(tree: &'a RojoTree, patch: AppliedPatchSet) -> Self {
        let removed = patch.removed;

        let mut added = HashMap::new();
        for id in patch.added {
            // Applied patches are queued as bare ids and only turned into
            // messages here, at send time, long after the tree lock was
            // released. Anything that changed the tree in the meantime (a file
            // written and then deleted, a project reload replacing a subtree)
            // can have removed an instance this patch still lists as added.
            //
            // Skipping the id instead of panicking is safe: the patch that
            // removed the instance is already queued behind this one and will
            // be delivered too, and the client treats a removal of an unknown
            // id as a no-op. Its descendants are gone with it, so they are
            // skipped as well.
            let Some(instance) = tree.get_instance(id) else {
                log::warn!(
                    "Skipping instance {:?} in an outgoing patch: it is no longer in the tree",
                    id
                );
                continue;
            };

            added.insert(id, Instance::from_rojo_instance(instance));

            for instance in tree.descendants(id) {
                added.insert(instance.id(), Instance::from_rojo_instance(instance));
            }
        }

        let updated = patch
            .updated
            .into_iter()
            .map(|update| {
                let changed_metadata = update
                    .changed_metadata
                    .as_ref()
                    .map(InstanceMetadata::from_rojo_metadata);

                let changed_properties = update
                    .changed_properties
                    .into_iter()
                    .filter(|(_key, value)| property_filter(value.as_ref()))
                    .collect();

                InstanceUpdate {
                    id: update.id,
                    changed_name: update.changed_name,
                    changed_class_name: update.changed_class_name,
                    changed_properties,
                    changed_metadata,
                }
            })
            .collect();

        Self {
            removed,
            added,
            updated,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InstanceUpdate {
    pub id: Ref,
    pub changed_name: Option<String>,
    pub changed_class_name: Option<Ustr>,

    // TODO: Transform from UstrMap<String, Option<_>> to something else, since
    // null will get lost when decoding from JSON in some languages.
    #[serde(default)]
    pub changed_properties: UstrMap<Option<Variant>>,
    pub changed_metadata: Option<InstanceMetadata>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InstanceMetadata {
    pub ignore_unknown_instances: bool,
}

impl InstanceMetadata {
    pub(crate) fn from_rojo_metadata(meta: &RojoInstanceMetadata) -> Self {
        Self {
            ignore_unknown_instances: meta.ignore_unknown_instances,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct Instance<'a> {
    pub id: Ref,
    pub parent: Ref,
    pub name: Cow<'a, str>,
    pub class_name: Ustr,
    pub properties: UstrMap<Cow<'a, Variant>>,
    pub children: Cow<'a, [Ref]>,
    pub metadata: Option<InstanceMetadata>,
}

impl Instance<'_> {
    pub(crate) fn from_rojo_instance(source: InstanceWithMeta<'_>) -> Instance<'_> {
        let properties = source
            .properties()
            .iter()
            .filter(|(_key, value)| property_filter(Some(value)))
            .map(|(key, value)| (*key, Cow::Borrowed(value)))
            .collect();

        Instance {
            id: source.id(),
            parent: source.parent(),
            name: Cow::Borrowed(source.name()),
            class_name: source.class_name(),
            properties,
            children: Cow::Borrowed(source.children()),
            metadata: Some(InstanceMetadata::from_rojo_metadata(source.metadata())),
        }
    }
}

fn property_filter(value: Option<&Variant>) -> bool {
    let ty = value.map(|value| value.ty());

    // Lua can't do anything with SharedString values. They also can't be
    // serialized directly by Serde!
    ty != Some(VariantType::SharedString)
}

/// Response body from /api/rojo
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ServerInfoResponse {
    pub session_id: SessionId,
    pub server_version: String,
    pub protocol_version: u64,
    pub project_name: String,
    pub expected_place_ids: Option<HashSet<u64>>,
    pub unexpected_place_ids: Option<HashSet<u64>>,
    pub game_id: Option<u64>,
    pub place_id: Option<u64>,
    pub root_instance_id: Ref,
}

/// Response body from /api/read/{id}
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReadResponse<'a> {
    pub session_id: SessionId,
    pub message_cursor: u32,
    pub instances: HashMap<Ref, Instance<'a>>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WriteRequest {
    pub session_id: SessionId,
    pub removed: Vec<Ref>,

    #[serde(default)]
    pub added: HashMap<Ref, ()>,
    pub updated: Vec<InstanceUpdate>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WriteResponse {
    pub session_id: SessionId,
}

/// Packet type enum for different websocket message types
#[derive(Debug, Serialize, Deserialize, Display, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
#[strum(serialize_all = "camelCase")]
pub enum SocketPacketType {
    Messages,
    // TODO: Can we cleanly use the socket for all communication?
    // Serialize,
    // RefPatch,
}

/// Body content for messages packet type
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MessagesPacket<'a> {
    pub message_cursor: u32,
    pub messages: Vec<SubscribeMessage<'a>>,
}

/// Body content for different packet types
#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SocketPacketBody<'a> {
    Messages(MessagesPacket<'a>),
    // TODO: Can we cleanly use the socket for all communication?
    // Serialize(SerializePacket),
    // RefPatch(RefPatchPacket<'a>),
}

/// Message content from /api/socket
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SocketPacket<'a> {
    pub session_id: SessionId,
    pub packet_type: SocketPacketType,
    pub body: SocketPacketBody<'a>,
}

/// Response body from /api/open/{id}
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OpenResponse {
    pub session_id: SessionId,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SerializeRequest {
    pub session_id: SessionId,
    pub ids: Vec<Ref>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SerializeResponse {
    pub session_id: SessionId,
    #[serde(with = "serde_bytes")]
    pub model_contents: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RefPatchRequest {
    pub session_id: SessionId,
    pub ids: HashSet<Ref>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RefPatchResponse<'a> {
    pub session_id: SessionId,
    pub patch: SubscribeMessage<'a>,
}

/// General response type returned from all Rojo routes
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ErrorResponse {
    kind: ErrorResponseKind,
    details: String,
}

impl ErrorResponse {
    pub fn not_found<S: Into<String>>(details: S) -> Self {
        Self {
            kind: ErrorResponseKind::NotFound,
            details: details.into(),
        }
    }

    pub fn bad_request<S: Into<String>>(details: S) -> Self {
        Self {
            kind: ErrorResponseKind::BadRequest,
            details: details.into(),
        }
    }

    pub fn forbidden<S: Into<String>>(details: S) -> Self {
        Self {
            kind: ErrorResponseKind::Forbidden,
            details: details.into(),
        }
    }

    pub fn internal_error<S: Into<String>>(details: S) -> Self {
        Self {
            kind: ErrorResponseKind::InternalError,
            details: details.into(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum ErrorResponseKind {
    NotFound,
    BadRequest,
    Forbidden,
    InternalError,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot::{AppliedPatchUpdate, InstanceSnapshot};

    /// Builds a tree with a single named child, returning the tree and the
    /// child's id.
    fn tree_with_child() -> (RojoTree, Ref) {
        let mut tree = RojoTree::new(InstanceSnapshot::new().name("ROOT").class_name("ROOT"));
        let root_id = tree.get_root_id();
        let child_id = tree.insert_instance(
            root_id,
            InstanceSnapshot::new().name("Child").class_name("Folder"),
        );
        (tree, child_id)
    }

    #[test]
    fn skips_additions_for_instances_no_longer_in_the_tree() {
        let (tree, child_id) = tree_with_child();

        // An id that was queued as added and then removed from the tree before
        // the patch was turned into an API message, which is what happens when
        // a file is created and deleted faster than the socket drains.
        let stale_id = Ref::new();
        let removed_id = Ref::new();

        let patch = AppliedPatchSet {
            removed: vec![removed_id],
            added: vec![child_id, stale_id],
            updated: vec![AppliedPatchUpdate::new(child_id)],
        };

        let message = SubscribeMessage::from_patch_update(&tree, patch);

        // The id that is still in the tree is delivered as before.
        assert_eq!(message.added.len(), 1);
        assert_eq!(message.added[&child_id].name, "Child");

        // The stale id is dropped rather than panicking, and the removed and
        // updated sections are forwarded untouched.
        assert!(!message.added.contains_key(&stale_id));
        assert_eq!(message.removed, vec![removed_id]);
        assert_eq!(message.updated.len(), 1);
        assert_eq!(message.updated[0].id, child_id);
    }
}
