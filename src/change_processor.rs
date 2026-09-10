use crossbeam_channel::{select, Receiver, RecvError, Sender};
use jod_thread::JoinHandle;
use memofs::{IoResultExt, Vfs, VfsEvent};
use rbx_dom_weak::types::{Ref, Variant};
use std::path::PathBuf;
use std::{
    fs,
    sync::{Arc, Mutex},
};

use crate::{
    message_queue::MessageQueue,
    snapshot::{
        apply_patch_set, compute_patch_set, AppliedPatchSet, InstigatingSource, PatchSet, RojoTree,
    },
    snapshot_middleware::{snapshot_from_vfs, snapshot_project_node},
};

/// Processes file change events, updates the DOM, and sends those updates
/// through a channel for other stuff to consume.
///
/// Owns the connection between Rojo's VFS and its DOM by holding onto another
/// thread that processes messages.
///
/// Consumers of ChangeProcessor, like ServeSession, are intended to communicate
/// with this object via channels.
///
/// ChangeProcessor expects to be the only writer to the RojoTree and Vfs
/// objects passed to it.
pub struct ChangeProcessor {
    /// Controls the runtime of the processor thread. When signaled, the job
    /// thread will finish its current work and terminate.
    ///
    /// This channel should be signaled before dropping ChangeProcessor or we'll
    /// hang forever waiting for the message processing loop to terminate.
    shutdown_sender: Sender<()>,

    /// A handle to the message processing thread. When dropped, we'll block
    /// until it's done.
    ///
    /// Allowed to be unused because dropping this value has side effects.
    #[allow(unused)]
    job_thread: JoinHandle<Result<(), RecvError>>,
}

impl ChangeProcessor {
    /// Spin up the ChangeProcessor, connecting it to the given tree, VFS, and
    /// outbound message queue.
    pub fn start(
        tree: Arc<Mutex<RojoTree>>,
        vfs: Arc<Vfs>,
        message_queue: Arc<MessageQueue<AppliedPatchSet>>,
        tree_mutation_receiver: Receiver<PatchSet>,
    ) -> Self {
        let (shutdown_sender, shutdown_receiver) = crossbeam_channel::bounded(1);
        let vfs_receiver = vfs.event_receiver();
        let task = JobThreadContext {
            tree,
            vfs,
            message_queue,
        };

        let job_thread = jod_thread::Builder::new()
            .name("ChangeProcessor thread".to_owned())
            .spawn(move || {
                log::trace!("ChangeProcessor thread started");

                loop {
                    select! {
                        recv(vfs_receiver) -> event => {
                            task.handle_vfs_event(event?);
                        },
                        recv(tree_mutation_receiver) -> patch_set => {
                            task.handle_tree_event(patch_set?);
                        },
                        recv(shutdown_receiver) -> _ => {
                            log::trace!("ChangeProcessor shutdown signal received...");
                            return Ok(());
                        },
                    }
                }
            })
            .expect("Could not start ChangeProcessor thread");

        Self {
            shutdown_sender,
            job_thread,
        }
    }
}

impl Drop for ChangeProcessor {
    fn drop(&mut self) {
        // Signal the job thread to start spinning down. Without this we'll hang
        // forever waiting for the thread to finish its infinite loop.
        let _ = self.shutdown_sender.send(());

        // After this function ends, the job thread will be joined. It might
        // block for a small period of time while it processes its last work.
    }
}

/// Contains all of the state needed to synchronize the DOM and VFS.
struct JobThreadContext {
    /// A handle to the DOM we're managing.
    tree: Arc<Mutex<RojoTree>>,

    /// A handle to the VFS we're managing.
    vfs: Arc<Vfs>,

    /// Whenever changes are applied to the DOM, we should push those changes
    /// into this message queue to inform any connected clients.
    message_queue: Arc<MessageQueue<AppliedPatchSet>>,
}

impl JobThreadContext {
    /// Computes and applies patches to the DOM for a given file path.
    ///
    /// This function finds the nearest ancestor to the given path that has associated instances
    /// in the tree.
    /// It then computes and applies changes for each affected instance ID and
    /// returns a vector of applied patch sets.
    fn apply_patches(&self, path: PathBuf) -> Vec<AppliedPatchSet> {
        let mut tree = self.tree.lock().unwrap();
        let mut applied_patches = Vec::new();

        // Find the nearest ancestor to this path that has
        // associated instances in the tree. This helps make sure
        // that we handle additions correctly, especially if we
        // receive events for descendants of a large tree being
        // created all at once.
        let mut current_path = path.as_path();
        let affected_ids = loop {
            let ids = tree.get_ids_at_path(current_path);

            log::trace!("Path {} affects IDs {:?}", current_path.display(), ids);

            if !ids.is_empty() {
                break ids.to_vec();
            }

            log::trace!("Trying parent path...");
            match current_path.parent() {
                Some(parent) => current_path = parent,
                None => break Vec::new(),
            }
        };

        for id in affected_ids {
            if tree.get_metadata(id).is_none() {
                log::trace!(
                    "Instance {:?} was affected but no longer present in tree; skipping",
                    id
                );
                continue;
            }
            if let Some(patch) = compute_and_apply_changes(&mut tree, &self.vfs, id) {
                if !patch.is_empty() {
                    applied_patches.push(patch);
                }
            }
        }

        applied_patches
    }

    fn handle_vfs_event(&self, event: VfsEvent) {
        log::trace!("Vfs event: {:?}", event);

        // Update the VFS immediately with the event.
        if let Err(err) = self.vfs.commit_event(&event) {
            log::error!("Error applying VFS change: {:?}", err);
            return;
        }

        // For a given VFS event, we might have many changes to different parts
        // of the tree. Calculate and apply all of these changes.
        let applied_patches = match event {
            VfsEvent::Create(path) | VfsEvent::Write(path) | VfsEvent::Remove(path) => {
                match canonicalize_event_path(&self.vfs, &path) {
                    Ok(path) => self.apply_patches(path),
                    Err(err) => {
                        log::warn!(
                            "Could not resolve filesystem event {}: {}",
                            path.display(),
                            err
                        );
                        Vec::new()
                    }
                }
            }
            _ => {
                log::warn!("Unhandled VFS event: {:?}", event);
                Vec::new()
            }
        };

        // Notify anyone listening to the message queue about the changes we
        // just made.
        self.message_queue.push_messages(&applied_patches);
    }

    fn handle_tree_event(&self, patch_set: PatchSet) {
        log::trace!("Applying PatchSet from client: {:#?}", patch_set);

        let applied_patch = {
            let mut tree = self.tree.lock().unwrap();

            for &id in &patch_set.removed_instances {
                if let Some(instance) = tree.get_instance(id) {
                    if let Some(instigating_source) = &instance.metadata().instigating_source {
                        match instigating_source {
                            InstigatingSource::Path(path) => {
                                if let Err(err) = fs::remove_file(path) {
                                    log::error!(
                                        "Failed to remove file {}: {}",
                                        path.display(),
                                        err
                                    );
                                }
                            }
                            InstigatingSource::ProjectNode { .. } => {
                                log::warn!(
                                    "Cannot remove instance {:?}, it's from a project file",
                                    id
                                );
                            }
                        }
                    } else {
                        // TODO
                        log::warn!(
                            "Cannot remove instance {:?}, it is not an instigating source.",
                            id
                        );
                    }
                } else {
                    log::warn!("Cannot remove instance {:?}, it does not exist.", id);
                }
            }

            for update in &patch_set.updated_instances {
                let id = update.id;

                if let Some(instance) = tree.get_instance(id) {
                    if update.changed_name.is_some() {
                        log::warn!("Cannot rename instances yet.");
                    }

                    if update.changed_class_name.is_some() {
                        log::warn!("Cannot change ClassName yet.");
                    }

                    if update.changed_metadata.is_some() {
                        log::warn!("Cannot change metadata yet.");
                    }

                    for (key, changed_value) in &update.changed_properties {
                        if key == "Source" {
                            if let Some(instigating_source) =
                                &instance.metadata().instigating_source
                            {
                                match instigating_source {
                                    InstigatingSource::Path(path) => {
                                        if let Some(Variant::String(value)) = changed_value {
                                            if let Err(err) = fs::write(path, value) {
                                                log::error!(
                                                    "Failed to write file {}: {}",
                                                    path.display(),
                                                    err
                                                );
                                            }
                                        } else {
                                            log::warn!("Cannot change Source to non-string value.");
                                        }
                                    }
                                    InstigatingSource::ProjectNode { .. } => {
                                        log::warn!(
                                            "Cannot remove instance {:?}, it's from a project file",
                                            id
                                        );
                                    }
                                }
                            } else {
                                log::warn!(
                                    "Cannot update instance {:?}, it is not an instigating source.",
                                    id
                                );
                            }
                        } else {
                            log::warn!("Cannot change properties besides BaseScript.Source.");
                        }
                    }
                } else {
                    log::warn!("Cannot update instance {:?}, it does not exist.", id);
                }
            }

            apply_patch_set(&mut tree, patch_set)
        };

        if !applied_patch.is_empty() {
            self.message_queue.push_messages(&[applied_patch]);
        }
    }
}

// Debounced events can outlive both their file and its containing directories.
// Normalize the surviving ancestor and preserve the missing suffix so removals
// still reconcile the original tree IDs instead of being dropped.
fn canonicalize_event_path(vfs: &Vfs, path: &std::path::Path) -> std::io::Result<PathBuf> {
    let mut ancestor = path;
    let mut suffix = PathBuf::new();
    loop {
        match vfs.canonicalize(ancestor) {
            Ok(mut normalized) => {
                if !suffix.as_os_str().is_empty() {
                    normalized.push(suffix);
                }
                return Ok(normalized);
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                match (ancestor.parent(), ancestor.file_name()) {
                    (Some(parent), Some(name)) => {
                        suffix = PathBuf::from(name).join(suffix);
                        ancestor = parent;
                    }
                    _ => return Err(err),
                }
            }
            Err(err) => return Err(err),
        }
    }
}

fn compute_and_apply_changes(tree: &mut RojoTree, vfs: &Vfs, id: Ref) -> Option<AppliedPatchSet> {
    let metadata = match tree.get_metadata(id) {
        Some(metadata) => metadata,
        None => {
            log::trace!(
                "Instance {:?} was affected by an event but is no longer present in the tree; skipping",
                id
            );
            return None;
        }
    };

    let instigating_source = match &metadata.instigating_source {
        Some(path) => path,
        None => {
            log::error!(
                "Instance {:?} did not have an instigating source, but was considered for an update.",
                id
            );
            log::error!("This is a bug. Please file an issue!");
            return None;
        }
    };

    // How we process a file change event depends on what created this
    // file/folder in the first place.
    let applied_patch_set = match instigating_source {
        InstigatingSource::Path(path) => match vfs.metadata(path).with_not_found() {
            Ok(Some(_)) => {
                // Our instance was previously created from a path and that
                // path still exists. We can generate a snapshot starting at
                // that path and use it as the source for our patch.

                let snapshot = match snapshot_from_vfs(&metadata.context, vfs, path) {
                    Ok(snapshot) => snapshot,
                    Err(err) => {
                        log::error!("Snapshot error: {:?}", err);
                        return None;
                    }
                };
                if id == tree.get_root_id() && snapshot.is_none() {
                    log::warn!(
                        "Snapshot for root project file {} was None; retaining last good tree",
                        path.display()
                    );
                    return None;
                }

                let patch_set = compute_patch_set(snapshot, tree, id);
                apply_patch_set(tree, patch_set)
            }
            Ok(None) => {
                // Our instance was previously created from a path, but that
                // path no longer exists.
                //
                // We associate deleting the instigating file for an
                // instance with deleting that instance, unless it is the root
                // project, in which case we retain the last good tree.
                if id == tree.get_root_id() {
                    log::warn!(
                        "Root project file {} is missing; retaining last good tree",
                        path.display()
                    );
                    return None;
                }

                let mut patch_set = PatchSet::new();
                patch_set.removed_instances.push(id);

                apply_patch_set(tree, patch_set)
            }
            Err(err) => {
                log::error!("Error processing filesystem change: {:?}", err);
                return None;
            }
        },

        InstigatingSource::ProjectNode {
            path,
            name,
            node,
            parent_class,
        } => {
            // This instance is the direct subject of a project node. Since
            // there might be information associated with our instance from
            // the project file, we snapshot the entire project node again.

            let snapshot_result = snapshot_project_node(
                &metadata.context,
                path,
                name,
                node,
                vfs,
                parent_class.as_ref().map(|name| name.as_str()),
            );

            let snapshot = match snapshot_result {
                Ok(snapshot) => snapshot,
                Err(err) => {
                    log::error!("{:?}", err);
                    return None;
                }
            };
            if id == tree.get_root_id() && snapshot.is_none() {
                log::warn!(
                    "Snapshot for root project node {} was None; retaining last good tree",
                    path.display()
                );
                return None;
            }

            let patch_set = compute_patch_set(snapshot, tree, id);
            apply_patch_set(tree, patch_set)
        }
    };

    Some(applied_patch_set)
}

#[cfg(test)]
mod event_path_tests {
    use super::*;
    use memofs::StdBackend;

    #[test]
    fn deleted_parent_and_grandparent_keep_event_identity() {
        let root = tempfile::tempdir().unwrap();
        let parent = root.path().join("Assets/Nested");
        fs::create_dir_all(&parent).unwrap();
        let path = parent.join("Child.luau");
        fs::write(&path, "return 1").unwrap();
        let expected = fs::canonicalize(&path).unwrap();
        let vfs = Vfs::new(StdBackend::new().unwrap());
        fs::remove_dir_all(root.path().join("Assets")).unwrap();
        assert_eq!(canonicalize_event_path(&vfs, &path).unwrap(), expected);
    }

    #[test]
    fn existing_event_path_is_canonicalized() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("Child.luau");
        fs::write(&path, "return 1").unwrap();
        let vfs = Vfs::new(StdBackend::new().unwrap());
        assert_eq!(
            canonicalize_event_path(&vfs, &path).unwrap(),
            fs::canonicalize(path).unwrap()
        );
    }

    #[cfg(unix)]
    #[test]
    fn deleted_descendants_under_symlink_keep_canonical_identity() {
        let root = tempfile::tempdir().unwrap();
        fs::create_dir(root.path().join("actual")).unwrap();
        std::os::unix::fs::symlink(root.path().join("actual"), root.path().join("alias")).unwrap();
        let parent = root.path().join("alias/Assets");
        fs::create_dir(&parent).unwrap();
        let path = parent.join("Child.luau");
        fs::write(&path, "return 1").unwrap();
        let expected = fs::canonicalize(&path).unwrap();
        fs::remove_dir_all(parent).unwrap();
        let vfs = Vfs::new(StdBackend::new().unwrap());
        assert_eq!(canonicalize_event_path(&vfs, &path).unwrap(), expected);
    }
}

#[cfg(test)]
mod resilience_tests {
    use super::*;
    use crate::snapshot::{InstanceContext, InstanceSnapshot};
    use memofs::StdBackend;

    fn test_context(
        root_dir: &std::path::Path,
        project_name: &str,
        project_json: &str,
    ) -> (JobThreadContext, PathBuf, Ref) {
        let project_path = root_dir.join(format!("{}.project.json", project_name));
        fs::write(&project_path, project_json).unwrap();

        let vfs = Vfs::new(StdBackend::new().unwrap());
        let canonical_project_path = vfs.canonicalize(&project_path).unwrap();

        let mut tree = RojoTree::new(InstanceSnapshot::new());
        let root_id = tree.get_root_id();
        let context = InstanceContext::new();
        let snapshot = snapshot_from_vfs(&context, &vfs, &canonical_project_path)
            .unwrap()
            .expect("initial snapshot should succeed");
        let patch_set = compute_patch_set(Some(snapshot), &tree, root_id);
        apply_patch_set(&mut tree, patch_set);

        let tree_arc = Arc::new(Mutex::new(tree));
        let vfs_arc = Arc::new(vfs);
        let message_queue = Arc::new(MessageQueue::new());

        (
            JobThreadContext {
                tree: tree_arc,
                vfs: vfs_arc,
                message_queue,
            },
            canonical_project_path,
            root_id,
        )
    }

    #[test]
    fn root_delete_and_recreate_retains_last_good_and_updates() {
        let temp = tempfile::tempdir().unwrap();
        let initial_json = r#"{
            "name": "TestProject",
            "tree": {
                "$className": "DataModel"
            }
        }"#;
        let (ctx, project_path, root_id) = test_context(temp.path(), "default", initial_json);

        let initial_cursor = ctx.message_queue.cursor();

        // 1. Delete root project file
        fs::remove_file(&project_path).unwrap();
        ctx.handle_vfs_event(VfsEvent::Remove(project_path.clone()));

        // Root MUST NOT be deleted; last good tree is retained
        {
            let tree = ctx.tree.lock().unwrap();
            assert_eq!(tree.get_root_id(), root_id);
            let root_inst = tree.get_instance(root_id).expect("root must be retained");
            assert_eq!(root_inst.name(), "TestProject");
        }
        // No patch removing root should be emitted
        assert_eq!(ctx.message_queue.cursor(), initial_cursor);

        // 2. Recreate root project file with modified content
        let updated_json = r#"{
            "name": "UpdatedProject",
            "tree": {
                "$className": "DataModel"
            }
        }"#;
        fs::write(&project_path, updated_json).unwrap();
        ctx.handle_vfs_event(VfsEvent::Create(project_path.clone()));

        // Root instance ID remains stable, and tree is updated
        {
            let tree = ctx.tree.lock().unwrap();
            assert_eq!(tree.get_root_id(), root_id);
            let root_inst = tree.get_instance(root_id).expect("root must exist");
            assert_eq!(root_inst.name(), "UpdatedProject");
        }
        // Notification pushed to message queue
        assert!(ctx.message_queue.cursor() > initial_cursor);
    }

    #[test]
    fn malformed_and_restored_project_retains_last_good() {
        let temp = tempfile::tempdir().unwrap();
        let initial_json = r#"{
            "name": "ValidProject",
            "tree": {
                "$className": "DataModel"
            }
        }"#;
        let (ctx, project_path, root_id) = test_context(temp.path(), "default", initial_json);

        let initial_cursor = ctx.message_queue.cursor();

        // 1. Overwrite with malformed JSON
        fs::write(&project_path, "{ broken json ...").unwrap();
        ctx.handle_vfs_event(VfsEvent::Write(project_path.clone()));

        // Last good tree is retained
        {
            let tree = ctx.tree.lock().unwrap();
            assert_eq!(tree.get_root_id(), root_id);
            let root_inst = tree.get_instance(root_id).expect("root must be retained");
            assert_eq!(root_inst.name(), "ValidProject");
        }
        assert_eq!(ctx.message_queue.cursor(), initial_cursor);

        // 2. Restore with valid JSON
        let restored_json = r#"{
            "name": "RestoredProject",
            "tree": {
                "$className": "DataModel"
            }
        }"#;
        fs::write(&project_path, restored_json).unwrap();
        ctx.handle_vfs_event(VfsEvent::Write(project_path.clone()));

        // Tree is updated and root ID is stable
        {
            let tree = ctx.tree.lock().unwrap();
            assert_eq!(tree.get_root_id(), root_id);
            let root_inst = tree.get_instance(root_id).expect("root must exist");
            assert_eq!(root_inst.name(), "RestoredProject");
        }
        assert!(ctx.message_queue.cursor() > initial_cursor);
    }

    #[test]
    fn repeated_atomic_saves_followed_by_source_edit() {
        let temp = tempfile::tempdir().unwrap();
        let src_dir = temp.path().join("src");
        fs::create_dir_all(&src_dir).unwrap();
        let script_path = src_dir.join("Hello.server.luau");
        fs::write(&script_path, "print('initial')").unwrap();

        let initial_json = r#"{
            "name": "AtomicTest",
            "tree": {
                "$className": "DataModel",
                "Hello": {
                    "$path": "src/Hello.server.luau"
                }
            }
        }"#;
        let (ctx, project_path, root_id) = test_context(temp.path(), "default", initial_json);

        let canonical_script = ctx.vfs.canonicalize(&script_path).unwrap();

        // Perform multiple atomic saves of the project file (write to tmp, then replace)
        for i in 1..=3 {
            let tmp_path = temp.path().join(format!("default.project.json.tmp{}", i));
            let content = r#"{
                "name": "AtomicTest",
                "tree": {
                    "$className": "DataModel",
                    "Hello": {
                        "$path": "src/Hello.server.luau"
                    }
                }
            }"#;
            fs::write(&tmp_path, content).unwrap();
            fs::rename(&tmp_path, &project_path).unwrap();
            ctx.handle_vfs_event(VfsEvent::Write(project_path.clone()));
        }

        // Verify root is still alive and stable
        {
            let tree = ctx.tree.lock().unwrap();
            assert_eq!(tree.get_root_id(), root_id);
        }

        // Now edit the source file
        fs::write(&script_path, "print('updated')").unwrap();
        ctx.handle_vfs_event(VfsEvent::Write(canonical_script.clone()));

        // Changes to child should be reflected
        {
            let tree = ctx.tree.lock().unwrap();
            assert_eq!(tree.get_root_id(), root_id);
            let mut found_child = false;
            for inst in tree.descendants(root_id) {
                if inst.name() == "Hello" {
                    found_child = true;
                    if let Some((_, Variant::String(source))) = inst
                        .properties()
                        .iter()
                        .find(|(k, _)| k.as_str() == "Source")
                    {
                        assert_eq!(source.as_str(), "print('updated')");
                    }
                }
            }
            assert!(
                found_child,
                "child instance must still be present and updated"
            );
        }
    }

    #[test]
    fn normal_child_deletion_still_removes_instance() {
        let temp = tempfile::tempdir().unwrap();
        let src_dir = temp.path().join("src");
        fs::create_dir_all(&src_dir).unwrap();
        let child_path = src_dir.join("Child.server.luau");
        fs::write(&child_path, "return 123").unwrap();

        let initial_json = r#"{
            "name": "ChildDelTest",
            "tree": {
                "$className": "DataModel",
                "Workspace": {
                    "$className": "Workspace",
                    "$path": "src"
                }
            }
        }"#;
        let (ctx, _project_path, root_id) = test_context(temp.path(), "default", initial_json);

        let canonical_child = ctx.vfs.canonicalize(&child_path).unwrap();

        // Locate the child's Ref
        let child_id = {
            let tree = ctx.tree.lock().unwrap();
            let mut found = None;
            for inst in tree.descendants(root_id) {
                if inst.name() == "Child" {
                    found = Some(inst.id());
                    break;
                }
            }
            found.expect("Child instance must be present in tree")
        };

        // Delete the child file
        fs::remove_file(&child_path).unwrap();
        ctx.handle_vfs_event(VfsEvent::Remove(canonical_child));

        // Verify child IS removed from tree, while root is unaffected
        {
            let tree = ctx.tree.lock().unwrap();
            assert_eq!(tree.get_root_id(), root_id);
            assert!(tree.get_instance(root_id).is_some());
            assert!(
                tree.get_instance(child_id).is_none(),
                "child must be deleted from tree"
            );
        }
    }

    #[test]
    fn stale_affected_id_skipped_without_panic() {
        let temp = tempfile::tempdir().unwrap();
        let initial_json = r#"{
            "name": "StaleIdTest",
            "tree": {
                "$className": "DataModel"
            }
        }"#;
        let (ctx, _project_path, _root_id) = test_context(temp.path(), "default", initial_json);

        // compute_and_apply_changes with a nonexistent / stale Ref should return None and not panic
        let stale_id = Ref::new();
        let mut tree = ctx.tree.lock().unwrap();
        let result = compute_and_apply_changes(&mut tree, &ctx.vfs, stale_id);
        assert!(result.is_none());
    }
}
