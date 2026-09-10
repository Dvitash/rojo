use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use crossbeam_channel::Receiver;
use notify::{watcher, DebouncedEvent, RecommendedWatcher, RecursiveMode, Watcher};

use crate::{DirEntry, Metadata, ReadDir, VfsBackend, VfsEvent};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WatchKind {
    /// Recursively watching a directory and all descendants.
    Recursive,
    /// Non-recursively watching a parent directory on behalf of a file.
    NonRecursive,
}

/// `VfsBackend` that uses `std::fs` and the `notify` crate.
pub struct StdBackend {
    watcher: RecommendedWatcher,
    watcher_receiver: Receiver<VfsEvent>,
    watches: HashMap<PathBuf, WatchKind>,
}

impl StdBackend {
    pub fn new() -> io::Result<StdBackend> {
        let (notify_tx, notify_rx) = mpsc::channel();
        let watcher = watcher(notify_tx, Duration::from_millis(50)).map_err(io::Error::other)?;

        let (tx, rx) = crossbeam_channel::unbounded();

        thread::spawn(move || {
            for event in notify_rx {
                match event {
                    DebouncedEvent::Create(path) => {
                        tx.send(VfsEvent::Create(path))?;
                    }
                    DebouncedEvent::Write(path) => {
                        tx.send(VfsEvent::Write(path))?;
                    }
                    DebouncedEvent::Remove(path) => {
                        tx.send(VfsEvent::Remove(path))?;
                    }
                    DebouncedEvent::Rename(from, to) => {
                        tx.send(VfsEvent::Remove(from))?;
                        tx.send(VfsEvent::Create(to))?;
                    }
                    _ => {}
                }
            }

            Result::<(), crossbeam_channel::SendError<VfsEvent>>::Ok(())
        });

        Ok(Self {
            watcher,
            watcher_receiver: rx,
            watches: HashMap::new(),
        })
    }
}

impl VfsBackend for StdBackend {
    fn read(&mut self, path: &Path) -> io::Result<Vec<u8>> {
        fs_err::read(path)
    }

    fn write(&mut self, path: &Path, data: &[u8]) -> io::Result<()> {
        fs_err::write(path, data)
    }

    fn exists(&mut self, path: &Path) -> io::Result<bool> {
        std::fs::exists(path)
    }

    fn read_dir(&mut self, path: &Path) -> io::Result<ReadDir> {
        let entries: Result<Vec<_>, _> = fs_err::read_dir(path)?.collect();
        let mut entries = entries?;

        entries.sort_by_cached_key(|entry| entry.file_name());

        let inner = entries
            .into_iter()
            .map(|entry| Ok(DirEntry { path: entry.path() }));

        Ok(ReadDir {
            inner: Box::new(inner),
        })
    }

    fn create_dir(&mut self, path: &Path) -> io::Result<()> {
        fs_err::create_dir(path)
    }

    fn create_dir_all(&mut self, path: &Path) -> io::Result<()> {
        fs_err::create_dir_all(path)
    }

    fn remove_file(&mut self, path: &Path) -> io::Result<()> {
        fs_err::remove_file(path)
    }

    fn remove_dir_all(&mut self, path: &Path) -> io::Result<()> {
        fs_err::remove_dir_all(path)
    }

    fn metadata(&mut self, path: &Path) -> io::Result<Metadata> {
        let inner = fs_err::metadata(path)?;

        Ok(Metadata {
            is_file: inner.is_file(),
        })
    }

    fn canonicalize(&mut self, path: &Path) -> io::Result<PathBuf> {
        fs_err::canonicalize(path)
    }

    fn event_receiver(&self) -> crossbeam_channel::Receiver<VfsEvent> {
        self.watcher_receiver.clone()
    }

    fn watch(&mut self, path: &Path) -> io::Result<()> {
        let is_dir = match fs_err::metadata(path) {
            Ok(meta) => meta.is_dir(),
            Err(_) => path.is_dir(),
        };

        if is_dir {
            // Directories maintain recursive watch coverage.
            if self.watches.get(path) == Some(&WatchKind::Recursive) {
                return Ok(());
            }
            if path
                .ancestors()
                .skip(1)
                .any(|ancestor| self.watches.get(ancestor) == Some(&WatchKind::Recursive))
            {
                return Ok(());
            }

            self.watcher
                .watch(path, RecursiveMode::Recursive)
                .map_err(io::Error::other)?;
            self.watches
                .insert(path.to_path_buf(), WatchKind::Recursive);

            // Clean up any redundant sub-watches covered by this recursive watch.
            self.watches
                .retain(|watched_path, _| watched_path == path || !watched_path.starts_with(path));

            Ok(())
        } else {
            // For files, watch the parent directory non-recursively so that file
            // replacements (such as atomic saves, delete-and-recreate, or renames)
            // are observed, without broad ancestor recursive watches.
            let parent = match path.parent() {
                Some(parent) if !parent.as_os_str().is_empty() => parent,
                _ => Path::new("."),
            };

            // If the parent directory or any ancestor is already watched recursively,
            // the file's directory is already covered.
            if parent
                .ancestors()
                .any(|ancestor| self.watches.get(ancestor) == Some(&WatchKind::Recursive))
            {
                return Ok(());
            }

            // If the parent directory is already watched non-recursively, it will already
            // observe replacements of files in that directory.
            if self.watches.get(parent) == Some(&WatchKind::NonRecursive) {
                return Ok(());
            }

            self.watcher
                .watch(parent, RecursiveMode::NonRecursive)
                .map_err(io::Error::other)?;
            self.watches
                .insert(parent.to_path_buf(), WatchKind::NonRecursive);

            Ok(())
        }
    }

    fn unwatch(&mut self, path: &Path) -> io::Result<()> {
        let was_watched = self.watches.remove(path).is_some();

        // Clean descendant cache on unwatch: any watches recorded under `path`
        // are removed from our watch tracking.
        let mut descendants = Vec::new();
        self.watches.retain(|watched_path, _| {
            if watched_path.starts_with(path) {
                descendants.push(watched_path.clone());
                false
            } else {
                true
            }
        });

        for descendant in descendants {
            let _ = self.watcher.unwatch(&descendant);
        }

        if was_watched {
            let _ = self.watcher.unwatch(path);
        }

        Ok(())
    }
}

#[cfg(test)]
impl StdBackend {
    pub(crate) fn is_watched(&self, path: &Path) -> bool {
        self.watches.contains_key(path)
    }

    pub(crate) fn watch_count(&self) -> usize {
        self.watches.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn watcher_failure_retry_records_only_on_success() {
        let mut backend = StdBackend::new().unwrap();
        let invalid_path = Path::new("/nonexistent_dir_12345/nonexistent_child");

        // Watch attempt on nonexistent directory should fail
        assert!(backend.watch(invalid_path).is_err());
        // And must NOT record it in watches
        assert!(!backend.is_watched(invalid_path));
        assert_eq!(backend.watch_count(), 0);

        // Valid directory watch succeeds and is recorded
        let temp_dir = tempfile::tempdir().unwrap();
        assert!(backend.watch(temp_dir.path()).is_ok());
        assert!(backend.is_watched(temp_dir.path()));
        assert_eq!(backend.watch_count(), 1);
    }

    #[test]
    fn file_watch_installs_parent_dir_and_observes_replacement() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("default.project.json");
        std::fs::write(&file_path, "{}").unwrap();

        let mut backend = StdBackend::new().unwrap();
        backend.watch(&file_path).unwrap();

        // Parent directory is watched, not the direct file
        assert!(backend.is_watched(temp_dir.path()));
        assert!(!backend.is_watched(&file_path));

        // Simulate atomic replacement: write to temp file then rename
        let tmp_file = temp_dir.path().join("default.project.json.tmp");
        std::fs::write(&tmp_file, "{\"name\":\"test\"}").unwrap();
        std::fs::rename(&tmp_file, &file_path).unwrap();

        // Receive events from the watcher
        let rx = backend.event_receiver();
        let mut got_event = false;
        let start = std::time::Instant::now();
        while start.elapsed() < Duration::from_secs(2) {
            if let Ok(event) = rx.recv_timeout(Duration::from_millis(200)) {
                match event {
                    VfsEvent::Create(p) | VfsEvent::Write(p) => {
                        if p.file_name() == file_path.file_name() {
                            got_event = true;
                            break;
                        }
                    }
                    _ => {}
                }
            }
        }
        assert!(
            got_event,
            "Expected filesystem event for atomically replaced file"
        );
    }

    #[test]
    fn unwatch_cleans_descendant_cache() {
        let temp_dir = tempfile::tempdir().unwrap();
        let sub_dir = temp_dir.path().join("sub");
        std::fs::create_dir_all(&sub_dir).unwrap();

        let mut backend = StdBackend::new().unwrap();
        backend.watch(&sub_dir).unwrap();
        assert!(backend.is_watched(&sub_dir));

        // Unwatch the parent temp_dir
        backend.unwatch(temp_dir.path()).unwrap();
        // Descendant cache should be cleaned
        assert!(!backend.is_watched(&sub_dir));
        assert_eq!(backend.watch_count(), 0);
    }

    #[test]
    fn recursive_directory_watch_avoids_redundant_descendant_watches() {
        let temp_dir = tempfile::tempdir().unwrap();
        let child_dir = temp_dir.path().join("child");
        std::fs::create_dir_all(&child_dir).unwrap();
        let file_path = child_dir.join("test.txt");
        std::fs::write(&file_path, "hello").unwrap();

        let mut backend = StdBackend::new().unwrap();
        // Watching root recursively
        backend.watch(temp_dir.path()).unwrap();
        assert_eq!(backend.watch_count(), 1);

        // Watching child dir does not add redundant watch
        backend.watch(&child_dir).unwrap();
        assert_eq!(backend.watch_count(), 1);

        // Watching file in descendant does not add redundant watch
        backend.watch(&file_path).unwrap();
        assert_eq!(backend.watch_count(), 1);
    }
}
