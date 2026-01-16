//! Snapshot functionality for AgentFS mounted filesystems.
//!
//! This module provides signal-based snapshot support for mounted filesystems.
//! When the mount process receives SIGUSR1, it will:
//! 1. Checkpoint the WAL (flush to main db file)
//! 2. Copy the database to {db_path}.snapshot.{timestamp}
//! 3. Create a {db_path}.snapshot.{timestamp}.done file with metadata

use serde::{Deserialize, Serialize};
use std::fs;
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing;
use turso::Connection;

/// Global flag set by the signal handler when SIGUSR1 is received
static SNAPSHOT_REQUESTED: AtomicBool = AtomicBool::new(false);

/// Signal handler for SIGUSR1 - just sets a flag
extern "C" fn sigusr1_handler(_: libc::c_int) {
    SNAPSHOT_REQUESTED.store(true, Ordering::SeqCst);
}

/// Metadata written to the .done file
#[derive(Serialize, Deserialize, Debug)]
pub struct SnapshotMetadata {
    pub timestamp: u64,
    pub source: String,
    pub snapshot: String,
    pub size: u64,
}

/// State needed for snapshot operations
pub struct SnapshotHandler {
    db_path: PathBuf,
    conn: Arc<Connection>,
    runtime: tokio::runtime::Runtime,
}

impl SnapshotHandler {
    /// Create a new snapshot handler
    pub fn new(db_path: PathBuf, conn: Arc<Connection>) -> Self {
        let runtime = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");
        Self {
            db_path,
            conn,
            runtime,
        }
    }

    /// Set up the SIGUSR1 signal handler using libc
    pub fn setup_signal_handler() -> anyhow::Result<()> {
        unsafe {
            // Set up the signal handler
            let result =
                libc::signal(libc::SIGUSR1, sigusr1_handler as *const () as libc::sighandler_t);
            if result == libc::SIG_ERR {
                anyhow::bail!("Failed to set up SIGUSR1 handler");
            }
        }
        tracing::info!("Snapshot signal handler installed (send SIGUSR1 to trigger snapshot)");
        Ok(())
    }

    /// Check if a snapshot was requested and perform it if so
    pub fn check_and_snapshot(&self) -> anyhow::Result<bool> {
        if !SNAPSHOT_REQUESTED.swap(false, Ordering::SeqCst) {
            return Ok(false);
        }

        tracing::info!("Snapshot requested via SIGUSR1");
        let _ = self.perform_snapshot()?;
        Ok(true)
    }

    /// Perform the snapshot operation
    ///
    /// This checkpoints the WAL, copies the database, and creates a .done file.
    /// Returns the paths to the snapshot and done files.
    pub fn perform_snapshot(&self) -> anyhow::Result<(PathBuf, PathBuf)> {
        // Include timestamp in filename so multiple snapshots can coexist
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let snapshot_path = self
            .db_path
            .with_extension(format!("db.snapshot.{}", timestamp));
        let done_path = self
            .db_path
            .with_extension(format!("db.snapshot.{}.done", timestamp));

        // 1. Checkpoint the WAL
        tracing::info!("Checkpointing WAL...");
        self.runtime.block_on(async {
            // wal_checkpoint returns rows (busy, log, checkpointed), so use query and consume them
            let mut rows = self
                .conn
                .query("PRAGMA wal_checkpoint(TRUNCATE)", ())
                .await?;
            while rows.next().await?.is_some() {}
            Ok::<_, anyhow::Error>(())
        })?;
        tracing::info!("WAL checkpoint complete");

        // 2. Copy the database file
        tracing::info!("Copying database to {:?}...", snapshot_path);
        fs::copy(&self.db_path, &snapshot_path)?;

        // Get file size
        let metadata = fs::metadata(&snapshot_path)?;
        let size = metadata.len();

        // 3. Create .done file with metadata
        let done_metadata = SnapshotMetadata {
            timestamp,
            source: self.db_path.to_string_lossy().to_string(),
            snapshot: snapshot_path.to_string_lossy().to_string(),
            size,
        };

        let mut done_file = fs::File::create(&done_path)?;
        serde_json::to_writer_pretty(&mut done_file, &done_metadata)?;
        done_file.flush()?;
        done_file.sync_all()?;

        tracing::info!(
            "Snapshot complete: {} ({} bytes)",
            snapshot_path.display(),
            size
        );
        eprintln!(
            "Snapshot complete: {} ({} bytes)",
            snapshot_path.display(),
            size
        );

        Ok((snapshot_path, done_path))
    }
}

/// Start the snapshot handler in a background thread.
///
/// Returns a handle that can be used to stop the handler.
pub fn start_snapshot_handler(
    db_path: PathBuf,
    conn: Arc<Connection>,
) -> anyhow::Result<Arc<SnapshotHandler>> {
    // Set up the signal handler
    SnapshotHandler::setup_signal_handler()?;

    let handler = Arc::new(SnapshotHandler::new(db_path, conn));
    let handler_clone = handler.clone();

    // Spawn a thread that periodically checks for snapshot requests
    thread::spawn(move || {
        loop {
            // Check every 100ms for a snapshot request
            thread::sleep(std::time::Duration::from_millis(100));

            if let Err(e) = handler_clone.check_and_snapshot() {
                tracing::error!("Snapshot failed: {}", e);
                eprintln!("Snapshot failed: {}", e);
            }
        }
    });

    Ok(handler)
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentfs_sdk::{AgentFS, AgentFSOptions};
    use std::time::Duration;
    use tempfile::TempDir;

    /// Helper to create a test database with some data
    async fn create_test_db(dir: &TempDir) -> (PathBuf, Arc<Connection>) {
        let db_path = dir.path().join("test.db");
        let db_path_str = db_path.to_string_lossy().to_string();

        let agentfs = AgentFS::open(AgentFSOptions::with_path(&db_path_str))
            .await
            .expect("Failed to create test database");

        // Write some test data
        agentfs
            .fs
            .write_file("/test.txt", b"hello world")
            .await
            .expect("Failed to write test file");

        let conn = agentfs.get_connection();
        (db_path, conn)
    }

    #[test]
    fn test_snapshot_creates_files() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let temp_dir = TempDir::new().expect("Failed to create temp dir");

        let (db_path, conn) = rt.block_on(create_test_db(&temp_dir));

        // Create snapshot handler and perform snapshot
        let handler = SnapshotHandler::new(db_path.clone(), conn);
        let (snapshot_path, done_path) = handler
            .perform_snapshot()
            .expect("Snapshot should succeed");

        // Verify snapshot file exists
        assert!(snapshot_path.exists(), "Snapshot file should exist");

        // Verify done file exists
        assert!(done_path.exists(), "Done file should exist");

        // Verify snapshot file has content (should be > 0 bytes)
        let snapshot_meta =
            fs::metadata(&snapshot_path).expect("Failed to get snapshot metadata");
        assert!(
            snapshot_meta.len() > 0,
            "Snapshot file should have content"
        );

        // Verify done file has valid JSON metadata
        let done_content = fs::read_to_string(&done_path).expect("Failed to read done file");
        let metadata: SnapshotMetadata =
            serde_json::from_str(&done_content).expect("Done file should be valid JSON");

        assert_eq!(metadata.source, db_path.to_string_lossy().to_string());
        assert_eq!(
            metadata.snapshot,
            snapshot_path.to_string_lossy().to_string()
        );
        assert!(metadata.size > 0);
        assert!(metadata.timestamp > 0);
    }

    #[test]
    fn test_multiple_snapshots_have_different_names() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let temp_dir = TempDir::new().expect("Failed to create temp dir");

        let (db_path, conn) = rt.block_on(create_test_db(&temp_dir));

        let handler = SnapshotHandler::new(db_path, conn);

        // Take first snapshot
        let (snapshot1, done1) = handler
            .perform_snapshot()
            .expect("First snapshot should succeed");

        // Wait a bit to ensure different timestamp
        std::thread::sleep(Duration::from_secs(1));

        // Take second snapshot
        let (snapshot2, done2) = handler
            .perform_snapshot()
            .expect("Second snapshot should succeed");

        // Verify both snapshots exist
        assert!(snapshot1.exists(), "First snapshot should exist");
        assert!(snapshot2.exists(), "Second snapshot should exist");
        assert!(done1.exists(), "First done file should exist");
        assert!(done2.exists(), "Second done file should exist");

        // Verify they have different names (different timestamps)
        assert_ne!(
            snapshot1, snapshot2,
            "Snapshots should have different names"
        );
        assert_ne!(done1, done2, "Done files should have different names");
    }

    #[test]
    fn test_snapshot_via_signal() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let temp_dir = TempDir::new().expect("Failed to create temp dir");

        let (db_path, conn) = rt.block_on(create_test_db(&temp_dir));

        // Set up signal handler
        SnapshotHandler::setup_signal_handler().expect("Failed to set up signal handler");

        let handler = SnapshotHandler::new(db_path.clone(), conn);

        // Verify no snapshot requested initially
        assert!(
            !SNAPSHOT_REQUESTED.load(Ordering::SeqCst),
            "No snapshot should be requested initially"
        );

        // Send SIGUSR1 to self
        unsafe {
            libc::raise(libc::SIGUSR1);
        }

        // Give signal a moment to be delivered
        std::thread::sleep(Duration::from_millis(10));

        // Verify snapshot was requested
        assert!(
            SNAPSHOT_REQUESTED.load(Ordering::SeqCst),
            "Snapshot should be requested after signal"
        );

        // Check and snapshot should perform the snapshot
        let result = handler
            .check_and_snapshot()
            .expect("check_and_snapshot should succeed");
        assert!(result, "check_and_snapshot should return true");

        // Verify flag is cleared
        assert!(
            !SNAPSHOT_REQUESTED.load(Ordering::SeqCst),
            "Flag should be cleared after snapshot"
        );

        // Verify snapshot files exist (find them by pattern)
        let entries: Vec<_> = fs::read_dir(temp_dir.path())
            .expect("Failed to read temp dir")
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().contains(".db.snapshot."))
            .collect();

        assert!(
            entries.len() >= 2,
            "Should have snapshot and done files, found: {:?}",
            entries
        );
    }

    #[test]
    fn test_snapshot_metadata_fields() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let temp_dir = TempDir::new().expect("Failed to create temp dir");

        let (db_path, conn) = rt.block_on(create_test_db(&temp_dir));

        let handler = SnapshotHandler::new(db_path.clone(), conn);
        let (snapshot_path, done_path) = handler
            .perform_snapshot()
            .expect("Snapshot should succeed");

        // Read and parse done file
        let done_content = fs::read_to_string(&done_path).expect("Failed to read done file");
        let metadata: SnapshotMetadata =
            serde_json::from_str(&done_content).expect("Done file should be valid JSON");

        // Verify all fields are populated correctly
        assert_eq!(
            metadata.source,
            db_path.to_string_lossy().to_string(),
            "Source should match db_path"
        );
        assert_eq!(
            metadata.snapshot,
            snapshot_path.to_string_lossy().to_string(),
            "Snapshot path should match"
        );

        // Size should match actual file size
        let actual_size = fs::metadata(&snapshot_path)
            .expect("Failed to get snapshot metadata")
            .len();
        assert_eq!(metadata.size, actual_size, "Size should match actual file");

        // Timestamp should be recent (within last minute)
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        assert!(
            metadata.timestamp <= now && metadata.timestamp > now - 60,
            "Timestamp should be recent"
        );
    }
}
