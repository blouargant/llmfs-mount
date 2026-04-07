use std::io::IsTerminal;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use clap::Parser;
use tracing::info;
use xet_data::processing::configurations::TranslatorConfig;
use xet_data::processing::data_client::default_config;
use xet_data::processing::{CacheConfig, FileDownloadSession, create_remote_client, get_cache};

use crate::cached_xet_client::CachedXetClient;
use crate::hub_api::{HubApiClient, HubOps, HubTokenRefresher, SourceKind, parse_repo_id, split_path_prefix};
use crate::providers::gitea::{GiteaClient, NoOpXet};
use crate::providers::github::GitHubClient;
use crate::providers::gitlab::GitLabClient;
use crate::virtual_fs::{VfsConfig, VirtualFs};
use crate::xet::{StagingDir, XetOps, XetSessions};

#[derive(clap::Subcommand)]
pub enum Source {
    /// Mount a HuggingFace bucket (read-write by default)
    Bucket {
        /// Bucket ID, optionally with a subfolder (e.g. "user/bucket" or "user/bucket/path/to/dir")
        bucket_id: String,
        /// Local directory where the filesystem will be mounted
        mount_point: PathBuf,
    },
    /// Mount a HuggingFace repo read-only (type auto-detected from prefix)
    Repo {
        /// Repo ID, optionally with a subfolder (e.g. "user/model", "user/model/sub/dir", "datasets/user/ds/train")
        repo_id: String,
        /// Local directory where the filesystem will be mounted
        mount_point: PathBuf,
        /// Git revision to mount
        #[arg(long, default_value = "main")]
        revision: String,
    },
    /// Mount a Gitea repository read-only
    Gitea {
        /// Repository in owner/repo format, optionally with a subfolder (e.g. "user/repo/sub/dir")
        repo_id: String,
        /// Local directory where the filesystem will be mounted
        mount_point: PathBuf,
        /// Git revision (branch, tag, or commit) to mount
        #[arg(long, default_value = "main")]
        revision: String,
    },
    /// Mount a GitHub repository read-only (github.com or GitHub Enterprise Server)
    Github {
        /// Repository in owner/repo format, optionally with a subfolder (e.g. "user/repo/sub/dir")
        repo_id: String,
        /// Local directory where the filesystem will be mounted
        mount_point: PathBuf,
        /// Git revision (branch, tag, or commit) to mount
        #[arg(long, default_value = "main")]
        revision: String,
    },
    /// Mount a GitLab repository read-only (gitlab.com or self-hosted)
    Gitlab {
        /// Repository in owner/repo format, optionally with a subfolder (e.g. "user/repo/sub/dir")
        repo_id: String,
        /// Local directory where the filesystem will be mounted
        mount_point: PathBuf,
        /// Git revision (branch, tag, or commit) to mount
        #[arg(long, default_value = "main")]
        revision: String,
    },
}

impl Source {
    pub fn mount_point(&self) -> &Path {
        match self {
            Source::Bucket { mount_point, .. }
            | Source::Repo { mount_point, .. }
            | Source::Gitea { mount_point, .. }
            | Source::Github { mount_point, .. }
            | Source::Gitlab { mount_point, .. } => mount_point,
        }
    }

    /// Human-readable label matching `SourceKind::Display` format.
    pub fn label(&self) -> String {
        match self {
            Source::Bucket { bucket_id, .. } => format!("bucket/{bucket_id}"),
            Source::Repo { repo_id, revision, .. } => {
                let (repo_type, parsed_id) = parse_repo_id(repo_id);
                format!("{repo_type}/{parsed_id}/{revision}")
            }
            Source::Gitea { repo_id, revision, .. } => {
                format!("gitea/{repo_id}/{revision}")
            }
            Source::Github { repo_id, revision, .. } => {
                format!("github/{repo_id}/{revision}")
            }
            Source::Gitlab { repo_id, revision, .. } => {
                format!("gitlab/{repo_id}/{revision}")
            }
        }
    }
}

/// Mount options shared across all binaries (FUSE, NFS, daemon).
#[derive(clap::Args)]
pub struct MountOptions {
    /// HuggingFace API token (also read from HF_TOKEN env var).
    /// Required for private repos/buckets, optional for public repos.
    #[arg(long, env = "HF_TOKEN")]
    pub hf_token: Option<String>,

    /// Path to a file containing the API token. The file is re-read before
    /// each Hub request, allowing external credential managers to refresh
    /// tokens without remounting. Takes precedence over --hf-token when
    /// the file exists and is non-empty.
    #[arg(long)]
    pub token_file: Option<PathBuf>,

    /// HuggingFace Hub endpoint URL
    #[arg(long, default_value = "https://huggingface.co")]
    pub hub_endpoint: String,

    /// Gitea server endpoint URL (required for gitea subcommand)
    #[arg(long, env = "GITEA_ENDPOINT")]
    pub gitea_endpoint: Option<String>,

    /// Gitea API token (also read from GITEA_TOKEN env var)
    #[arg(long, env = "GITEA_TOKEN")]
    pub gitea_token: Option<String>,

    /// GitHub server endpoint URL (defaults to https://github.com)
    #[arg(long, env = "GITHUB_ENDPOINT", default_value = "https://github.com")]
    pub github_endpoint: String,

    /// GitHub personal access token (also read from GITHUB_TOKEN env var)
    #[arg(long, env = "GITHUB_TOKEN")]
    pub github_token: Option<String>,

    /// GitLab server endpoint URL (defaults to https://gitlab.com)
    #[arg(long, env = "GITLAB_ENDPOINT", default_value = "https://gitlab.com")]
    pub gitlab_endpoint: String,

    /// GitLab personal/project access token (also read from GITLAB_TOKEN env var)
    #[arg(long, env = "GITLAB_TOKEN")]
    pub gitlab_token: Option<String>,

    /// Directory for on-disk caches (file chunks, staging files)
    #[arg(long, default_value = "/tmp/llmfs-mount-cache")]
    pub cache_dir: PathBuf,

    /// Override the UID for all files and directories (defaults to current user)
    #[arg(long)]
    pub uid: Option<u32>,

    /// Override the GID for all files and directories (defaults to current group)
    #[arg(long)]
    pub gid: Option<u32>,

    /// Mount in read-only mode (no writes allowed)
    #[arg(long, default_value_t = false)]
    pub read_only: bool,

    /// Use staging files + async flush for writes (supports random writes and seek).
    /// Default mode is append-only with synchronous close.
    #[arg(long, default_value_t = false)]
    pub advanced_writes: bool,

    /// Interval in seconds for polling remote changes (0 to disable).
    #[arg(long, default_value_t = 30)]
    pub poll_interval_secs: u64,

    /// Maximum size in bytes for the on-disk chunk cache.
    #[arg(long, default_value_t = 10_000_000_000)]
    pub cache_size: u64,

    /// Disable the on-disk chunk cache. Every read fetches data from
    /// HF storage (no local disk caching between reads). Useful for
    /// benchmarking without cache effects.
    #[arg(long, default_value_t = false)]
    pub no_disk_cache: bool,

    /// Bypass the kernel page cache (FOPEN_DIRECT_IO). Every read goes
    /// through the FUSE handler instead of being served from cached pages.
    /// Useful for benchmarking; not recommended for production (disables
    /// efficient mmap caching).
    #[arg(long, default_value_t = false)]
    pub direct_io: bool,

    /// Kernel metadata cache TTL in milliseconds. Controls how long file
    /// attributes are trusted before re-checking via HEAD. Lower values
    /// give fresher metadata but increase latency on directory traversals
    /// (e.g. `du`, `find`, `ls -lR`) since each file lookup triggers a
    /// HEAD request after the TTL expires.
    #[arg(long, default_value_t = 10_000)]
    pub metadata_ttl_ms: u64,

    /// Always HEAD on every lookup (skip in-memory TTL cache).
    #[arg(long, default_value_t = false)]
    pub metadata_ttl_minimal: bool,

    /// Maximum number of FUSE worker threads
    #[arg(long, default_value_t = 16)]
    pub max_threads: usize,

    /// Flush debounce delay in milliseconds. After the first dirty file is
    /// enqueued, the flush batch waits this long for more writes before firing.
    #[arg(long, default_value_t = 2_000)]
    pub flush_debounce_ms: u64,

    /// Maximum flush batch window in milliseconds. A dirty file will be flushed
    /// within this time regardless of ongoing writes resetting the debounce.
    #[arg(long, default_value_t = 30_000)]
    pub flush_max_batch_window_ms: u64,

    /// Disable filtering of OS junk files (.DS_Store, Thumbs.db, etc.).
    /// By default these files are rejected on create/mkdir/rename.
    #[arg(long, default_value_t = false)]
    pub no_filter_os_files: bool,

    /// Restrict mount access to the mounting user only (FUSE only).
    /// By default all users can access the mount.
    /// When not set, requires `user_allow_other` in /etc/fuse.conf on Linux.
    #[arg(long, default_value_t = false)]
    pub fuse_owner_only: bool,
}

/// CLI args for the foreground FUSE/NFS binaries.
#[derive(Parser)]
#[command(about = "Mount a HuggingFace bucket or repo as a filesystem", version)]
pub struct Args {
    #[command(subcommand)]
    pub source: Source,

    #[command(flatten)]
    pub options: MountOptions,
}

/// Everything needed to run a mount backend (FUSE or NFS).
pub struct MountSetup {
    pub runtime: tokio::runtime::Runtime,
    pub virtual_fs: Arc<VirtualFs>,
    pub mount_point: PathBuf,
    pub read_only: bool,
    pub advanced_writes: bool,
    pub direct_io: bool,
    pub metadata_ttl: std::time::Duration,
    pub max_threads: usize,
    pub metadata_ttl_ms: u64,
    pub fuse_owner_only: bool,
}

// ── Tracing + env vars (no threads) ──────────────────────────────────

/// Initialize tracing and xet-core env vars.
/// No threads are spawned. Safe to fork() after this returns.
pub fn init_tracing(daemon: bool) {
    // Use RUST_LOG if set, otherwise default to llmfs_mount=info.
    let filter = if std::env::var("RUST_LOG").is_ok() {
        tracing_subscriber::EnvFilter::from_default_env()
    } else {
        tracing_subscriber::EnvFilter::new("llmfs_mount=info")
    };
    // Disable ANSI colors when daemonizing (output goes to a log file)
    // or when stderr is not a terminal.
    let ansi = !daemon && std::io::stderr().is_terminal();
    tracing_subscriber::fmt().with_env_filter(filter).with_ansi(ansi).init();

    // Tune xet-core for interactive FUSE reads (not batch downloads).
    for (k, v) in [
        ("HF_XET_CLIENT_AC_INITIAL_DOWNLOAD_CONCURRENCY", "16"),
        ("HF_XET_CLIENT_AC_MIN_BYTES_REQUIRED_FOR_ADJUSTMENT", "4194304"),
        ("HF_XET_RECONSTRUCTION_MIN_RECONSTRUCTION_FETCH_SIZE", "8388608"),
        ("HF_XET_RECONSTRUCTION_MIN_PREFETCH_BUFFER", "8388608"),
        ("HF_XET_RECONSTRUCTION_TARGET_BLOCK_COMPLETION_TIME", "30"),
        ("HF_XET_RECONSTRUCTION_DOWNLOAD_BUFFER_SIZE", "134217728"),
        ("HF_XET_RECONSTRUCTION_DOWNLOAD_BUFFER_LIMIT", "268435456"),
        // Raise read_timeout from 120s default so large shard uploads don't get killed
        // by the global client read_timeout before the per-request timeout kicks in.
        ("HF_XET_CLIENT_READ_TIMEOUT", "600"),
        // Upload tuning: skip slow adaptive concurrency ramp-up
        ("HF_XET_CLIENT_AC_INITIAL_UPLOAD_CONCURRENCY", "16"),
        // Larger ingestion blocks = fewer CDC calls
        ("HF_XET_DATA_INGESTION_BLOCK_SIZE", "16777216"),
    ] {
        if std::env::var(k).is_err() {
            // SAFETY: called before any threads are spawned.
            unsafe { std::env::set_var(k, v) };
        }
    }
}

// ── Build runtime + VFS (spawns threads) ─────────────────────────────

/// Build tokio runtime, storage client, Hub client, and VFS.
/// `is_nfs` controls whether advanced writes are forced (NFS has no open/close).
pub fn build(source: Source, options: MountOptions, is_nfs: bool) -> MountSetup {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Failed to create tokio runtime");

    // Dispatch to provider-specific build paths.
    match source {
        Source::Gitea {
            repo_id,
            mount_point,
            revision,
        } => build_gitea(runtime, repo_id, mount_point, revision, options, is_nfs),
        Source::Github {
            repo_id,
            mount_point,
            revision,
        } => build_github(runtime, repo_id, mount_point, revision, options, is_nfs),
        Source::Gitlab {
            repo_id,
            mount_point,
            revision,
        } => build_gitlab(runtime, repo_id, mount_point, revision, options, is_nfs),
        _ => build_hf(runtime, source, options, is_nfs),
    }
}

/// Build a Gitea-backed mount (read-only, HTTP-based content download).
fn build_gitea(
    runtime: tokio::runtime::Runtime,
    repo_id: String,
    mount_point: PathBuf,
    revision: String,
    options: MountOptions,
    is_nfs: bool,
) -> MountSetup {
    let gitea_endpoint = options
        .gitea_endpoint
        .as_deref()
        .unwrap_or_else(|| panic!("--gitea-endpoint (or GITEA_ENDPOINT env var) is required for Gitea mounts"));

    let (id, prefix) = split_path_prefix(&repo_id).unwrap_or_else(|e| panic!("invalid gitea repo path: {e}"));
    let (owner, repo_name) = id
        .split_once('/')
        .unwrap_or_else(|| panic!("Gitea repo must be in owner/repo format, got: {id}"));

    let client = runtime.block_on(async {
        GiteaClient::new(
            gitea_endpoint,
            options.gitea_token.as_deref(),
            owner,
            repo_name,
            &revision,
            prefix.to_string(),
        )
        .await
        .unwrap_or_else(|e| panic!("Failed to initialize Gitea client: {e}"))
    });

    if !client.path_prefix().is_empty() {
        runtime.block_on(async {
            client.validate_path_prefix().await.unwrap_or_else(|e| panic!("{e}"));
        });
    }
    build_external_provider(runtime, client, mount_point, prefix, "Gitea", options, is_nfs)
}

/// Build a GitHub-backed mount (read-only, HTTP-based content download).
fn build_github(
    runtime: tokio::runtime::Runtime,
    repo_id: String,
    mount_point: PathBuf,
    revision: String,
    options: MountOptions,
    is_nfs: bool,
) -> MountSetup {
    let (id, prefix) = split_path_prefix(&repo_id).unwrap_or_else(|e| panic!("invalid github repo path: {e}"));
    let (owner, repo_name) = id
        .split_once('/')
        .unwrap_or_else(|| panic!("GitHub repo must be in owner/repo format, got: {id}"));

    let client = runtime.block_on(async {
        GitHubClient::new(
            &options.github_endpoint,
            options.github_token.as_deref(),
            owner,
            repo_name,
            &revision,
            prefix.to_string(),
        )
        .await
        .unwrap_or_else(|e| panic!("Failed to initialize GitHub client: {e}"))
    });

    if !client.path_prefix().is_empty() {
        runtime.block_on(async {
            client.validate_path_prefix().await.unwrap_or_else(|e| panic!("{e}"));
        });
    }
    build_external_provider(runtime, client, mount_point, prefix, "GitHub", options, is_nfs)
}

/// Build a GitLab-backed mount (read-only, HTTP-based content download).
fn build_gitlab(
    runtime: tokio::runtime::Runtime,
    repo_id: String,
    mount_point: PathBuf,
    revision: String,
    options: MountOptions,
    is_nfs: bool,
) -> MountSetup {
    let (id, prefix) = split_path_prefix(&repo_id).unwrap_or_else(|e| panic!("invalid gitlab repo path: {e}"));
    let (owner, repo_name) = id
        .split_once('/')
        .unwrap_or_else(|| panic!("GitLab repo must be in owner/repo format, got: {id}"));

    let client = runtime.block_on(async {
        GitLabClient::new(
            &options.gitlab_endpoint,
            options.gitlab_token.as_deref(),
            owner,
            repo_name,
            &revision,
            prefix.to_string(),
        )
        .await
        .unwrap_or_else(|e| panic!("Failed to initialize GitLab client: {e}"))
    });

    if !client.path_prefix().is_empty() {
        runtime.block_on(async {
            client.validate_path_prefix().await.unwrap_or_else(|e| panic!("{e}"));
        });
    }
    build_external_provider(runtime, client, mount_point, prefix, "GitLab", options, is_nfs)
}

/// Shared builder for all external (non-HF) providers.
/// The provider client (`hub_client`) already implements `HubOps`.
fn build_external_provider(
    runtime: tokio::runtime::Runtime,
    hub_client: Arc<dyn HubOps>,
    mount_point: PathBuf,
    prefix: &str,
    provider_name: &str,
    options: MountOptions,
    is_nfs: bool,
) -> MountSetup {
    let read_only = true;
    if !options.read_only {
        info!("{provider_name} mounts are always read-only");
    }

    let xet_sessions: Arc<dyn XetOps> = Arc::new(NoOpXet);

    std::fs::create_dir_all(&options.cache_dir)
        .unwrap_or_else(|e| panic!("Failed to create cache dir {:?}: {e}", options.cache_dir));

    let staging_dir = Some(StagingDir::new(&options.cache_dir));

    let uid = options.uid.unwrap_or_else(|| unsafe { libc::getuid() });
    let gid = options.gid.unwrap_or_else(|| unsafe { libc::getgid() });

    if let Err(e) = std::fs::create_dir_all(&mount_point)
        && e.raw_os_error() != Some(libc::EEXIST)
    {
        panic!("Failed to create mount point {:?}: {e}", mount_point);
    }

    let backend_name = if is_nfs { "nfs" } else { "fuse" };
    let subfolder_info = if prefix.is_empty() {
        String::new()
    } else {
        format!(" (subfolder: {})", prefix)
    };
    info!(
        "Mounting {}{} at {:?} (read-only, backend={})",
        hub_client.source(),
        subfolder_info,
        mount_point,
        backend_name,
    );

    let metadata_ttl = std::time::Duration::from_millis(options.metadata_ttl_ms);

    let virtual_fs = VirtualFs::new(
        runtime.handle().clone(),
        hub_client,
        xet_sessions,
        staging_dir,
        VfsConfig {
            read_only,
            advanced_writes: false,
            uid,
            gid,
            poll_interval_secs: options.poll_interval_secs,
            metadata_ttl,
            serve_lookup_from_cache: !options.metadata_ttl_minimal,
            filter_os_files: !options.no_filter_os_files,
            direct_io: options.direct_io && !is_nfs,
            flush_debounce: std::time::Duration::from_millis(options.flush_debounce_ms),
            flush_max_batch_window: std::time::Duration::from_millis(options.flush_max_batch_window_ms),
        },
    );

    MountSetup {
        runtime,
        virtual_fs,
        mount_point,
        read_only,
        advanced_writes: false,
        direct_io: options.direct_io,
        metadata_ttl,
        max_threads: options.max_threads,
        metadata_ttl_ms: options.metadata_ttl_ms,
        fuse_owner_only: options.fuse_owner_only,
    }
}

/// Build a HuggingFace-backed mount (original flow).
fn build_hf(runtime: tokio::runtime::Runtime, source: Source, options: MountOptions, is_nfs: bool) -> MountSetup {
    let (mount_point, source_kind, path_prefix) = match source {
        Source::Bucket { bucket_id, mount_point } => {
            let (id, prefix) = split_path_prefix(&bucket_id).unwrap_or_else(|e| panic!("invalid bucket path: {e}"));
            (
                mount_point,
                SourceKind::Bucket {
                    bucket_id: id.to_string(),
                },
                prefix.to_string(),
            )
        }
        Source::Repo {
            repo_id,
            mount_point,
            revision,
        } => {
            let (repo_type, rest) = parse_repo_id(&repo_id);
            let (id, prefix) = split_path_prefix(&rest).unwrap_or_else(|e| panic!("invalid repo path: {e}"));
            (
                mount_point,
                SourceKind::Repo {
                    repo_id: id.to_string(),
                    repo_type,
                    revision,
                },
                prefix.to_string(),
            )
        }
        Source::Gitea { .. } | Source::Github { .. } | Source::Gitlab { .. } => {
            unreachable!("external provider sources are handled in their own build functions")
        }
    };

    let backend = if is_nfs { "nfs" } else { "fuse" };
    let hub_client = runtime.block_on(async {
        HubApiClient::from_source(
            &options.hub_endpoint,
            options.hf_token.as_deref(),
            options.token_file.clone(),
            source_kind,
            path_prefix,
            backend,
        )
        .await
        .unwrap_or_else(|e| panic!("Failed to initialize Hub client: {e}"))
    });

    // Validate that the subfolder exists on the remote.
    if !hub_client.path_prefix().is_empty() {
        runtime.block_on(async {
            hub_client.validate_path_prefix().await.unwrap_or_else(|e| {
                panic!("{e}");
            });
        });
    }

    let read_only = options.read_only || hub_client.is_repo();
    if hub_client.is_repo() && !options.read_only {
        info!("Repo mounts are always read-only");
    }

    let refresher = hub_client.token_refresher(read_only);
    let cas_config = build_cas_config(&runtime, &refresher);

    // Ensure cache directory exists and is writable (needed for staging even without chunk cache).
    std::fs::create_dir_all(&options.cache_dir)
        .unwrap_or_else(|e| panic!("Failed to create cache dir {:?}: {e}", options.cache_dir));

    let xorb_cache = if options.no_disk_cache {
        None
    } else {
        let xorbs_dir = options.cache_dir.join("xorbs");
        std::fs::create_dir_all(&xorbs_dir)
            .unwrap_or_else(|e| panic!("Failed to create xorbs dir {:?}: {e}", xorbs_dir));
        let config = CacheConfig {
            cache_directory: xorbs_dir,
            cache_size: options.cache_size,
        };
        Some(get_cache(&config).expect("Failed to create chunk cache"))
    };

    let raw_client = runtime
        .block_on(create_remote_client(
            &cas_config,
            &uuid::Uuid::new_v4().to_string(),
            false,
        ))
        .expect("Failed to create storage client");
    let cached_client = CachedXetClient::new(raw_client);
    let download_session = FileDownloadSession::from_client(cached_client.clone(), None, xorb_cache);
    let upload_config = if read_only { None } else { Some(cas_config) };
    let xet_sessions = XetSessions::new(download_session, upload_config, cached_client);

    let advanced_writes = options.advanced_writes || (is_nfs && !read_only);
    // Repos need a staging dir for HTTP download cache (open_readonly),
    // even when advanced_writes is disabled.
    let staging_dir = if advanced_writes || hub_client.is_repo() {
        Some(StagingDir::new(&options.cache_dir))
    } else {
        None
    };

    let uid = options.uid.unwrap_or_else(|| unsafe { libc::getuid() });
    let gid = options.gid.unwrap_or_else(|| unsafe { libc::getgid() });

    // Ignore EEXIST: the directory may already exist from a previous (possibly
    // stale) mount. FUSE/NFS will fail at mount time if it's actually busy.
    if let Err(e) = std::fs::create_dir_all(&mount_point)
        && e.raw_os_error() != Some(libc::EEXIST)
    {
        panic!("Failed to create mount point {:?}: {e}", mount_point);
    }

    if is_nfs && options.direct_io {
        info!("--direct-io is ignored for NFS mounts (no NFS equivalent)");
    }

    let backend_name = if is_nfs { "nfs" } else { "fuse" };
    let subfolder_info = if hub_client.path_prefix().is_empty() {
        String::new()
    } else {
        format!(" (subfolder: {})", hub_client.path_prefix())
    };
    info!(
        "Mounting {}{} at {:?} ({}, backend={})",
        hub_client.source(),
        subfolder_info,
        mount_point,
        if read_only { "read-only" } else { "read-write" },
        backend_name,
    );
    info!(
        "Config: advanced_writes={} direct_io={} poll_interval={}s metadata_ttl={}ms \
         cache_dir={:?} cache_size={} no_disk_cache={} max_threads={} \
         flush_debounce={}ms flush_max_batch={}ms uid={} gid={} filter_os_files={}",
        advanced_writes,
        options.direct_io,
        options.poll_interval_secs,
        options.metadata_ttl_ms,
        options.cache_dir,
        options.cache_size,
        options.no_disk_cache,
        options.max_threads,
        options.flush_debounce_ms,
        options.flush_max_batch_window_ms,
        uid,
        gid,
        !options.no_filter_os_files,
    );

    let metadata_ttl = std::time::Duration::from_millis(options.metadata_ttl_ms);

    let virtual_fs = VirtualFs::new(
        runtime.handle().clone(),
        hub_client,
        xet_sessions,
        staging_dir,
        VfsConfig {
            read_only,
            advanced_writes,
            uid,
            gid,
            poll_interval_secs: options.poll_interval_secs,
            metadata_ttl,
            serve_lookup_from_cache: !options.metadata_ttl_minimal,
            filter_os_files: !options.no_filter_os_files,
            direct_io: options.direct_io && !is_nfs,
            flush_debounce: std::time::Duration::from_millis(options.flush_debounce_ms),
            flush_max_batch_window: std::time::Duration::from_millis(options.flush_max_batch_window_ms),
        },
    );

    MountSetup {
        runtime,
        virtual_fs,
        mount_point,
        read_only,
        advanced_writes,
        direct_io: options.direct_io,
        metadata_ttl,
        max_threads: options.max_threads,
        metadata_ttl_ms: options.metadata_ttl_ms,
        fuse_owner_only: options.fuse_owner_only,
    }
}

// ── Combined entry point (foreground binaries) ──────────────────────

/// Parse CLI args, build VFS and all dependencies.
/// `is_nfs` controls whether advanced writes are forced (NFS has no open/close).
pub fn setup(is_nfs: bool) -> MountSetup {
    raise_fd_limit();
    let args = Args::parse();
    init_tracing(false);
    build(args.source, args.options, is_nfs)
}

/// Try to raise the soft file descriptor limit to avoid "Too many open files"
/// errors during large batch operations. Most FUSE/NFS filesystems do this.
pub fn raise_fd_limit() {
    const TARGET_NOFILE: u64 = 65536;
    let mut rlim = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: rlim is a plain C struct, getrlimit/setrlimit are standard POSIX.
    if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut rlim) } != 0 || rlim.rlim_cur >= TARGET_NOFILE {
        return;
    }
    rlim.rlim_cur = TARGET_NOFILE.min(rlim.rlim_max);
    if unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &rlim) } != 0 {
        eprintln!("warning: failed to raise file descriptor limit to {TARGET_NOFILE}");
    }
}

fn build_cas_config(runtime: &tokio::runtime::Runtime, refresher: &Arc<HubTokenRefresher>) -> Arc<TranslatorConfig> {
    let jwt = runtime
        .block_on(refresher.fetch_initial())
        .unwrap_or_else(|e| panic!("Failed to get storage token: {e}"));
    info!("Got storage token for endpoint: {}", jwt.cas_url);
    Arc::new(
        default_config(
            jwt.cas_url,
            None,
            Some((jwt.access_token, jwt.exp)),
            Some(refresher.clone()),
            None,
        )
        .unwrap_or_else(|e| panic!("Failed to build TranslatorConfig: {e}")),
    )
}
