use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use reqwest::Client;
use serde::Deserialize;
use tokio::io::AsyncWriteExt;
use tracing::info;

use crate::error::{Error, Result};
use crate::hub_api::{BatchOp, HeadFileInfo, HubOps, SourceKind, TreeEntry, mtime_from_str, send_with_retry};
use crate::xet::{DownloadStreamOps, StreamingWriterOps, XetOps};

// ── Gitea API response types ──────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct GiteaRepoInfo {
    default_branch: Option<String>,
    updated_at: Option<String>,
}

/// Entry in the tree response from `/repos/{owner}/{repo}/git/trees/{ref}`.
#[derive(Debug, Deserialize)]
struct GiteaTreeEntry {
    path: String,
    /// "blob" (file), "tree" (directory), "commit" (submodule)
    #[serde(rename = "type")]
    entry_type: String,
    sha: String,
    size: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct GiteaTreeResponse {
    tree: Vec<GiteaTreeEntry>,
    truncated: bool,
    #[allow(dead_code)]
    page: Option<u64>,
    #[allow(dead_code)]
    total_count: Option<u64>,
}

/// Entry in the contents response from `/repos/{owner}/{repo}/contents/{filepath}`.
#[derive(Debug, Deserialize)]
struct GiteaContentsResponse {
    sha: Option<String>,
    size: Option<u64>,
    #[serde(rename = "type")]
    #[allow(dead_code)]
    entry_type: Option<String>,
    #[allow(dead_code)]
    last_commit_sha: Option<String>,
}

// ── GiteaClient ────────────────────────────────────────────────────────

pub struct GiteaClient {
    client: Client,
    endpoint: String,
    owner: String,
    repo: String,
    revision: String,
    token: Option<String>,
    last_modified: SystemTime,
    path_prefix: String,
    source: SourceKind,
}

impl GiteaClient {
    /// Create a client for a Gitea repository.
    ///
    /// `endpoint` is the base URL of the Gitea instance (e.g. `https://codeberg.org`).
    /// `owner` and `repo` identify the repository.
    /// `revision` is the git ref (branch, tag, or commit hash).
    pub async fn new(
        endpoint: &str,
        token: Option<&str>,
        owner: &str,
        repo: &str,
        revision: &str,
        path_prefix: String,
    ) -> Result<Arc<Self>> {
        let endpoint = endpoint.trim_end_matches('/').to_string();
        let user_agent = format!("llmfs-mount/{}; provider/gitea", env!("CARGO_PKG_VERSION"));
        let client = Client::builder()
            .user_agent(&user_agent)
            .build()
            .expect("failed to build HTTP client");

        // Fetch repo info to validate access and get last modified time.
        let url = format!("{}/api/v1/repos/{}/{}", endpoint, owner, repo);
        let resp = send_with_retry(
            || {
                let mut req = client.get(&url);
                if let Some(t) = token {
                    req = req.header("Authorization", format!("token {t}"));
                }
                req
            },
            &format!("gitea repo info {owner}/{repo}"),
            false,
        )
        .await?;

        let info: GiteaRepoInfo = resp.json().await?;
        let last_modified = info.updated_at.as_deref().map(mtime_from_str).unwrap_or(UNIX_EPOCH);

        // Use the repo's default branch if no revision was specified.
        let effective_revision = if revision == "main" {
            info.default_branch.unwrap_or_else(|| "main".to_string())
        } else {
            revision.to_string()
        };

        let source = SourceKind::GiteaRepo {
            endpoint: endpoint.clone(),
            owner: owner.to_string(),
            repo: repo.to_string(),
            revision: effective_revision.clone(),
        };

        Ok(Arc::new(Self {
            client,
            endpoint,
            owner: owner.to_string(),
            repo: repo.to_string(),
            revision: effective_revision,
            token: token.map(|t| t.to_string()),
            last_modified,
            path_prefix,
            source,
        }))
    }

    /// Attach authentication header to a request.
    /// Gitea uses `Authorization: token <access_token>`.
    fn auth(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.token {
            Some(t) => req.header("Authorization", format!("token {t}")),
            None => req,
        }
    }

    /// Join `path_prefix` and `path` into the full API path.
    fn prefixed_path(&self, path: &str) -> String {
        if self.path_prefix.is_empty() {
            path.to_string()
        } else if path.is_empty() {
            self.path_prefix.clone()
        } else {
            format!("{}/{}", self.path_prefix, path)
        }
    }

    /// Strip `path_prefix` from the beginning of a full path.
    fn strip_path_prefix<'a>(&self, full: &'a str) -> Option<&'a str> {
        if self.path_prefix.is_empty() {
            return Some(full);
        }
        if full == self.path_prefix {
            return Some("");
        }
        full.strip_prefix(&self.path_prefix)
            .and_then(|rest| rest.strip_prefix('/'))
    }

    /// Return the path prefix this client is scoped to.
    pub fn path_prefix(&self) -> &str {
        &self.path_prefix
    }

    /// Validate that the path prefix exists on the remote.
    pub async fn validate_path_prefix(&self) -> Result<()> {
        if self.path_prefix.is_empty() {
            return Ok(());
        }
        let entries = self.list_tree("").await.map_err(|e| {
            Error::hub(format!(
                "subfolder '{}' not found in {}: {e}",
                self.path_prefix, self.source
            ))
        })?;
        if entries.is_empty() {
            return Err(Error::hub(format!(
                "subfolder '{}' is empty or does not exist in {}",
                self.path_prefix, self.source,
            )));
        }
        Ok(())
    }

    /// List tree entries using the Gitea Git Trees API with recursive listing.
    /// Handles pagination when results are truncated.
    async fn list_tree_recursive(&self, _prefix: &str) -> Result<Vec<TreeEntry>> {
        let mut all_entries = Vec::new();
        let mut page = 1u64;

        loop {
            let url = format!(
                "{}/api/v1/repos/{}/{}/git/trees/{}?recursive=true&per_page=1000&page={}",
                self.endpoint, self.owner, self.repo, self.revision, page,
            );

            let resp = send_with_retry(|| self.auth(self.client.get(&url)), "gitea tree listing", false).await?;

            let tree_resp: GiteaTreeResponse = resp.json().await?;

            for entry in tree_resp.tree {
                // Skip submodules.
                if entry.entry_type == "commit" {
                    continue;
                }

                let entry_type = if entry.entry_type == "tree" {
                    "directory"
                } else {
                    "file"
                };

                all_entries.push(TreeEntry {
                    path: entry.path,
                    entry_type: entry_type.to_string(),
                    size: entry.size,
                    // Gitea files have no xet hash.
                    xet_hash: None,
                    // Use git blob SHA as OID for change detection.
                    oid: Some(entry.sha),
                    mtime: None,
                });
            }

            if !tree_resp.truncated {
                break;
            }
            page += 1;
        }

        Ok(all_entries)
    }
}

#[async_trait::async_trait]
impl HubOps for GiteaClient {
    async fn list_tree(&self, prefix: &str) -> Result<Vec<TreeEntry>> {
        let api_prefix = self.prefixed_path(prefix);

        // Use recursive tree listing and filter to direct children of prefix.
        let all_entries = self.list_tree_recursive(&api_prefix).await?;

        // Filter to entries under the prefix (direct children only).
        let prefix_slash = if api_prefix.is_empty() {
            String::new()
        } else {
            format!("{}/", api_prefix)
        };

        let mut result = Vec::new();
        let mut seen_dirs = std::collections::HashSet::new();

        for entry in all_entries {
            let relative = if api_prefix.is_empty() {
                entry.path.as_str()
            } else if entry.path == api_prefix && entry.entry_type == "directory" {
                // The prefix directory itself — skip.
                continue;
            } else if let Some(rest) = entry.path.strip_prefix(&prefix_slash) {
                rest
            } else {
                continue;
            };

            if let Some(slash) = relative.find('/') {
                // Nested path: synthesize a directory entry for the immediate child.
                let dir_name = &relative[..slash];
                let dir_path = if api_prefix.is_empty() {
                    dir_name.to_string()
                } else {
                    format!("{}/{}", api_prefix, dir_name)
                };
                if seen_dirs.insert(dir_path.clone()) {
                    result.push(TreeEntry {
                        path: dir_path,
                        entry_type: "directory".to_string(),
                        size: None,
                        xet_hash: None,
                        oid: None,
                        mtime: None,
                    });
                }
            } else {
                result.push(entry);
            }
        }

        // Strip path prefix from returned entries.
        if !self.path_prefix.is_empty() {
            result.retain_mut(|e| match self.strip_path_prefix(&e.path) {
                Some(stripped) if !stripped.is_empty() => {
                    e.path = stripped.to_string();
                    true
                }
                _ => false,
            });
        }

        Ok(result)
    }

    async fn head_file(&self, path: &str) -> Result<Option<HeadFileInfo>> {
        let api_path = self.prefixed_path(path);
        let url = format!(
            "{}/api/v1/repos/{}/{}/contents/{}?ref={}",
            self.endpoint, self.owner, self.repo, api_path, self.revision,
        );

        let resp = send_with_retry(|| self.auth(self.client.get(&url)), "gitea head_file", false).await;

        let resp = match resp {
            Ok(r) => r,
            Err(Error::Hub { status: Some(404), .. }) => return Ok(None),
            Err(err) => return Err(err),
        };

        let info: GiteaContentsResponse = resp.json().await?;

        Ok(Some(HeadFileInfo {
            // No xet hash for Gitea files.
            xet_hash: None,
            etag: info.sha,
            size: info.size,
            last_modified: None,
        }))
    }

    async fn batch_operations(&self, _ops: &[BatchOp]) -> Result<()> {
        // Write operations are not supported for Gitea in read-only mode.
        Err(Error::hub("batch operations not supported for Gitea repos (read-only)"))
    }

    async fn download_file_http(&self, path: &str, dest: &Path) -> Result<()> {
        let api_path = self.prefixed_path(path);
        // Use the /media/ endpoint which transparently resolves LFS pointers.
        let url = format!(
            "{}/api/v1/repos/{}/{}/media/{}?ref={}",
            self.endpoint, self.owner, self.repo, api_path, self.revision,
        );

        // Read cached ETag if present.
        let etag_path = dest.with_extension("etag");
        let cached_etag = if dest.exists() {
            std::fs::read_to_string(&etag_path).ok()
        } else {
            None
        };

        info!("HTTP download (gitea): {} → {:?}", path, dest);
        let resp = send_with_retry(
            || {
                let mut r = self.auth(self.client.get(&url));
                if let Some(ref etag) = cached_etag {
                    r = r.header("If-None-Match", format!("\"{}\"", etag.trim()));
                }
                r
            },
            "gitea HTTP download",
            false,
        )
        .await;

        let resp = match resp {
            Ok(r) => r,
            Err(Error::Hub { status: Some(304), .. }) => {
                info!("HTTP cache hit (304): {}", path);
                return Ok(());
            }
            Err(err) => return Err(err),
        };

        let new_etag = resp
            .headers()
            .get("etag")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.trim_matches('"').to_string());

        // Stream response body to a temp file, then atomic-rename.
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let tmp = dest.with_extension(format!("tmp.{}", std::process::id()));
        let result: std::result::Result<(), Error> = async {
            let mut file = tokio::fs::File::create(&tmp).await?;
            let mut stream = resp.bytes_stream();
            use futures::StreamExt;
            while let Some(chunk) = stream.next().await {
                let chunk = chunk?;
                file.write_all(&chunk).await?;
            }
            file.shutdown().await?;
            drop(file);
            tokio::fs::rename(&tmp, dest).await?;
            if let Some(etag) = &new_etag {
                tokio::fs::write(&etag_path, etag).await.ok();
            } else {
                tokio::fs::remove_file(&etag_path).await.ok();
            }
            Ok(())
        }
        .await;
        if result.is_err() {
            tokio::fs::remove_file(&tmp).await.ok();
        }
        result
    }

    fn default_mtime(&self) -> SystemTime {
        self.last_modified
    }

    fn source(&self) -> &SourceKind {
        &self.source
    }

    fn is_repo(&self) -> bool {
        true
    }
}

// ── NoOpXet ────────────────────────────────────────────────────────────

/// Stub XetOps implementation for providers that don't use the Xet CAS protocol.
/// All content is downloaded via `HubOps::download_file_http()` instead.
/// Calling any method on this struct returns an error.
pub struct NoOpXet;

#[async_trait::async_trait]
impl XetOps for NoOpXet {
    async fn create_streaming_writer(&self) -> Result<Box<dyn StreamingWriterOps>> {
        Err(Error::hub("streaming writes not supported for this provider"))
    }

    async fn download_to_file(&self, _xet_hash: &str, _file_size: u64, _dest: &Path) -> Result<()> {
        Err(Error::hub("xet download not supported for this provider"))
    }

    async fn upload_files(&self, _paths: &[&Path]) -> Result<Vec<xet_data::processing::XetFileInfo>> {
        Err(Error::hub("xet upload not supported for this provider"))
    }

    fn download_stream_boxed(
        &self,
        _file_info: &xet_data::processing::XetFileInfo,
        _offset: u64,
        _end: Option<u64>,
    ) -> Result<Box<dyn DownloadStreamOps>> {
        Err(Error::hub("xet streaming not supported for this provider"))
    }

    async fn warm_reconstruction_cache(&self, _xet_hash: &str) {
        // No-op.
    }
}
