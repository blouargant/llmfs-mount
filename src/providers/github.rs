use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use reqwest::Client;
use serde::Deserialize;
use tokio::io::AsyncWriteExt;
use tracing::info;

use crate::error::{Error, Result};
use crate::hub_api::{
    BatchOp, HeadFileInfo, HubOps, SourceKind, TreeEntry, mtime_from_str, send_with_retry,
};

// ── GitHub API response types ─────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct GitHubRepoInfo {
    default_branch: Option<String>,
    updated_at: Option<String>,
}

/// Entry in the tree response from `/repos/{owner}/{repo}/git/trees/{ref}`.
#[derive(Debug, Deserialize)]
struct GitHubTreeEntry {
    path: String,
    /// "blob" or "tree"
    #[serde(rename = "type")]
    entry_type: String,
    sha: String,
    size: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct GitHubTreeResponse {
    tree: Vec<GitHubTreeEntry>,
    truncated: bool,
}

/// Entry in the contents response from `/repos/{owner}/{repo}/contents/{path}`.
#[derive(Debug, Deserialize)]
struct GitHubContentsResponse {
    sha: Option<String>,
    size: Option<u64>,
}

// ── GitHubClient ──────────────────────────────────────────────────────

pub struct GitHubClient {
    client: Client,
    api_base: String,
    owner: String,
    repo: String,
    revision: String,
    token: Option<String>,
    last_modified: SystemTime,
    path_prefix: String,
    source: SourceKind,
}

impl GitHubClient {
    /// Create a client for a GitHub repository.
    ///
    /// `endpoint` is the base URL (e.g. `https://github.com` or a GHES instance).
    /// For github.com the API base is `https://api.github.com`; for GHES it is
    /// `{endpoint}/api/v3`.
    pub async fn new(
        endpoint: &str,
        token: Option<&str>,
        owner: &str,
        repo: &str,
        revision: &str,
        path_prefix: String,
    ) -> Result<Arc<Self>> {
        let endpoint = endpoint.trim_end_matches('/').to_string();
        let api_base = if endpoint.contains("github.com") {
            "https://api.github.com".to_string()
        } else {
            // GitHub Enterprise Server
            format!("{}/api/v3", endpoint)
        };

        let user_agent = format!("llmfs-mount/{}; provider/github", env!("CARGO_PKG_VERSION"));
        let client = Client::builder()
            .user_agent(&user_agent)
            .build()
            .expect("failed to build HTTP client");

        // Fetch repo info to validate access and get last modified time.
        let url = format!("{}/repos/{}/{}", api_base, owner, repo);
        let resp = send_with_retry(
            || {
                let mut req = client.get(&url);
                req = req.header("Accept", "application/vnd.github+json");
                if let Some(t) = token {
                    req = req.header("Authorization", format!("Bearer {t}"));
                }
                req
            },
            &format!("github repo info {owner}/{repo}"),
            false,
        )
        .await?;

        let info: GitHubRepoInfo = resp.json().await?;
        let last_modified = info
            .updated_at
            .as_deref()
            .map(mtime_from_str)
            .unwrap_or(UNIX_EPOCH);

        let effective_revision = if revision == "main" {
            info.default_branch.unwrap_or_else(|| "main".to_string())
        } else {
            revision.to_string()
        };

        let source = SourceKind::GitHubRepo {
            endpoint: endpoint.clone(),
            owner: owner.to_string(),
            repo: repo.to_string(),
            revision: effective_revision.clone(),
        };

        Ok(Arc::new(Self {
            client,
            api_base,
            owner: owner.to_string(),
            repo: repo.to_string(),
            revision: effective_revision,
            token: token.map(|t| t.to_string()),
            last_modified,
            path_prefix,
            source,
        }))
    }

    fn auth(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        let req = req.header("Accept", "application/vnd.github+json");
        match &self.token {
            Some(t) => req.header("Authorization", format!("Bearer {t}")),
            None => req,
        }
    }

    fn prefixed_path(&self, path: &str) -> String {
        if self.path_prefix.is_empty() {
            path.to_string()
        } else if path.is_empty() {
            self.path_prefix.clone()
        } else {
            format!("{}/{}", self.path_prefix, path)
        }
    }

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

    pub fn path_prefix(&self) -> &str {
        &self.path_prefix
    }

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

    /// Recursive tree listing via the Git Trees API.
    /// GitHub returns the entire tree in one call when `recursive=1`.
    /// If the tree is truncated (>100k entries), falls back to non-recursive.
    async fn list_tree_recursive(&self) -> Result<Vec<TreeEntry>> {
        let url = format!(
            "{}/repos/{}/{}/git/trees/{}?recursive=1",
            self.api_base, self.owner, self.repo, self.revision,
        );

        let resp = send_with_retry(
            || self.auth(self.client.get(&url)),
            "github tree listing",
            false,
        )
        .await?;

        let tree_resp: GitHubTreeResponse = resp.json().await?;

        if tree_resp.truncated {
            info!("GitHub tree listing was truncated; some files may be missing");
        }

        let mut entries = Vec::with_capacity(tree_resp.tree.len());
        for entry in tree_resp.tree {
            if entry.entry_type == "commit" {
                continue;
            }
            let entry_type = if entry.entry_type == "tree" {
                "directory"
            } else {
                "file"
            };
            entries.push(TreeEntry {
                path: entry.path,
                entry_type: entry_type.to_string(),
                size: entry.size,
                xet_hash: None,
                oid: Some(entry.sha),
                mtime: None,
            });
        }
        Ok(entries)
    }
}

#[async_trait::async_trait]
impl HubOps for GitHubClient {
    async fn list_tree(&self, prefix: &str) -> Result<Vec<TreeEntry>> {
        let api_prefix = self.prefixed_path(prefix);
        let all_entries = self.list_tree_recursive().await?;

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
                continue;
            } else if let Some(rest) = entry.path.strip_prefix(&prefix_slash) {
                rest
            } else {
                continue;
            };

            if let Some(slash) = relative.find('/') {
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
            "{}/repos/{}/{}/contents/{}?ref={}",
            self.api_base, self.owner, self.repo, api_path, self.revision,
        );

        let resp = send_with_retry(
            || self.auth(self.client.get(&url)),
            "github head_file",
            false,
        )
        .await;

        let resp = match resp {
            Ok(r) => r,
            Err(Error::Hub { status: Some(404), .. }) => return Ok(None),
            Err(err) => return Err(err),
        };

        let info: GitHubContentsResponse = resp.json().await?;

        Ok(Some(HeadFileInfo {
            xet_hash: None,
            etag: info.sha,
            size: info.size,
            last_modified: None,
        }))
    }

    async fn batch_operations(&self, _ops: &[BatchOp]) -> Result<()> {
        Err(Error::hub("batch operations not supported for GitHub repos (read-only)"))
    }

    async fn download_file_http(&self, path: &str, dest: &Path) -> Result<()> {
        let api_path = self.prefixed_path(path);
        // Use the contents API with raw media type to get file content.
        // This transparently handles LFS pointers for repos with LFS.
        let url = format!(
            "{}/repos/{}/{}/contents/{}?ref={}",
            self.api_base, self.owner, self.repo, api_path, self.revision,
        );

        let etag_path = dest.with_extension("etag");
        let cached_etag = if dest.exists() {
            std::fs::read_to_string(&etag_path).ok()
        } else {
            None
        };

        info!("HTTP download (github): {} → {:?}", path, dest);
        let resp = send_with_retry(
            || {
                let mut r = self.client.get(&url);
                // Request raw content via Accept header.
                r = r.header("Accept", "application/vnd.github.raw+json");
                if let Some(ref t) = self.token {
                    r = r.header("Authorization", format!("Bearer {t}"));
                }
                if let Some(ref etag) = cached_etag {
                    r = r.header("If-None-Match", format!("\"{}\"", etag.trim()));
                }
                r
            },
            "github HTTP download",
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
