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

// ── GitLab API response types ─────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct GitLabProjectInfo {
    default_branch: Option<String>,
    last_activity_at: Option<String>,
}

/// Entry in the tree response from
/// `GET /api/v4/projects/:id/repository/tree`.
#[derive(Debug, Deserialize)]
struct GitLabTreeEntry {
    /// File or directory name (not the full path when `recursive=false`).
    /// Full path when `recursive=true`.
    path: String,
    /// "blob" or "tree"
    #[serde(rename = "type")]
    entry_type: String,
    id: String,
}

/// Entry in the file metadata response from
/// `GET /api/v4/projects/:id/repository/files/:file_path`.
#[derive(Debug, Deserialize)]
struct GitLabFileInfo {
    blob_id: Option<String>,
    size: Option<u64>,
    #[allow(dead_code)]
    last_commit_id: Option<String>,
}

// ── GitLabClient ──────────────────────────────────────────────────────

pub struct GitLabClient {
    client: Client,
    api_base: String,
    /// URL-encoded `owner/repo` (e.g. `user%2Frepo`).
    project_id: String,
    #[allow(dead_code)]
    owner: String,
    #[allow(dead_code)]
    repo: String,
    revision: String,
    token: Option<String>,
    last_modified: SystemTime,
    path_prefix: String,
    source: SourceKind,
}

impl GitLabClient {
    /// Create a client for a GitLab repository.
    ///
    /// `endpoint` is the base URL (e.g. `https://gitlab.com` or a self-hosted
    /// instance). The API base is always `{endpoint}/api/v4`.
    pub async fn new(
        endpoint: &str,
        token: Option<&str>,
        owner: &str,
        repo: &str,
        revision: &str,
        path_prefix: String,
    ) -> Result<Arc<Self>> {
        let endpoint = endpoint.trim_end_matches('/').to_string();
        let api_base = format!("{}/api/v4", endpoint);
        let project_id = urlencoding::encode(&format!("{owner}/{repo}")).into_owned();

        let user_agent = format!("llmfs-mount/{}; provider/gitlab", env!("CARGO_PKG_VERSION"));
        let client = Client::builder()
            .user_agent(&user_agent)
            .build()
            .expect("failed to build HTTP client");

        // Fetch project info to validate access and get last modified time.
        let url = format!("{}/projects/{}", api_base, project_id);
        let resp = send_with_retry(
            || {
                let mut req = client.get(&url);
                if let Some(t) = token {
                    req = req.header("PRIVATE-TOKEN", t);
                }
                req
            },
            &format!("gitlab project info {owner}/{repo}"),
            false,
        )
        .await?;

        let info: GitLabProjectInfo = resp.json().await?;
        let last_modified = info
            .last_activity_at
            .as_deref()
            .map(mtime_from_str)
            .unwrap_or(UNIX_EPOCH);

        let effective_revision = if revision == "main" {
            info.default_branch.unwrap_or_else(|| "main".to_string())
        } else {
            revision.to_string()
        };

        let source = SourceKind::GitLabRepo {
            endpoint: endpoint.clone(),
            owner: owner.to_string(),
            repo: repo.to_string(),
            revision: effective_revision.clone(),
        };

        Ok(Arc::new(Self {
            client,
            api_base,
            project_id,
            owner: owner.to_string(),
            repo: repo.to_string(),
            revision: effective_revision,
            token: token.map(|t| t.to_string()),
            last_modified,
            path_prefix,
            source,
        }))
    }

    /// Attach authentication header. GitLab uses `PRIVATE-TOKEN`.
    fn auth(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.token {
            Some(t) => req.header("PRIVATE-TOKEN", t.as_str()),
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

    /// Recursive tree listing via the Repository Tree API with pagination.
    async fn list_tree_recursive(&self) -> Result<Vec<TreeEntry>> {
        let mut all_entries = Vec::new();
        let mut page = 1u64;

        loop {
            let url = format!(
                "{}/projects/{}/repository/tree?ref={}&recursive=true&per_page=100&page={}",
                self.api_base, self.project_id, self.revision, page,
            );

            let resp = send_with_retry(
                || self.auth(self.client.get(&url)),
                "gitlab tree listing",
                false,
            )
            .await?;

            // GitLab paginates via X-Next-Page header.
            let next_page = resp
                .headers()
                .get("x-next-page")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<u64>().ok());

            let items: Vec<GitLabTreeEntry> = resp.json().await?;
            if items.is_empty() {
                break;
            }

            for entry in items {
                let entry_type = if entry.entry_type == "tree" {
                    "directory"
                } else {
                    "file"
                };
                all_entries.push(TreeEntry {
                    path: entry.path,
                    entry_type: entry_type.to_string(),
                    size: None, // GitLab tree listing doesn't include size.
                    xet_hash: None,
                    oid: Some(entry.id),
                    mtime: None,
                });
            }

            match next_page {
                Some(np) if np > page => page = np,
                _ => break,
            }
        }

        Ok(all_entries)
    }
}

#[async_trait::async_trait]
impl HubOps for GitLabClient {
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
        // GitLab requires URL-encoded file paths.
        let encoded_path = urlencoding::encode(&api_path);
        let url = format!(
            "{}/projects/{}/repository/files/{}?ref={}",
            self.api_base, self.project_id, encoded_path, self.revision,
        );

        let resp = send_with_retry(
            || self.auth(self.client.get(&url)),
            "gitlab head_file",
            false,
        )
        .await;

        let resp = match resp {
            Ok(r) => r,
            Err(Error::Hub { status: Some(404), .. }) => return Ok(None),
            Err(err) => return Err(err),
        };

        let info: GitLabFileInfo = resp.json().await?;

        Ok(Some(HeadFileInfo {
            xet_hash: None,
            etag: info.blob_id,
            size: info.size,
            last_modified: None,
        }))
    }

    async fn batch_operations(&self, _ops: &[BatchOp]) -> Result<()> {
        Err(Error::hub("batch operations not supported for GitLab repos (read-only)"))
    }

    async fn download_file_http(&self, path: &str, dest: &Path) -> Result<()> {
        let api_path = self.prefixed_path(path);
        let encoded_path = urlencoding::encode(&api_path);
        // Use the raw file endpoint for direct content download.
        let url = format!(
            "{}/projects/{}/repository/files/{}/raw?ref={}",
            self.api_base, self.project_id, encoded_path, self.revision,
        );

        let etag_path = dest.with_extension("etag");
        let cached_etag = if dest.exists() {
            std::fs::read_to_string(&etag_path).ok()
        } else {
            None
        };

        info!("HTTP download (gitlab): {} → {:?}", path, dest);
        let resp = send_with_retry(
            || {
                let mut r = self.auth(self.client.get(&url));
                if let Some(ref etag) = cached_etag {
                    r = r.header("If-None-Match", format!("\"{}\"", etag.trim()));
                }
                r
            },
            "gitlab HTTP download",
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
