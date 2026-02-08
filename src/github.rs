use anyhow::{Context, bail};
use concepts::ContentDigest;
use serde::Deserialize;
use std::{
    fmt::{self, Display},
    path::{Path, PathBuf},
    str::FromStr,
    time::Duration,
};
use tokio::io::AsyncWriteExt;
use tracing::{debug, info, instrument, warn};
use utils::sha256sum::calculate_sha256_file;

pub(crate) const GH_SCHEMA_PREFIX: &str = "gh://";
const GITHUB_CLIENT_RETRIES: u64 = 10;

/// Reference to a GitHub release asset.
/// Format: `gh://owner/repo@tag/asset-name.wasm` or `gh://owner/repo@latest/asset-name.wasm`
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub(crate) struct GitHubReleaseReference {
    pub owner: String,
    pub repo: String,
    pub tag: GitHubReleaseTag,
    pub asset_name: String,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub(crate) enum GitHubReleaseTag {
    Specific(String),
    Latest,
}

impl Display for GitHubReleaseTag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GitHubReleaseTag::Specific(tag) => write!(f, "{tag}"),
            GitHubReleaseTag::Latest => write!(f, "latest"),
        }
    }
}

impl Display for GitHubReleaseReference {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{GH_SCHEMA_PREFIX}{}/{}@{}/{}",
            self.owner, self.repo, self.tag, self.asset_name
        )
    }
}

impl FromStr for GitHubReleaseReference {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Expected format: owner/repo@tag/asset-name.wasm
        // The gh:// prefix should already be stripped before calling this

        // Split by '@' to get owner/repo and tag/asset
        let (owner_repo, tag_asset) = s
            .split_once('@')
            .ok_or_else(|| anyhow::anyhow!("missing '@' in GitHub reference: {s}"))?;

        // Parse owner/repo
        let (owner, repo) = owner_repo
            .split_once('/')
            .ok_or_else(|| anyhow::anyhow!("missing '/' between owner and repo: {owner_repo}"))?;

        if owner.is_empty() {
            bail!("owner cannot be empty in GitHub reference");
        }
        if repo.is_empty() {
            bail!("repo cannot be empty in GitHub reference");
        }

        // Parse tag/asset-name
        let (tag_str, asset_name) = tag_asset.split_once('/').ok_or_else(|| {
            anyhow::anyhow!("missing '/' between tag and asset name: {tag_asset}")
        })?;

        if tag_str.is_empty() {
            bail!("tag cannot be empty in GitHub reference");
        }
        if asset_name.is_empty() {
            bail!("asset name cannot be empty in GitHub reference");
        }

        let tag = if tag_str == "latest" {
            GitHubReleaseTag::Latest
        } else {
            GitHubReleaseTag::Specific(tag_str.to_string())
        };

        Ok(GitHubReleaseReference {
            owner: owner.to_string(),
            repo: repo.to_string(),
            tag,
            asset_name: asset_name.to_string(),
        })
    }
}

/// GitHub API response for a release
#[derive(Debug, Deserialize)]
struct GitHubRelease {
    tag_name: String,
    assets: Vec<GitHubAsset>,
}

/// GitHub API response for a release asset
#[derive(Debug, Deserialize)]
struct GitHubAsset {
    name: String,
    browser_download_url: String,
}

pub(crate) fn content_digest_to_wasm_file(
    wasm_cache_dir: &Path,
    content_digest: &ContentDigest,
) -> PathBuf {
    wasm_cache_dir.join(format!("{}.wasm", content_digest.with_infix("_")))
}

/// Pull a WASM file from a GitHub release to the cache directory.
///
/// If `expected_content_digest` is provided and the file exists in cache, returns immediately.
/// Otherwise fetches from GitHub API and downloads the asset.
#[instrument(skip_all, fields(github_ref = %github_ref) err)]
pub(crate) async fn pull_to_cache_dir(
    github_ref: &GitHubReleaseReference,
    wasm_cache_dir: &Path,
) -> Result<(ContentDigest, PathBuf), anyhow::Error> {
    // Need to fetch from GitHub
    let client = GitHubClient::new(GITHUB_CLIENT_RETRIES);

    // Get release info (resolves "latest" to actual tag)
    info!("Fetching GitHub release metadata");
    let release = client.get_release(github_ref).await?;
    debug!("Resolved tag: {}", release.tag_name);

    // Find the asset
    let asset = release
        .assets
        .iter()
        .find(|a| a.name == github_ref.asset_name)
        .ok_or_else(|| {
            let available: Vec<_> = release.assets.iter().map(|a| &a.name).collect();
            anyhow::anyhow!(
                "asset '{}' not found in release '{}'. Available assets: {:?}",
                github_ref.asset_name,
                release.tag_name,
                available
            )
        })?;

    // Download the asset to a unique temporary file first to avoid race conditions
    // when multiple processes download the same asset concurrently.
    info!("Downloading asset from GitHub release");
    let temp_file = tempfile::NamedTempFile::new_in(wasm_cache_dir)?;
    let temp_path = temp_file.path().to_path_buf();
    temp_file.keep()?;
    client
        .download_asset(&asset.browser_download_url, &temp_path)
        .await?;

    // Calculate the actual content digest
    let actual_content_digest = calculate_sha256_file(&temp_path)
        .await
        .context("failed to calculate sha256 of downloaded file")?;

    // Atomic rename to final location based on content digest.
    // Rename is atomic on the same filesystem, which is guaranteed since temp file is in wasm_cache_dir.
    let wasm_path = content_digest_to_wasm_file(wasm_cache_dir, &actual_content_digest);
    tokio::fs::rename(&temp_path, &wasm_path)
        .await
        .with_context(|| format!("failed to rename {temp_path:?} to {wasm_path:?}"))?;

    Ok((actual_content_digest, wasm_path))
}

struct GitHubClient {
    client: reqwest::Client,
    retries: u64,
}

impl GitHubClient {
    fn new(retries: u64) -> Self {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            reqwest::header::ACCEPT,
            "application/vnd.github+json".parse().expect("valid header"),
        );
        headers.insert(
            "X-GitHub-Api-Version",
            "2022-11-28".parse().expect("valid header"),
        );
        headers.insert(
            reqwest::header::USER_AGENT,
            "obelisk".parse().expect("valid header"),
        );

        // Add authorization if GITHUB_TOKEN is set
        if let Ok(token) = std::env::var("GITHUB_TOKEN") {
            let mut headers = headers;
            headers.insert(
                reqwest::header::AUTHORIZATION,
                format!("Bearer {token}").parse().expect("valid header"),
            );
            Self {
                client: reqwest::Client::builder()
                    .default_headers(headers)
                    .build()
                    .expect("failed to build HTTP client"),
                retries,
            }
        } else {
            Self {
                client: reqwest::Client::builder()
                    .default_headers(headers)
                    .build()
                    .expect("failed to build HTTP client"),
                retries,
            }
        }
    }

    async fn retry<T, F, Fut>(&self, what: F, reason: &'static str) -> Result<T, anyhow::Error>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<T, anyhow::Error>>,
    {
        let mut tries = 0;
        loop {
            match what().await {
                Ok(ok) => return Ok(ok),
                Err(err) if tries == self.retries => return Err(err),
                Err(err) => {
                    tries += 1;
                    let duration = Duration::from_secs(tries);
                    debug!("Error {reason}: {err:?}");
                    warn!("Retrying after {duration:?}");
                    tokio::time::sleep(duration).await;
                }
            }
        }
    }

    #[instrument(skip(self))]
    async fn get_release(
        &self,
        github_ref: &GitHubReleaseReference,
    ) -> Result<GitHubRelease, anyhow::Error> {
        self.retry(
            || async {
                let url = match &github_ref.tag {
                    GitHubReleaseTag::Latest => {
                        format!(
                            "https://api.github.com/repos/{}/{}/releases/latest",
                            github_ref.owner, github_ref.repo
                        )
                    }
                    GitHubReleaseTag::Specific(tag) => {
                        format!(
                            "https://api.github.com/repos/{}/{}/releases/tags/{tag}",
                            github_ref.owner, github_ref.repo
                        )
                    }
                };

                let response = self.client.get(&url).send().await.with_context(|| {
                    format!("failed to fetch release info from GitHub: {url}")
                })?;

                let status = response.status();
                if status == reqwest::StatusCode::NOT_FOUND {
                    bail!(
                        "release not found: {}/{}@{}",
                        github_ref.owner,
                        github_ref.repo,
                        github_ref.tag
                    );
                }
                if status == reqwest::StatusCode::FORBIDDEN {
                    let rate_limit_remaining = response
                        .headers()
                        .get("x-ratelimit-remaining")
                        .and_then(|v| v.to_str().ok());
                    if rate_limit_remaining == Some("0") {
                        bail!(
                            "GitHub API rate limit exceeded. Set GITHUB_TOKEN environment variable to increase the limit."
                        );
                    }
                    bail!("access forbidden to GitHub release. If this is a private repo, set GITHUB_TOKEN environment variable.");
                }
                if !status.is_success() {
                    let body = response.text().await.unwrap_or_default();
                    bail!("GitHub API error (status {status}): {body}");
                }

                response
                    .json::<GitHubRelease>()
                    .await
                    .context("failed to parse GitHub release response")
            },
            "fetching release info",
        )
        .await
    }

    #[instrument(skip(self, url))]
    async fn download_asset(&self, url: &str, dest: &Path) -> Result<(), anyhow::Error> {
        self.retry(
            || async {
                let response = self
                    .client
                    .get(url)
                    .header(reqwest::header::ACCEPT, "application/octet-stream")
                    .send()
                    .await
                    .with_context(|| format!("failed to download asset from {url}"))?;

                let status = response.status();
                if !status.is_success() {
                    bail!("failed to download asset (status {status})");
                }

                let bytes = response
                    .bytes()
                    .await
                    .context("failed to read asset bytes")?;

                let file = tokio::fs::File::create(dest).await?;
                let mut buffer = tokio::io::BufWriter::new(file);
                buffer.write_all(&bytes).await?;
                buffer.flush().await?;

                Ok(())
            },
            "downloading asset",
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_github_ref_specific_tag() {
        let input = "obeli-sk/obelisk@v0.34.1/my-component.wasm";
        let parsed: GitHubReleaseReference = input.parse().unwrap();
        assert_eq!(parsed.owner, "obeli-sk");
        assert_eq!(parsed.repo, "obelisk");
        assert_eq!(
            parsed.tag,
            GitHubReleaseTag::Specific("v0.34.1".to_string())
        );
        assert_eq!(parsed.asset_name, "my-component.wasm");
    }

    #[test]
    fn parse_github_ref_latest() {
        let input = "obeli-sk/obelisk@latest/my-component.wasm";
        let parsed: GitHubReleaseReference = input.parse().unwrap();
        assert_eq!(parsed.owner, "obeli-sk");
        assert_eq!(parsed.repo, "obelisk");
        assert_eq!(parsed.tag, GitHubReleaseTag::Latest);
        assert_eq!(parsed.asset_name, "my-component.wasm");
    }

    #[test]
    fn display_github_ref() {
        let gh_ref = GitHubReleaseReference {
            owner: "obeli-sk".to_string(),
            repo: "obelisk".to_string(),
            tag: GitHubReleaseTag::Specific("v0.34.1".to_string()),
            asset_name: "my-component.wasm".to_string(),
        };
        assert_eq!(
            gh_ref.to_string(),
            "gh://obeli-sk/obelisk@v0.34.1/my-component.wasm"
        );
    }

    #[test]
    fn display_github_ref_latest() {
        let gh_ref = GitHubReleaseReference {
            owner: "owner".to_string(),
            repo: "repo".to_string(),
            tag: GitHubReleaseTag::Latest,
            asset_name: "asset.wasm".to_string(),
        };
        assert_eq!(gh_ref.to_string(), "gh://owner/repo@latest/asset.wasm");
    }

    #[test]
    fn parse_error_missing_at() {
        let input = "obeli-sk/obelisk/v0.34.1/my-component.wasm";
        let result: Result<GitHubReleaseReference, _> = input.parse();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("missing '@'"));
    }

    #[test]
    fn parse_error_missing_owner_repo_slash() {
        let input = "obelisk@v0.34.1/my-component.wasm";
        let result: Result<GitHubReleaseReference, _> = input.parse();
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("missing '/' between owner and repo")
        );
    }

    #[test]
    fn parse_error_empty_owner() {
        let input = "/obelisk@v0.34.1/my-component.wasm";
        let result: Result<GitHubReleaseReference, _> = input.parse();
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("owner cannot be empty")
        );
    }

    #[test]
    fn parse_error_empty_repo() {
        let input = "obeli-sk/@v0.34.1/my-component.wasm";
        let result: Result<GitHubReleaseReference, _> = input.parse();
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("repo cannot be empty")
        );
    }

    #[test]
    fn parse_error_missing_asset() {
        let input = "obeli-sk/obelisk@v0.34.1";
        let result: Result<GitHubReleaseReference, _> = input.parse();
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("missing '/' between tag and asset")
        );
    }

    #[test]
    fn parse_error_empty_tag() {
        let input = "obeli-sk/obelisk@/my-component.wasm";
        let result: Result<GitHubReleaseReference, _> = input.parse();
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("tag cannot be empty")
        );
    }

    #[test]
    fn parse_error_empty_asset_name() {
        let input = "obeli-sk/obelisk@v0.34.1/";
        let result: Result<GitHubReleaseReference, _> = input.parse();
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("asset name cannot be empty")
        );
    }
}
