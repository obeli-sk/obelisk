use super::{CloudArgs, DEFAULT_API_URL, app_name};
use crate::args::{AppPull, Cloud, CloudApp, CloudAppTarget, CloudDb};
use crate::config::server::SQLITE_FILE_NAME;
use anyhow::{Context as _, bail, ensure};
use reqwest::Url;
use secrecy::{ExposeSecret as _, SecretString};
use std::path::PathBuf;
use std::process::Stdio;

pub(super) struct DbRestore {
    app_name: String,
    directory: PathBuf,
    explicit_directory: bool,
    replica_url: String,
    access_key_id: String,
    secret_access_key: SecretString,
    force: bool,
    verbose: bool,
}

/// Older releases cannot decode the page headers the cloud's Litestream writes.
const MIN_LITESTREAM: (u64, u64, u64) = (0, 5, 18);

/// Fails unless `litestream` is on PATH and at least `MIN_LITESTREAM`; an unparsable version,
/// such as a development build, is let through.
async fn check_litestream() -> anyhow::Result<()> {
    let (major, minor, patch) = MIN_LITESTREAM;
    let output = match tokio::process::Command::new("litestream")
        .arg("version")
        .stdin(Stdio::null())
        .output()
        .await
    {
        Ok(output) => output,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            bail!(
                "`litestream` was not found on PATH; install Litestream {major}.{minor}.{patch} or newer to restore the database"
            )
        }
        Err(err) => return Err(err).context("cannot run `litestream version`"),
    };
    let found = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    if let Some(version) = parse_version(&found) {
        ensure!(
            version >= MIN_LITESTREAM,
            "Litestream {major}.{minor}.{patch} or newer is required to restore the database, found {found}"
        );
    }
    Ok(())
}

fn parse_version(text: &str) -> Option<(u64, u64, u64)> {
    let mut parts = text.strip_prefix('v').unwrap_or(text).splitn(3, '.');
    let mut next = |last: bool| {
        let part = parts.next()?;
        let digits = if last {
            &part[..part
                .find(|c: char| !c.is_ascii_digit())
                .unwrap_or(part.len())]
        } else {
            part
        };
        digits.parse().ok()
    };
    Some((next(false)?, next(false)?, next(true)?))
}

impl CloudArgs {
    pub(super) async fn db_restore(&self) -> anyhow::Result<Option<DbRestore>> {
        let (directory, force) = match &self.command {
            Cloud::App(CloudAppTarget {
                command:
                    CloudApp::Pull {
                        db: Some(directory),
                        pull: AppPull { force, .. },
                        ..
                    },
                ..
            }) => (directory, force),
            Cloud::App(CloudAppTarget {
                command:
                    CloudApp::Db {
                        command: CloudDb::Pull { output, force },
                        ..
                    },
                ..
            }) => (output, force),
            _ => return Ok(None),
        };
        let app_name = app_name(self.selected_app()).await?;
        check_litestream().await?;
        let token = self
            .api_token
            .as_ref()
            .context("An API token is required to read the database replica")?;
        // The replica accepts an `obk_<group>_<secret>` token as the S3 secret, its group as the key ID.
        let Some((group, _)) = token
            .expose_secret()
            .strip_prefix("obk_")
            .and_then(|rest| rest.split_once('_'))
        else {
            bail!(
                "The API token is not an `obk_` token, which reading the database replica requires"
            );
        };
        let api_url = self.api_url.as_deref().unwrap_or(DEFAULT_API_URL);
        let endpoint = Url::parse(api_url)
            .with_context(|| format!("Invalid cloud API URL `{api_url}`"))?
            .origin()
            .ascii_serialization();
        let (directory, explicit_directory) = match directory {
            Some(directory) => (directory.clone(), true),
            None => (
                crate::project_dirs()
                    .context("Cannot determine the data directory; pass the database directory explicitly")?
                    .data_dir()
                    .join("apps")
                    .join(&app_name)
                    .join("sqlite"),
                false,
            ),
        };
        Ok(Some(DbRestore {
            replica_url: format!("s3://{app_name}/sqlite?endpoint={endpoint}&forcePathStyle=true"),
            app_name,
            directory,
            explicit_directory,
            access_key_id: group.to_owned(),
            secret_access_key: token.clone(),
            force: *force,
            verbose: self.verbose,
        }))
    }
}

impl DbRestore {
    pub(super) async fn run(self) -> anyhow::Result<()> {
        tokio::fs::create_dir_all(&self.directory)
            .await
            .with_context(|| format!("cannot create database directory {:?}", self.directory))?;
        let directory = std::path::absolute(&self.directory)?;
        let db_path = directory.join(SQLITE_FILE_NAME);
        let mut command = tokio::process::Command::new("litestream");
        command
            .arg("restore")
            .arg("-o")
            .arg(&db_path)
            .args(self.force.then_some("-force"))
            .arg(&self.replica_url)
            .env("LITESTREAM_ACCESS_KEY_ID", &self.access_key_id)
            .env(
                "LITESTREAM_SECRET_ACCESS_KEY",
                self.secret_access_key.expose_secret(),
            )
            .stdin(Stdio::null());
        if self.verbose {
            eprintln!(
                "Running LITESTREAM_ACCESS_KEY_ID={} LITESTREAM_SECRET_ACCESS_KEY=<API token> litestream restore -o {}{} '{}'",
                self.access_key_id,
                db_path.display(),
                if self.force { " -force" } else { "" },
                self.replica_url
            );
        }
        let status = command
            .status()
            .await
            .context("cannot run `litestream restore`")?;
        ensure!(
            status.success(),
            "`litestream restore` failed ({status}){}",
            if self.force || !tokio::fs::try_exists(&db_path).await.unwrap_or_default() {
                ""
            } else {
                "; pass --force to overwrite the existing database"
            }
        );
        eprintln!(
            "Restored the database of app {} to {}",
            self.app_name,
            db_path.display()
        );
        if self.explicit_directory {
            eprintln!(
                "Run the server on it with OBELISK__DATABASE__SQLITE__DIRECTORY={}",
                directory.display()
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::parse_version;

    #[test]
    fn litestream_versions() {
        assert_eq!(parse_version("v0.5.18"), Some((0, 5, 18)));
        assert_eq!(parse_version("0.5.11"), Some((0, 5, 11)));
        assert_eq!(parse_version("0.5.18-rc1"), Some((0, 5, 18)));
        assert_eq!(parse_version("(development build)"), None);
    }
}
