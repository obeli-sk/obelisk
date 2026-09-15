use anyhow::{Context as _, bail, ensure};
use base64::Engine as _;
use ed25519_dalek::{Signature, Verifier as _, VerifyingKey};
use sha2::{Digest as _, Sha256};
use std::collections::{HashMap, HashSet, VecDeque};
use std::io::{Cursor, Read as _};
use std::path::Path;

use crate::config::deployment::NixCacheToml;

struct NarInfo {
    store_path: String,
    url: String,
    compression: String,
    file_hash: String,
    file_size: usize,
    nar_hash: String,
    nar_size: usize,
    references: Vec<String>,
    signatures: Vec<String>,
}

pub(crate) async fn resolve(
    roots: &[String],
    caches: &[NixCacheToml],
    output: &Path,
) -> anyhow::Result<Vec<String>> {
    tokio::fs::create_dir_all(output).await?;
    let client = reqwest::Client::new();
    let mut pending = roots
        .iter()
        .map(|root| store_basename(root).map(str::to_owned))
        .collect::<anyhow::Result<VecDeque<_>>>()?;
    let mut seen = HashSet::new();
    while let Some(basename) = pending.pop_front() {
        if !seen.insert(basename.clone()) {
            continue;
        }
        let digest = basename
            .split_once('-')
            .context("invalid Nix store basename")?
            .0;
        let marker = output.join(format!(".complete-{digest}"));
        let references_marker = output.join(format!(".references-{digest}"));
        let destination = output.join(&basename);
        if marker.is_file()
            && destination.exists()
            && let Ok(references) = tokio::fs::read_to_string(&references_marker).await
        {
            pending.extend(references.lines().map(str::to_owned));
            continue;
        }
        tracing::info!(store_path = %basename, "Fetching activity VM Nix path");
        let (cache, bytes) = get_from_caches(&client, caches, &format!("{digest}.narinfo")).await?;
        let info = parse_narinfo(std::str::from_utf8(&bytes)?)?;
        ensure!(
            store_basename(&info.store_path)? == basename,
            "narinfo store path mismatch"
        );
        verify_signature(&info, caches)?;
        pending.extend(info.references.iter().cloned());
        tokio::fs::write(&references_marker, info.references.join("\n")).await?;
        if marker.is_file() && destination.exists() {
            continue;
        }
        let relative_url = info.url.strip_prefix('/').unwrap_or(&info.url);
        ensure!(
            !relative_url.contains("://") && !relative_url.split('/').any(|part| part == ".."),
            "unsafe NAR URL in signed narinfo"
        );
        let compressed = get(
            &client,
            &format!("{}/{relative_url}", cache.url.trim_end_matches('/')),
        )
        .await?;
        verify_file(&info, &compressed)?;
        let nar = decompress(&info.compression, &compressed)?;
        ensure!(
            nar.len() == info.nar_size,
            "NAR size mismatch for {}",
            info.store_path
        );
        verify_sha256(&info.nar_hash, &nar).context("NAR SHA-256 mismatch")?;
        let temporary = output.join(format!(".{basename}.{}.part", std::process::id()));
        if temporary.exists() {
            tokio::fs::remove_dir_all(&temporary).await?;
        }
        let manifest = restore_nar(&nar, &temporary)?;
        if destination.exists() {
            tokio::fs::remove_dir_all(&destination).await?;
        }
        tokio::fs::rename(&temporary, &destination).await?;
        let manifest_path = output.join(".obelisk-activity-vm-manifest");
        let mut existing = tokio::fs::read_to_string(&manifest_path)
            .await
            .unwrap_or_default();
        for line in manifest.lines() {
            let (kind, rest) = line.split_once('\t').context("invalid NAR manifest")?;
            existing.push_str(kind);
            existing.push('\t');
            existing.push_str(&basename);
            existing.push('/');
            existing.push_str(rest);
            existing.push('\n');
        }
        tokio::fs::write(manifest_path, existing).await?;
        tokio::fs::write(marker, []).await?;
    }
    compact_manifest(output).await?;
    restore_store_symlinks(output).await?;
    let mut closure = seen.into_iter().collect::<Vec<_>>();
    closure.sort_unstable();
    Ok(closure)
}

async fn restore_store_symlinks(store: &Path) -> anyhow::Result<()> {
    let manifest = tokio::fs::read_to_string(store.join(".obelisk-activity-vm-manifest"))
        .await
        .unwrap_or_default();
    for line in manifest.lines() {
        let mut fields = line.splitn(3, '\t');
        if let (Some("L"), Some(relative), Some(target)) =
            (fields.next(), fields.next(), fields.next())
        {
            let path = store.join(relative);
            if !path.is_symlink() {
                if path.exists() {
                    tokio::fs::remove_file(&path).await?;
                }
                #[cfg(unix)]
                std::os::unix::fs::symlink(target, path)?;
                #[cfg(not(unix))]
                bail!("activity VM Nix stores require Unix symlink support");
            }
        }
    }
    Ok(())
}

/// Old cache entries included an `F` record for every ordinary file even though the guest only
/// needs to restore executable bits and symlinks. Filtering those records also migrates existing
/// activity VM stores without downloading their NARs again.
async fn compact_manifest(output: &Path) -> anyhow::Result<()> {
    let manifest_path = output.join(".obelisk-activity-vm-manifest");
    let manifest = tokio::fs::read_to_string(&manifest_path)
        .await
        .unwrap_or_default();
    let mut compact = String::new();
    for line in manifest.lines() {
        if line.starts_with("X\t") || line.starts_with("L\t") {
            compact.push_str(line);
            compact.push('\n');
        }
    }
    if compact != manifest {
        tracing::debug!(
            old_bytes = manifest.len(),
            new_bytes = compact.len(),
            "Compacting activity VM Nix metadata manifest"
        );
        tokio::fs::write(manifest_path, compact).await?;
    }
    Ok(())
}

async fn get_from_caches<'a>(
    client: &reqwest::Client,
    caches: &'a [NixCacheToml],
    path: &str,
) -> anyhow::Result<(&'a NixCacheToml, Vec<u8>)> {
    for cache in caches {
        if let Ok(bytes) = get(
            client,
            &format!("{}/{path}", cache.url.trim_end_matches('/')),
        )
        .await
        {
            return Ok((cache, bytes));
        }
    }
    bail!("{path} was not found in any configured Nix cache")
}

async fn get(client: &reqwest::Client, url: &str) -> anyhow::Result<Vec<u8>> {
    let response = client
        .get(url)
        .send()
        .await
        .with_context(|| format!("GET {url}"))?;
    ensure!(
        response.status().is_success(),
        "GET {url}: HTTP {}",
        response.status()
    );
    Ok(response.bytes().await?.to_vec())
}

fn parse_narinfo(text: &str) -> anyhow::Result<NarInfo> {
    let mut fields: HashMap<&str, Vec<&str>> = HashMap::new();
    for line in text.lines() {
        if let Some((key, value)) = line.split_once(": ") {
            fields.entry(key).or_default().push(value);
        }
    }
    let one = |name| {
        fields
            .get(name)
            .and_then(|values| values.first())
            .copied()
            .with_context(|| format!("narinfo missing {name}"))
    };
    Ok(NarInfo {
        store_path: one("StorePath")?.to_owned(),
        url: one("URL")?.to_owned(),
        compression: one("Compression")?.to_owned(),
        file_hash: one("FileHash")?.to_owned(),
        file_size: one("FileSize")?.parse()?,
        nar_hash: one("NarHash")?.to_owned(),
        nar_size: one("NarSize")?.parse()?,
        references: one("References")
            .unwrap_or("")
            .split_whitespace()
            .map(str::to_owned)
            .collect(),
        signatures: fields
            .get("Sig")
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(str::to_owned)
            .collect(),
    })
}

fn verify_signature(info: &NarInfo, caches: &[NixCacheToml]) -> anyhow::Result<()> {
    let references = info
        .references
        .iter()
        .map(|item| format!("/nix/store/{item}"))
        .collect::<Vec<_>>()
        .join(",");
    let fingerprint = format!(
        "1;{};{};{};{}",
        info.store_path, info.nar_hash, info.nar_size, references
    );
    for public_key in caches.iter().map(|cache| cache.public_key.as_str()) {
        let (key_name, key_data) = public_key
            .split_once(':')
            .context("invalid Nix cache public key")?;
        let key_bytes = base64::engine::general_purpose::STANDARD.decode(key_data)?;
        let key = VerifyingKey::from_bytes(key_bytes.as_slice().try_into()?)?;
        for value in &info.signatures {
            let Some((name, data)) = value.split_once(':') else {
                continue;
            };
            if name == key_name {
                let signature = Signature::from_slice(
                    &base64::engine::general_purpose::STANDARD.decode(data)?,
                )?;
                key.verify(fingerprint.as_bytes(), &signature)
                    .context("invalid narinfo signature")?;
                return Ok(());
            }
        }
    }
    bail!("narinfo is not signed by a configured Nix cache key")
}

fn verify_file(info: &NarInfo, bytes: &[u8]) -> anyhow::Result<()> {
    ensure!(
        bytes.len() == info.file_size,
        "compressed NAR size mismatch"
    );
    verify_sha256(&info.file_hash, bytes).context("compressed NAR SHA-256 mismatch")
}

fn verify_sha256(field: &str, bytes: &[u8]) -> anyhow::Result<()> {
    let text = field
        .strip_prefix("sha256:")
        .context("unsupported NAR hash")?;
    let expected = match text.len() {
        64 => (0..text.len())
            .step_by(2)
            .map(|index| u8::from_str_radix(&text[index..index + 2], 16).map_err(Into::into))
            .collect::<anyhow::Result<Vec<_>>>()?,
        52 => decode_nix_base32(text)?,
        44 => base64::engine::general_purpose::STANDARD.decode(text)?,
        _ => bail!("unsupported SHA-256 encoding"),
    };
    ensure!(
        Sha256::digest(bytes).as_slice() == expected,
        "SHA-256 digest mismatch"
    );
    Ok(())
}

fn decode_nix_base32(text: &str) -> anyhow::Result<Vec<u8>> {
    const ALPHABET: &str = "0123456789abcdfghijklmnpqrsvwxyz";
    let mut bytes = vec![0_u8; 32];
    for (position, character) in text.chars().rev().enumerate() {
        let digit = u8::try_from(ALPHABET.find(character).context("invalid Nix base32")?)?;
        let bit = position * 5;
        let (index, offset) = (bit / 8, bit % 8);
        bytes[index] |= digit << offset;
        if offset != 0 {
            let carry = digit >> (8 - offset);
            if index < 31 {
                bytes[index + 1] |= carry;
            } else {
                ensure!(carry == 0, "stray Nix base32 bits");
            }
        }
    }
    Ok(bytes)
}

fn decompress(kind: &str, bytes: &[u8]) -> anyhow::Result<Vec<u8>> {
    match kind {
        "none" => Ok(bytes.to_vec()),
        "zstd" => {
            let mut decoder = ruzstd::decoding::StreamingDecoder::new(Cursor::new(bytes))?;
            let mut output = Vec::new();
            decoder.read_to_end(&mut output)?;
            Ok(output)
        }
        "xz" => {
            let mut output = Vec::new();
            lzma_rs::xz_decompress(&mut Cursor::new(bytes), &mut output)?;
            Ok(output)
        }
        other => bail!("unsupported NAR compression `{other}`; expected zstd, xz, or none"),
    }
}

fn restore_nar(bytes: &[u8], destination: &Path) -> anyhow::Result<String> {
    let mut reader = NarReader { bytes, offset: 0 };
    let mut manifest = String::new();
    ensure!(reader.string()? == b"nix-archive-1", "invalid NAR magic");
    restore_node(&mut reader, destination, Path::new(""), &mut manifest)?;
    ensure!(reader.offset == bytes.len(), "trailing NAR data");
    Ok(manifest)
}

fn restore_node(
    reader: &mut NarReader<'_>,
    path: &Path,
    relative: &Path,
    manifest: &mut String,
) -> anyhow::Result<()> {
    reader.expect(b"(")?;
    reader.expect(b"type")?;
    match reader.string()? {
        b"directory" => {
            std::fs::create_dir_all(path)?;
            loop {
                let token = reader.string()?;
                if token == b")" {
                    break;
                }
                ensure!(token == b"entry", "expected NAR entry");
                reader.expect(b"(")?;
                reader.expect(b"name")?;
                let name = std::str::from_utf8(reader.string()?)?;
                ensure!(
                    !name.is_empty() && name != "." && name != ".." && !name.contains('/'),
                    "unsafe NAR entry name"
                );
                reader.expect(b"node")?;
                restore_node(reader, &path.join(name), &relative.join(name), manifest)?;
                reader.expect(b")")?;
            }
        }
        b"regular" => {
            let mut executable = false;
            let mut token = reader.string()?;
            if token == b"executable" {
                reader.expect(b"")?;
                executable = true;
                token = reader.string()?;
            }
            ensure!(token == b"contents", "expected NAR contents");
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::write(path, reader.string()?)?;
            if executable {
                manifest.push_str("X\t");
                manifest.push_str(relative.to_str().context("non-UTF-8 NAR path")?);
                manifest.push('\n');
            }
            #[cfg(unix)]
            if executable {
                use std::os::unix::fs::PermissionsExt as _;
                std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o555))?;
            }
            reader.expect(b")")?;
        }
        b"symlink" => {
            reader.expect(b"target")?;
            let target = std::str::from_utf8(reader.string()?)?;
            #[cfg(unix)]
            std::os::unix::fs::symlink(target, path)?;
            #[cfg(not(unix))]
            bail!("activity VM Nix stores require Unix symlink support");
            manifest.push_str("L\t");
            manifest.push_str(relative.to_str().context("non-UTF-8 NAR path")?);
            manifest.push('\t');
            manifest.push_str(target);
            manifest.push('\n');
            reader.expect(b")")?;
        }
        kind => bail!("unknown NAR node type {}", String::from_utf8_lossy(kind)),
    }
    Ok(())
}

struct NarReader<'a> {
    bytes: &'a [u8],
    offset: usize,
}
impl<'a> NarReader<'a> {
    fn string(&mut self) -> anyhow::Result<&'a [u8]> {
        let length = usize::try_from(u64::from_le_bytes(
            self.bytes
                .get(self.offset..self.offset + 8)
                .context("truncated NAR length")?
                .try_into()?,
        ))?;
        self.offset += 8;
        let value = self
            .bytes
            .get(self.offset..self.offset + length)
            .context("truncated NAR string")?;
        self.offset = self
            .offset
            .checked_add((length + 7) & !7)
            .context("NAR offset overflow")?;
        ensure!(self.offset <= self.bytes.len(), "truncated NAR padding");
        Ok(value)
    }
    fn expect(&mut self, expected: &[u8]) -> anyhow::Result<()> {
        ensure!(self.string()? == expected, "unexpected NAR token");
        Ok(())
    }
}

fn store_basename(path: &str) -> anyhow::Result<&str> {
    path.strip_prefix("/nix/store/")
        .filter(|name| !name.contains('/'))
        .context("expected exact /nix/store path")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decompresses_xz_nar() {
        let expected = b"nix-archive-1 fixture";
        let mut compressed = Vec::new();
        lzma_rs::xz_compress(&mut Cursor::new(expected), &mut compressed).unwrap();

        assert_eq!(decompress("xz", &compressed).unwrap(), expected);
    }

    #[tokio::test]
    async fn migrates_cached_symlink_placeholders() {
        let temporary = tempfile::tempdir().unwrap();
        let store = temporary.path().join("store");
        let package = "00000000000000000000000000000000-package";
        let link = store.join(package).join("tool");
        tokio::fs::create_dir_all(link.parent().unwrap())
            .await
            .unwrap();
        tokio::fs::write(&link, []).await.unwrap();
        tokio::fs::write(
            store.join(".obelisk-activity-vm-manifest"),
            format!("L\t{package}/tool\tbin/tool\n"),
        )
        .await
        .unwrap();

        restore_store_symlinks(&store).await.unwrap();

        assert_eq!(std::fs::read_link(link).unwrap(), Path::new("bin/tool"));
    }
}
