use concepts::ContentDigest;
use std::path::Path;

#[tracing::instrument(skip_all)]
pub async fn calculate_sha256_file<P: AsRef<Path>>(
    path: P,
) -> Result<ContentDigest, std::io::Error> {
    use sha2::Digest;
    use tokio::io::AsyncReadExt;
    let mut file = tokio::fs::File::open(path).await?;
    let mut hasher = sha2::Sha256::default();
    let mut buf = [0; 4096];
    loop {
        let n = file.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    let digest: [u8; 32] = hasher.finalize().into();
    Ok(ContentDigest::new(concepts::HashType::Sha256, digest))
}
