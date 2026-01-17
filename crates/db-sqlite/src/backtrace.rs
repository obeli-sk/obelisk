use concepts::storage::WasmBacktrace;
use sha2::{Digest, Sha256};

pub(crate) fn hash(backtrace: &WasmBacktrace) -> [u8; 33] {
    const BACKTRACE_HASH_VERSION: u8 = 1;
    let mut hasher = Sha256::default();

    for frame in &backtrace.frames {
        // Frame separator
        hasher.update(b"F|");

        hasher.update(frame.module.as_bytes());
        hasher.update(b"|");
        hasher.update(frame.func_name.as_bytes());
        hasher.update(b"|");

        for sym in &frame.symbols {
            hasher.update(b"S|");

            write_opt(&mut hasher, sym.func_name.as_deref());
            write_opt(&mut hasher, sym.file.as_deref());

            write_opt_u32(&mut hasher, sym.line);
            write_opt_u32(&mut hasher, sym.col);
        }
    }

    let hash_bytes = hasher.finalize();

    let mut result = [0u8; 33];
    result[0] = BACKTRACE_HASH_VERSION;
    result[1..].copy_from_slice(&hash_bytes);

    result
}

fn write_opt(hasher: &mut Sha256, v: Option<&str>) {
    match v {
        Some(s) => {
            hasher.update(b"1|");
            hasher.update(s.as_bytes());
            hasher.update(b"|");
        }
        None => {
            hasher.update(b"0|");
        }
    }
}

fn write_opt_u32(hasher: &mut Sha256, v: Option<u32>) {
    match v {
        Some(n) => {
            hasher.update(b"1|");
            hasher.update(n.to_le_bytes());
            hasher.update(b"|");
        }
        None => {
            hasher.update(b"0|");
        }
    }
}
