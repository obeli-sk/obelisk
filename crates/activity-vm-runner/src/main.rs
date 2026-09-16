use anyhow::Context as _;
use secrecy::SecretString;
use std::collections::HashMap;
use std::io::Read as _;
use std::path::PathBuf;
use wasmtime::{
    Cache, Config, Engine, InstanceAllocationStrategy, OptLevel, PoolingAllocationConfig, Strategy,
    WasmBacktraceDetails,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .map_err(|_| anyhow::anyhow!("TLS crypto provider was already installed"))?;
    let mut args = std::env::args_os().skip(1);
    let module_path = PathBuf::from(args.next().context("missing VM runtime path")?);
    let policy_path = PathBuf::from(args.next().context("missing HTTP policy path")?);
    let mapdirs = args
        .by_ref()
        .take_while(|arg| arg != "--")
        .map(|mapping| {
            let mapping = mapping.to_string_lossy();
            let (host, guest) = mapping
                .split_once("::")
                .context("VM mapping must be HOST::GUEST")?;
            Ok(obelisk_activity_vm_runner::MapDir::read_write(
                PathBuf::from(host),
                guest.to_owned(),
            ))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let mut guest_args = args
        .map(|arg| arg.to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    let spec = serde_json::from_slice(&tokio::fs::read(policy_path).await?)?;
    let mut stdin = String::new();
    std::io::stdin().read_to_string(&mut stdin)?;
    let input: RunnerInput = if stdin.trim().is_empty() {
        RunnerInput::default()
    } else {
        serde_json::from_str(&stdin)?
    };
    guest_args.extend(
        input
            .params
            .iter()
            .map(serde_json::to_string)
            .collect::<Result<Vec<_>, _>>()?,
    );
    let resolver = MapResolver(
        input
            .secrets
            .into_iter()
            .map(|(name, value)| (name, SecretString::from(value)))
            .collect(),
    );
    let (policy, placeholders) =
        wasm_workers::policy_builder::build_process_http_policy(spec, &resolver)?;
    let mut engine_config = Config::new();
    engine_config.shared_memory(true);
    engine_config.epoch_interruption(
        std::env::var_os("QEMU_WASMTIME_EPOCH").is_some_and(|value| value != "0"),
    );
    engine_config.consume_fuel(false);
    engine_config.parallel_compilation(true);
    engine_config.wasm_component_model_async(false);
    engine_config.concurrency_support(false);
    engine_config.wasm_backtrace_details(WasmBacktraceDetails::Enable);
    if std::env::var_os("QEMU_WASMTIME_CACHE").is_some() {
        engine_config.cache(Some(Cache::from_file(None)?));
    }
    if std::env::var_os("QEMU_WASMTIME_POOLING").is_some() {
        let mut pooling = PoolingAllocationConfig::default();
        pooling
            .total_core_instances(1024)
            .total_tables(32)
            .max_tables_per_module(2)
            .table_elements(65_536);
        engine_config.allocation_strategy(InstanceAllocationStrategy::Pooling(pooling));
    }
    if let Some(target) = std::env::var_os("QEMU_WASMTIME_TARGET") {
        engine_config.target(target.to_string_lossy().as_ref())?;
    }
    if let Some(strategy) = std::env::var_os("QEMU_WASMTIME_STRATEGY") {
        match strategy.to_string_lossy().as_ref() {
            "cranelift" => engine_config.strategy(Strategy::Cranelift),
            "winch" => engine_config.strategy(Strategy::Winch),
            other => anyhow::bail!("unsupported Wasmtime strategy {other}"),
        };
    }
    if let Some(level) = std::env::var_os("QEMU_WASMTIME_OPT_LEVEL") {
        engine_config.cranelift_opt_level(match level.to_string_lossy().as_ref() {
            "none" => OptLevel::None,
            "speed" => OptLevel::Speed,
            "speed-and-size" => OptLevel::SpeedAndSize,
            other => anyhow::bail!("unsupported Cranelift optimization level {other}"),
        });
    }
    if std::env::var_os("OBELISK_WASM_DEBUG").is_some() {
        // Mirrors `[wasm] debug = true` in the Obelisk worker. Keeping this
        // opt-in matters for QEMU: disabling Cranelift optimizations makes TCI
        // boot dramatically slower and substantially increases memory use.
        engine_config.debug_info(true);
        engine_config.cranelift_opt_level(OptLevel::None);
    }
    obelisk_activity_vm_runner::start_benchmark();
    let engine = Engine::new(&engine_config)?;
    let compile_started = std::time::Instant::now();
    let module = obelisk_activity_vm_runner::compile(&engine, &module_path)?;
    eprintln!(
        "QEMU outer module compile_ms={}",
        compile_started.elapsed().as_millis()
    );
    let qemu_runtime = std::env::var_os("OBELISK_QEMU_IMAGE_DIR")
        .map(PathBuf::from)
        .map(|image_dir| -> anyhow::Result<_> {
            let args: Vec<String> =
                serde_json::from_slice(&std::fs::read(image_dir.join("../args.json"))?)?;
            let guest_protocol = if args
                .iter()
                .any(|argument| argument.contains("mount_tag=store0"))
            {
                obelisk_activity_vm_runner::QemuGuestProtocol::DirectShell
            } else {
                obelisk_activity_vm_runner::QemuGuestProtocol::Pack
            };
            Ok(obelisk_activity_vm_runner::QemuRuntimeConfig {
                args,
                image_dir,
                guest_protocol,
            })
        })
        .transpose()?;
    let output = obelisk_activity_vm_runner::execute(
        &engine,
        module,
        mapdirs,
        qemu_runtime,
        guest_args,
        placeholders.into_iter().collect(),
        None,
        policy,
        std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        256 * 1024 * 1024,
        256 * 1024 * 1024,
    )
    .await?;
    use std::io::Write as _;
    std::io::stdout().write_all(&output.stdout)?;
    std::io::stderr().write_all(&output.stderr)?;
    if output.exit_code == 0 {
        Ok(())
    } else {
        std::process::exit(output.exit_code);
    }
}

#[derive(Default, serde::Deserialize)]
struct RunnerInput {
    #[serde(default)]
    secrets: HashMap<String, String>,
    #[serde(default)]
    params: Vec<serde_json::Value>,
}

#[derive(Debug)]
struct MapResolver(HashMap<String, SecretString>);

impl worker_common::SecretResolver for MapResolver {
    fn secret_lookup(&self, name: &str) -> Option<SecretString> {
        self.0.get(name).cloned()
    }
}
