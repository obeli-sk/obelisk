use clap::Parser;
use std::{ops::RangeInclusive, path::PathBuf};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub(crate) struct Args {
    #[command(subcommand)]
    pub(crate) command: Subcommand,
}

#[derive(Debug, clap::Subcommand)]
pub(crate) enum Subcommand {
    #[command(subcommand)]
    Server(Server),
    #[command(subcommand)]
    Worker(Worker),
    #[command(subcommand)]
    Function(Function),
    #[command(subcommand)]
    Exe(Exe),
}

#[derive(Debug, clap::Subcommand)]
pub(crate) enum Server {
    Run {
        #[arg(short, long)]
        clean: bool,
    },
    Shutdown {
        #[arg(short, long)]
        ungraceful: bool,
    },
}

#[derive(Debug, clap::Subcommand)]
pub(crate) enum Worker {
    Add {
        #[arg(num_args(1..), required(true))]
        wasm_file: PathBuf,
        #[arg(short, long)]
        replace: bool,
    },
    List,
    Remove {
        #[arg(num_args(1..), required(true))]
        hashes: Vec<String>,
    },
}

#[derive(Debug, clap::Subcommand)]
pub(crate) enum Function {
    List,
    Validate {
        function: String,
        params: Vec<String>,
    },
    Schedule {
        when: String,
        function: String,
        params: Vec<String>,
    },
    Run {
        function: String,
        params: Vec<String>,
    },
}

#[derive(Debug, clap::Subcommand)]
pub(crate) enum Exe {
    List,
    Status { execution_id: String },
}

const PORT_RANGE: RangeInclusive<usize> = 0..=65535;

fn port_in_range(s: &str) -> Result<u16, String> {
    let port: usize = s
        .parse()
        .map_err(|_| format!("`{s}` isn't a port number"))?;
    if PORT_RANGE.contains(&port) {
        Ok(port as u16)
    } else {
        Err(format!(
            "port not in range {}-{}",
            PORT_RANGE.start(),
            PORT_RANGE.end()
        ))
    }
}
