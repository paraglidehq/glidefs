use clap::{Parser, Subcommand};
use std::path::PathBuf;

pub mod password;
pub mod server;

#[derive(Parser)]
#[command(name = "glidefs")]
#[command(author, version, about = "High-performance S3-backed block storage for ZFS", long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Generate a default configuration file
    Init {
        #[arg(default_value = "glidefs.toml")]
        path: PathBuf,
    },
    /// Run the NBD block device server
    Run {
        #[arg(short, long)]
        config: PathBuf,
    },
    /// Change the encryption password
    ///
    /// Reads new password from stdin. Examples:
    ///
    /// echo "newpassword" | glidefs change-password -c config.toml
    ///
    /// glidefs change-password -c config.toml < password.txt
    ChangePassword {
        #[arg(short, long)]
        config: PathBuf,
    },
}

impl Cli {
    pub fn parse_args() -> Self {
        Self::parse()
    }
}
