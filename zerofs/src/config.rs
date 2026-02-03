use anyhow::{Context, Result};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use std::collections::HashSet;
use std::fmt;
use std::fs;
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;

/// Compression algorithm configuration for block data.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum CompressionConfig {
    #[default]
    Lz4,
    Zstd(i32),
}

impl Serialize for CompressionConfig {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            CompressionConfig::Lz4 => serializer.serialize_str("lz4"),
            CompressionConfig::Zstd(level) => serializer.serialize_str(&format!("zstd-{}", level)),
        }
    }
}

impl<'de> Deserialize<'de> for CompressionConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct CompressionConfigVisitor;

        impl de::Visitor<'_> for CompressionConfigVisitor {
            type Value = CompressionConfig;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("'lz4' or 'zstd-{level}' where level is 1-22")
            }

            fn visit_str<E>(self, value: &str) -> Result<CompressionConfig, E>
            where
                E: de::Error,
            {
                if value == "lz4" {
                    return Ok(CompressionConfig::Lz4);
                }

                if let Some(level_str) = value.strip_prefix("zstd-") {
                    let level: i32 = level_str.parse().map_err(|_| {
                        de::Error::invalid_value(
                            de::Unexpected::Str(value),
                            &"'zstd-{level}' where level is a number 1-22",
                        )
                    })?;

                    if !(1..=22).contains(&level) {
                        return Err(de::Error::invalid_value(
                            de::Unexpected::Signed(level as i64),
                            &"zstd level must be between 1 and 22",
                        ));
                    }

                    return Ok(CompressionConfig::Zstd(level));
                }

                Err(de::Error::invalid_value(
                    de::Unexpected::Str(value),
                    &"'lz4' or 'zstd-{level}' where level is 1-22",
                ))
            }
        }

        deserializer.deserialize_str(CompressionConfigVisitor)
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct Settings {
    pub cache: CacheConfig,
    pub storage: StorageConfig,
    pub servers: ServerConfig,
    /// Compression setting (lz4 or zstd-{level})
    #[serde(default)]
    pub compression: CompressionConfig,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aws: Option<AwsConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub azure: Option<AzureConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gcp: Option<GcsConfig>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct CacheConfig {
    #[serde(deserialize_with = "deserialize_expandable_path")]
    pub dir: PathBuf,
    pub disk_size_gb: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memory_size_gb: Option<f64>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct StorageConfig {
    #[serde(deserialize_with = "deserialize_expandable_string")]
    pub url: String,
    #[serde(deserialize_with = "deserialize_expandable_string")]
    pub encryption_password: String,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nbd: Option<NbdConfig>,
}

/// NBD server configuration for block device access.
#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct NbdConfig {
    /// TCP addresses to listen on (e.g., "127.0.0.1:10809")
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub addresses: Option<HashSet<SocketAddr>>,

    /// Unix socket path for local connections
    #[serde(
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_expandable_path",
        default
    )]
    pub unix_socket: Option<PathBuf>,

    /// HTTP API address for dynamic export management (e.g., "127.0.0.1:8080")
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub api_address: Option<SocketAddr>,

    /// Block size in bytes (default: 128KB to match ZFS recordsize)
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub block_size: Option<usize>,

    /// Static exports loaded at startup
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub exports: Vec<ExportConfig>,

    // Legacy single-device fields (for backward compatibility)
    /// Name of the NBD device (DEPRECATED: use exports array instead)
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub device_name: Option<String>,

    /// Size of the block device in gigabytes (DEPRECATED: use exports array instead)
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub device_size_gb: Option<f64>,
}

/// Configuration for a single NBD export (virtual block device).
#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct ExportConfig {
    /// Export name (used by NBD client: nbd-client -N <name>)
    pub name: String,

    /// Device size in gigabytes
    pub size_gb: f64,

    /// S3 prefix for this export's blocks (default: derived from name)
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub s3_prefix: Option<String>,
}

impl ExportConfig {
    /// Get the S3 prefix for this export, defaulting to the export name.
    pub fn s3_prefix(&self) -> &str {
        self.s3_prefix.as_deref().unwrap_or(&self.name)
    }

    /// Get the device size in bytes.
    pub fn size_bytes(&self) -> u64 {
        (self.size_gb * 1_000_000_000.0) as u64
    }
}

impl NbdConfig {
    pub const DEFAULT_BLOCK_SIZE: usize = 128 * 1024;
    pub const DEFAULT_DEVICE_SIZE_GB: f64 = 100.0;
    pub const DEFAULT_DEVICE_NAME: &'static str = "zerofs";

    pub fn block_size(&self) -> usize {
        self.block_size.unwrap_or(Self::DEFAULT_BLOCK_SIZE)
    }

    /// Get the list of exports, handling legacy single-device config.
    pub fn get_exports(&self) -> Vec<ExportConfig> {
        if !self.exports.is_empty() {
            return self.exports.clone();
        }

        // Legacy support: convert old device_name/device_size_gb to export
        if let (Some(name), Some(size_gb)) = (&self.device_name, self.device_size_gb) {
            return vec![ExportConfig {
                name: name.clone(),
                size_gb,
                s3_prefix: None,
            }];
        }

        // Default: single export with default values
        vec![ExportConfig {
            name: Self::DEFAULT_DEVICE_NAME.to_string(),
            size_gb: Self::DEFAULT_DEVICE_SIZE_GB,
            s3_prefix: None,
        }]
    }
}

fn default_nbd_addresses() -> HashSet<SocketAddr> {
    let mut set = HashSet::new();
    set.insert(SocketAddr::new(
        IpAddr::V4(std::net::Ipv4Addr::new(0, 0, 0, 0)),
        10809,
    ));
    set
}

fn default_api_address() -> SocketAddr {
    SocketAddr::new(
        IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)),
        8080,
    )
}

#[derive(Debug, Serialize, Clone)]
pub struct AwsConfig(pub std::collections::HashMap<String, String>);

impl<'de> Deserialize<'de> for AwsConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(AwsConfig(deserialize_expandable_hashmap(deserializer)?))
    }
}

#[derive(Debug, Serialize, Clone)]
pub struct AzureConfig(pub std::collections::HashMap<String, String>);

impl<'de> Deserialize<'de> for AzureConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(AzureConfig(deserialize_expandable_hashmap(deserializer)?))
    }
}

#[derive(Debug, Serialize, Clone)]
pub struct GcsConfig(pub std::collections::HashMap<String, String>);

impl<'de> Deserialize<'de> for GcsConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(GcsConfig(deserialize_expandable_hashmap(deserializer)?))
    }
}

fn deserialize_expandable_string<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    match shellexpand::env(&s) {
        Ok(expanded) => Ok(expanded.into_owned()),
        Err(e) => Err(serde::de::Error::custom(format!(
            "Failed to expand environment variable: {}",
            e
        ))),
    }
}

fn deserialize_expandable_path<'de, D>(deserializer: D) -> Result<PathBuf, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    match shellexpand::env(&s) {
        Ok(expanded) => Ok(PathBuf::from(expanded.into_owned())),
        Err(e) => Err(serde::de::Error::custom(format!(
            "Failed to expand environment variable: {}",
            e
        ))),
    }
}

fn deserialize_optional_expandable_path<'de, D>(
    deserializer: D,
) -> Result<Option<PathBuf>, D::Error>
where
    D: Deserializer<'de>,
{
    let opt = Option::<String>::deserialize(deserializer)?;
    opt.map(|s| match shellexpand::env(&s) {
        Ok(expanded) => Ok(PathBuf::from(expanded.into_owned())),
        Err(e) => Err(serde::de::Error::custom(format!(
            "Failed to expand environment variable: {}",
            e
        ))),
    })
    .transpose()
}

fn deserialize_expandable_hashmap<'de, D>(
    deserializer: D,
) -> Result<std::collections::HashMap<String, String>, D::Error>
where
    D: Deserializer<'de>,
{
    let map = std::collections::HashMap::<String, String>::deserialize(deserializer)?;
    map.into_iter()
        .map(|(k, v)| match shellexpand::env(&v) {
            Ok(expanded) => Ok((k, expanded.into_owned())),
            Err(e) => Err(serde::de::Error::custom(format!(
                "Failed to expand environment variable: {}",
                e
            ))),
        })
        .collect()
}

impl Settings {
    pub fn compression(&self) -> CompressionConfig {
        self.compression
    }

    pub fn from_file(config_path: impl AsRef<std::path::Path>) -> Result<Self> {
        let path = config_path.as_ref();
        let content = fs::read_to_string(path)
            .with_context(|| format!("Failed to read config file: {}", path.display()))?;

        let settings: Settings = toml::from_str(&content)
            .with_context(|| format!("Failed to parse config file: {}", path.display()))?;

        Ok(settings)
    }

    pub fn cloud_provider_env_vars(&self) -> Vec<(String, String)> {
        let mut env_vars = Vec::new();
        if let Some(aws) = &self.aws {
            for (k, v) in &aws.0 {
                env_vars.push((format!("aws_{}", k.to_lowercase()), v.clone()));
            }
        }
        if let Some(azure) = &self.azure {
            for (k, v) in &azure.0 {
                env_vars.push((format!("azure_{}", k.to_lowercase()), v.clone()));
            }
        }
        if let Some(gcp) = &self.gcp {
            for (k, v) in &gcp.0 {
                env_vars.push((format!("google_{}", k.to_lowercase()), v.clone()));
            }
        }
        env_vars
    }

    pub fn generate_default() -> Self {
        let mut aws_config = std::collections::HashMap::new();
        aws_config.insert(
            "access_key_id".to_string(),
            "${AWS_ACCESS_KEY_ID}".to_string(),
        );
        aws_config.insert(
            "secret_access_key".to_string(),
            "${AWS_SECRET_ACCESS_KEY}".to_string(),
        );

        Settings {
            cache: CacheConfig {
                dir: PathBuf::from("${HOME}/.cache/zerofs"),
                disk_size_gb: 10.0,
                memory_size_gb: Some(1.0),
            },
            storage: StorageConfig {
                url: "s3://your-bucket/zerofs-data".to_string(),
                encryption_password: "${ZEROFS_PASSWORD}".to_string(),
            },
            servers: ServerConfig {
                nbd: Some(NbdConfig {
                    addresses: Some(default_nbd_addresses()),
                    unix_socket: Some(PathBuf::from("/tmp/zerofs.nbd.sock")),
                    api_address: Some(default_api_address()),
                    block_size: None,
                    exports: vec![ExportConfig {
                        name: "default".to_string(),
                        size_gb: 100.0,
                        s3_prefix: None,
                    }],
                    device_name: None,
                    device_size_gb: None,
                }),
            },
            compression: CompressionConfig::default(),
            aws: Some(AwsConfig(aws_config)),
            azure: None,
            gcp: None,
        }
    }

    pub fn write_default_config(path: impl AsRef<std::path::Path>) -> Result<()> {
        let default = Self::generate_default();
        let toml_string = toml::to_string_pretty(&default)?;

        let commented = format!(
            "# ZeroFS Configuration File\n\
             # Generated by ZeroFS v{}\n\
             #\n\
             # High-performance S3-backed block storage for ZFS\n\
             #\n\
             # Environment variables are supported: ${{VAR}} or $VAR\n\
             #\n\
             # NBD Server:\n\
             #   - FLUSH returns after local SSD fsync (<10ms)\n\
             #   - Background sync to S3 (continuous drain)\n\
             #   - Send SIGUSR1 to drain all dirty blocks (for VM migration)\n\
             #\n\
             \n{}",
            env!("CARGO_PKG_VERSION"),
            toml_string
        );

        fs::write(path, commented)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use tempfile::NamedTempFile;

    #[test]
    fn test_env_var_expansion() {
        unsafe {
            env::set_var("ZEROFS_TEST_PASSWORD", "secret123");
            env::set_var("ZEROFS_TEST_BUCKET", "my-bucket");
        }

        let config_content = r#"
[cache]
dir = "/tmp/cache"
disk_size_gb = 1.0

[storage]
url = "s3://${ZEROFS_TEST_BUCKET}/data"
encryption_password = "${ZEROFS_TEST_PASSWORD}"

[servers]
"#;

        let temp_file = NamedTempFile::new().unwrap();
        std::fs::write(temp_file.path(), config_content).unwrap();

        let settings = Settings::from_file(temp_file.path().to_str().unwrap()).unwrap();
        assert_eq!(settings.storage.url, "s3://my-bucket/data");
        assert_eq!(settings.storage.encryption_password, "secret123");
    }

    #[test]
    fn test_home_env_var() {
        let home_dir = env::home_dir().expect("HOME not set");
        unsafe {
            env::set_var("ZEROFS_TEST_HOME", home_dir.to_str().unwrap());
        }

        let config_content = r#"
[cache]
dir = "${ZEROFS_TEST_HOME}/test-cache"
disk_size_gb = 1.0

[storage]
url = "file://${ZEROFS_TEST_HOME}/data"
encryption_password = "test"

[servers]

[servers.nbd]
unix_socket = "${ZEROFS_TEST_HOME}/zerofs.sock"
"#;

        let temp_file = NamedTempFile::new().unwrap();
        std::fs::write(temp_file.path(), config_content).unwrap();

        let settings = Settings::from_file(temp_file.path().to_str().unwrap()).unwrap();

        assert_eq!(settings.cache.dir, home_dir.join("test-cache"));
        assert_eq!(
            settings.storage.url,
            format!("file://{}/data", home_dir.display())
        );
        if let Some(nbd) = settings.servers.nbd {
            assert_eq!(nbd.unix_socket.unwrap(), home_dir.join("zerofs.sock"));
        } else {
            panic!("Expected NBD config");
        }
    }

    #[test]
    fn test_undefined_env_var_error() {
        let config_content = r#"
[cache]
dir = "/tmp/cache"
disk_size_gb = 1.0

[storage]
url = "s3://bucket/data"
encryption_password = "${ZEROFS_TEST_UNDEFINED_VAR_THAT_SHOULD_NOT_EXIST}"

[servers]
"#;

        let temp_file = NamedTempFile::new().unwrap();
        std::fs::write(temp_file.path(), config_content).unwrap();

        let result = Settings::from_file(temp_file.path().to_str().unwrap());
        assert!(result.is_err());
    }

    #[test]
    fn test_aws_config_expansion() {
        unsafe {
            env::set_var("ZEROFS_TEST_AWS_KEY", "aws123");
            env::set_var("ZEROFS_TEST_AWS_SECRET", "aws_secret");
        }

        let config_content = r#"
[cache]
dir = "/tmp/cache"
disk_size_gb = 1.0

[storage]
url = "s3://bucket/data"
encryption_password = "test"

[servers]

[aws]
access_key_id = "${ZEROFS_TEST_AWS_KEY}"
secret_access_key = "${ZEROFS_TEST_AWS_SECRET}"
"#;

        let temp_file = NamedTempFile::new().unwrap();
        std::fs::write(temp_file.path(), config_content).unwrap();

        let settings = Settings::from_file(temp_file.path().to_str().unwrap()).unwrap();

        let aws = settings.aws.unwrap();
        assert_eq!(aws.0.get("access_key_id").unwrap(), "aws123");
        assert_eq!(aws.0.get("secret_access_key").unwrap(), "aws_secret");
    }
}
