use std::{
    collections::{BTreeMap, HashSet},
    fmt,
    net::SocketAddr,
    path::{Path, PathBuf},
};

use anyhow::{Context, anyhow, bail};
use rust_decimal::Decimal;
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Deserializer, de};
use url::Url;

const DEFAULT_UPSTREAM_BASE_URL: &str = "https://chatgpt.com/backend-api/codex";
const DEFAULT_OAUTH_TOKEN_URL: &str = "https://auth.openai.com/oauth/token";

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Config {
    pub(crate) server: ServerConfig,
    pub(crate) state: StateConfig,
    pub(crate) upstream: UpstreamConfig,
    pub(crate) api_keys: Vec<ApiKeyConfig>,
    pub(crate) model_prices: BTreeMap<String, ModelPrice>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ServerConfig {
    pub(crate) listen: SocketAddr,
    #[serde(default)]
    pub(crate) enable_websockets: bool,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StateConfig {
    pub(crate) path: PathBuf,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct UpstreamConfig {
    #[serde(
        default = "default_upstream_base_url",
        deserialize_with = "deserialize_url"
    )]
    pub(crate) base_url: Url,
    #[serde(
        default = "default_oauth_token_url",
        deserialize_with = "deserialize_url"
    )]
    pub(crate) oauth_token_url: Url,
    pub(crate) auth_file: PathBuf,
    #[serde(default)]
    pub(crate) supports_websockets: bool,
}

#[derive(Clone)]
pub(crate) struct ApiKeyConfig {
    pub(crate) id: String,
    pub(crate) secret: SecretString,
    pub(crate) weekly_limit_usd: Option<Decimal>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ApiKeyConfigInput {
    id: String,
    secret: Option<SecretString>,
    secret_file: Option<PathBuf>,
    #[serde(default, deserialize_with = "deserialize_optional_decimal")]
    weekly_limit_usd: Option<Decimal>,
}

impl<'de> Deserialize<'de> for ApiKeyConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let input = ApiKeyConfigInput::deserialize(deserializer)?;
        let secret = match (input.secret, input.secret_file) {
            (Some(secret), None) => secret,
            (None, Some(path)) => {
                let mut secret = std::fs::read_to_string(&path).map_err(|error| {
                    de::Error::custom(format!(
                        "failed to read API key secret file {}: {error}",
                        path.display()
                    ))
                })?;
                if secret.ends_with("\r\n") {
                    secret.truncate(secret.len() - 2);
                } else if secret.ends_with('\n') {
                    secret.pop();
                }
                SecretString::from(secret)
            }
            (Some(_), Some(_)) => {
                return Err(de::Error::custom(
                    "API key must set exactly one of secret or secret_file",
                ));
            }
            (None, None) => {
                return Err(de::Error::custom(
                    "API key must set exactly one of secret or secret_file",
                ));
            }
        };

        Ok(Self {
            id: input.id,
            secret,
            weekly_limit_usd: input.weekly_limit_usd,
        })
    }
}

impl fmt::Debug for ApiKeyConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ApiKeyConfig")
            .field("id", &self.id)
            .field("secret", &"[REDACTED]")
            .field("weekly_limit_usd", &self.weekly_limit_usd)
            .finish()
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ModelPrice {
    #[serde(deserialize_with = "deserialize_decimal")]
    pub(crate) input_usd_per_million: Decimal,
    #[serde(deserialize_with = "deserialize_decimal")]
    pub(crate) cached_input_usd_per_million: Decimal,
    #[serde(deserialize_with = "deserialize_decimal")]
    pub(crate) output_usd_per_million: Decimal,
}

impl Config {
    pub(crate) fn load(path: &Path) -> anyhow::Result<Self> {
        let contents = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read configuration file {}", path.display()))?;
        let config = toml::from_str::<Self>(&contents).map_err(|mut error| {
            // TOML's normal display includes the offending source line, which can
            // contain an API key. Preserve its field path and message without
            // retaining any configuration contents in the returned error.
            error.set_input(None);
            anyhow!("invalid configuration: {error}")
        })?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> anyhow::Result<()> {
        if self.state.path.as_os_str().is_empty() {
            bail!("configuration state.path must not be empty");
        }
        if self.upstream.auth_file.as_os_str().is_empty() {
            bail!("configuration upstream.auth_file must not be empty");
        }
        validate_http_url("upstream.base_url", &self.upstream.base_url)?;
        validate_http_url("upstream.oauth_token_url", &self.upstream.oauth_token_url)?;

        if self.server.enable_websockets && !self.upstream.supports_websockets {
            bail!(
                "configuration enables downstream WebSockets while upstream.supports_websockets is false"
            );
        }

        if self.api_keys.is_empty() {
            bail!("configuration api_keys must contain at least one key");
        }
        let mut ids = HashSet::with_capacity(self.api_keys.len());
        let mut secrets = HashSet::with_capacity(self.api_keys.len());
        for api_key in &self.api_keys {
            if api_key.id.is_empty() {
                bail!("configuration api_keys id must not be empty");
            }
            let secret = api_key.secret.expose_secret();
            if secret.is_empty() {
                bail!("configuration api_keys secret must not be empty");
            }
            if secret.bytes().any(|byte| byte.is_ascii_whitespace()) {
                bail!("configuration api_keys secret must not contain ASCII whitespace");
            }
            if !ids.insert(api_key.id.as_str()) {
                bail!(
                    "configuration contains duplicate API key id {:?}",
                    api_key.id
                );
            }
            if !secrets.insert(secret) {
                bail!("configuration contains a duplicate API key secret");
            }
            if api_key
                .weekly_limit_usd
                .is_some_and(|limit| limit < Decimal::ZERO)
            {
                bail!("configuration API key weekly_limit_usd must be non-negative");
            }
        }

        if self.model_prices.is_empty() {
            bail!("configuration model_prices must contain at least one model");
        }
        for (model, price) in &self.model_prices {
            if model.is_empty() {
                bail!("configuration model_prices contains an empty model name");
            }
            if price.input_usd_per_million < Decimal::ZERO {
                bail!("configuration model {model:?} input price must be non-negative");
            }
            if price.cached_input_usd_per_million < Decimal::ZERO {
                bail!("configuration model {model:?} cached input price must be non-negative");
            }
            if price.output_usd_per_million < Decimal::ZERO {
                bail!("configuration model {model:?} output price must be non-negative");
            }
        }

        Ok(())
    }
}

fn deserialize_url<'de, D>(deserializer: D) -> Result<Url, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    Url::parse(&value).map_err(de::Error::custom)
}

fn deserialize_decimal<'de, D>(deserializer: D) -> Result<Decimal, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    value.parse().map_err(de::Error::custom)
}

fn deserialize_optional_decimal<'de, D>(deserializer: D) -> Result<Option<Decimal>, D::Error>
where
    D: Deserializer<'de>,
{
    Option::<String>::deserialize(deserializer)?
        .map(|value| value.parse().map_err(de::Error::custom))
        .transpose()
}

fn default_upstream_base_url() -> Url {
    Url::parse(DEFAULT_UPSTREAM_BASE_URL).expect("hard-coded upstream URL is valid")
}

fn default_oauth_token_url() -> Url {
    Url::parse(DEFAULT_OAUTH_TOKEN_URL).expect("hard-coded OAuth URL is valid")
}

fn validate_http_url(name: &str, url: &Url) -> anyhow::Result<()> {
    if !matches!(url.scheme(), "http" | "https") {
        bail!("configuration {name} must use http or https");
    }
    Ok(())
}
