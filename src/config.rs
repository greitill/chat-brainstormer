use std::env;

#[derive(Clone, Debug)]
pub struct Config {
    pub publish_key: String,
    pub subscribe_key: String,
    pub secret_key: Option<String>,
    pub host: String,
    pub port: u16,
    pub openai_api_key: Option<String>,
    pub openai_model: String,
}

impl Config {
    pub fn from_env() -> Self {
        dotenvy::dotenv().ok();
        Self {
            publish_key: required("PUBNUB_PUB_KEY"),
            subscribe_key: required("PUBNUB_SUB_KEY"),
            secret_key: optional("PUBNUB_SECRET_KEY"),
            host: env::var("HOST").unwrap_or_else(|_| "127.0.0.1".into()),
            port: env::var("PORT")
                .ok()
                .and_then(|p| p.parse().ok())
                .unwrap_or(8080),
            openai_api_key: optional("OPENAI_API_KEY"),
            openai_model: env::var("OPENAI_MODEL").unwrap_or_else(|_| "gpt-4o-mini".into()),
        }
    }
}

fn required(key: &str) -> String {
    env::var(key)
        .unwrap_or_else(|_| panic!("{key} required"))
        .trim()
        .trim_matches('"')
        .to_string()
}

fn optional(key: &str) -> Option<String> {
    env::var(key)
        .ok()
        .map(|v| v.trim().trim_matches('"').to_string())
        .filter(|v| !v.is_empty())
}
