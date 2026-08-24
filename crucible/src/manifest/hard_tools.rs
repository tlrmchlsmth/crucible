use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

/// One nm-hard-tools MCP service aggregated behind Crucible's privileged broker.
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HardToolCfg {
    /// Prefix applied to every upstream tool before it is exposed by the broker.
    pub name: String,
    /// Full nm-hard-tools Streamable HTTP endpoint, including `/mcp`.
    pub url: String,
    /// Optional environment variable holding the upstream bearer token. The broker resolves it;
    /// the value never enters the agent sandbox.
    #[serde(default)]
    pub bearer_token_env: Option<String>,
}

#[derive(Debug, thiserror::Error)]
enum HardToolConfigError {
    #[error("[[agent.broker.hard_tool]] requires [agent.broker].enabled = true")]
    RequiresBroker,
    #[error("[[agent.broker.hard_tool]].name must not be empty")]
    EmptyName,
    #[error(
        "nm-hard-tools service name {name:?} may contain only ASCII letters, digits, '_' and '-'"
    )]
    InvalidName { name: String },
    #[error("duplicate nm-hard-tools service name {name:?}")]
    DuplicateName { name: String },
    #[error("invalid nm-hard-tools service URL for {name:?}: {reason}")]
    InvalidUrl { name: String, reason: String },
    #[error("nm-hard-tools service {name:?} has an empty bearer_token_env")]
    EmptyTokenEnv { name: String },
}

pub(super) fn validate(services: &[HardToolCfg], broker_enabled: bool) -> Result<()> {
    if !services.is_empty() && !broker_enabled {
        return Err(HardToolConfigError::RequiresBroker.into());
    }
    let mut names = BTreeSet::new();
    for service in services {
        if service.name.trim().is_empty() {
            return Err(HardToolConfigError::EmptyName.into());
        }
        if !service
            .name
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '-')
        {
            return Err(HardToolConfigError::InvalidName {
                name: service.name.clone(),
            }
            .into());
        }
        if !names.insert(&service.name) {
            return Err(HardToolConfigError::DuplicateName {
                name: service.name.clone(),
            }
            .into());
        }
        super::broker_endpoint_from_url(&service.url).map_err(|error| {
            HardToolConfigError::InvalidUrl {
                name: service.name.clone(),
                reason: error.to_string(),
            }
        })?;
        if service
            .bearer_token_env
            .as_ref()
            .is_some_and(|name| name.trim().is_empty())
        {
            return Err(HardToolConfigError::EmptyTokenEnv {
                name: service.name.clone(),
            }
            .into());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::manifest::Manifest;

    const BASE: &str = r#"
        [repo]
        path = "."
        [agent]
        backend = "openshell"
        goal = "benchmark it"
        [agent.broker]
        enabled = true
        bin = "crucible-broker"
        [[agent.broker.hard_tool]]
        name = "eval"
        url = "http://hard-eval.eval.svc:8080/mcp"
        bearer_token_env = "HARD_EVAL_TOKEN"
        [judge]
        measure_cmd = "./measure"
        direction = "higher"
        objective = "score"
    "#;

    #[test]
    fn parses_and_validates_a_broker_owned_hard_tools_service() {
        let manifest: Manifest = toml::from_str(BASE).unwrap();
        manifest.validate().unwrap();
        let service = &manifest.agent.broker.hard_tool[0];
        assert_eq!(service.name, "eval");
        assert_eq!(service.bearer_token_env.as_deref(), Some("HARD_EVAL_TOKEN"));
    }

    #[test]
    fn refuses_hard_tools_without_the_privileged_broker() {
        let manifest: Manifest =
            toml::from_str(&BASE.replace("enabled = true", "enabled = false")).unwrap();
        let error = manifest.validate().unwrap_err().to_string();
        assert!(error.contains("requires [agent.broker].enabled = true"));
    }
}
