//! Host policy, not provider prices or a provider-enforced spending guarantee.
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct WebPolicy {
    pub enabled: bool,
    /// Used only if no Conversation/shared model was configured.
    pub model: Option<String>,
    pub concurrency: usize,
    pub max_requests: u8,
    pub max_server_calls: u8,
    pub max_fetch_urls: u8,
    pub input_tokens: u64,
    pub output_tokens: u32,
    pub turn_tokens: u64,
    pub turn_cost_micro_usd: u64,
    /// Provisional local escrow when no host token-price bounds are configured.
    pub max_request_cost_micro_usd: u64,
    pub input_micro_usd_per_million: Option<u64>,
    pub output_micro_usd_per_million: Option<u64>,
    /// Host-attested ceiling per server use, not a claim about advertised prices.
    pub server_call_micro_usd: u64,
    pub timeout_secs: u64,
}

impl Default for WebPolicy {
    fn default() -> Self {
        Self {
            enabled: true,
            model: None,
            concurrency: 4,
            max_requests: 4,
            max_server_calls: 4,
            max_fetch_urls: 4,
            input_tokens: 65536,
            output_tokens: 2048,
            turn_tokens: 270336,
            turn_cost_micro_usd: 1_000_000,
            max_request_cost_micro_usd: 250_000,
            input_micro_usd_per_million: None,
            output_micro_usd_per_million: None,
            server_call_micro_usd: 10_000,
            timeout_secs: 60,
        }
    }
}

impl WebPolicy {
    pub fn validate(&self) -> Result<(), &'static str> {
        if !(1..=64).contains(&self.concurrency)
            || !(1..=4).contains(&self.max_requests)
            || !(1..=16).contains(&self.max_server_calls)
            || !(1..=4).contains(&self.max_fetch_urls)
            || !(65536..=1_048_576).contains(&self.input_tokens)
            || !(1..=8192).contains(&self.output_tokens)
            || self.turn_tokens == 0
            || self.turn_cost_micro_usd == 0
            || self.max_request_cost_micro_usd == 0
            || self.server_call_micro_usd == 0
            || !(1..=120).contains(&self.timeout_secs)
            || self.input_micro_usd_per_million.is_some()
                != self.output_micro_usd_per_million.is_some()
        {
            return Err("invalid finite host web policy");
        }
        Ok(())
    }

    pub fn reservation(&self, calls: u8) -> Result<(u64, u64), &'static str> {
        self.validate()?;
        let tokens = self
            .input_tokens
            .checked_add(u64::from(self.output_tokens))
            .ok_or("web token overflow")?;
        let fees = u128::from(self.server_call_micro_usd) * u128::from(calls);
        let cost = match (
            self.input_micro_usd_per_million,
            self.output_micro_usd_per_million,
        ) {
            (Some(input), Some(output)) => {
                (u128::from(self.input_tokens) * u128::from(input)
                    + u128::from(self.output_tokens) * u128::from(output))
                .div_ceil(1_000_000)
                    + fees
            }
            _ => u128::from(self.max_request_cost_micro_usd).max(fees),
        };
        Ok((
            tokens,
            u64::try_from(cost).map_err(|_| "web cost overflow")?,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn defaults_are_enabled_bounded_and_not_provider_prices() {
        let defaults: super::super::Config = toml::from_str("").unwrap();
        assert!(defaults.web.enabled);
        assert_eq!(defaults.web.reservation(1).unwrap(), (67584, 250000));
        assert_eq!(defaults.web.reservation(4).unwrap(), (67584, 250000));
        let disabled: super::super::Config = toml::from_str("[web]\nenabled = false\n").unwrap();
        assert!(!disabled.web.enabled);
        let priced = WebPolicy {
            input_micro_usd_per_million: Some(1),
            output_micro_usd_per_million: Some(1),
            ..Default::default()
        };
        assert_eq!(priced.reservation(4).unwrap().1, 40001);
        assert!(WebPolicy {
            input_micro_usd_per_million: Some(1),
            ..Default::default()
        }
        .validate()
        .is_err());
    }
}
