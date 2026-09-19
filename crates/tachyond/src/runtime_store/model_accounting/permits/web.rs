use super::*;
use tachyon_api::{
    agents::{Control, Reply, Request},
    web::{WebLimits, WebRequest, WebStatus},
};
use tachyon_util::config::WebPolicy;
#[cfg(test)]
mod tests;

impl ModelBroker {
    pub(crate) fn with_web(
        mut self,
        grant: tachyon_api::campaign::CampaignWeb,
        policy: WebPolicy,
    ) -> Result<Self, String> {
        policy.validate()?;
        if !(1..=4).contains(&grant.max_requests)
            || !(1..=16).contains(&grant.max_server_calls)
            || grant.inference_provider.trim().is_empty()
            || grant.inference_provider.len() > 128
            || grant.inference_provider.eq_ignore_ascii_case("openrouter")
            || grant
                .inference_provider
                .chars()
                .any(|c| c.is_control() || c.is_whitespace())
        {
            return Err("invalid campaign web allowance".into());
        }
        if policy.enabled {
            self.allowed_controls
                .extend([Control::WebSearch, Control::WebFetch]);
            self.web = Some((grant, policy));
        }
        Ok(self)
    }

    pub(super) async fn web_private(
        &self,
        permit: &ModelPermit,
        reservation: &RequestReservation,
        request: Request,
        deadline: Instant,
    ) -> Reply {
        let deadline = deadline.min(
            Instant::now()
                + std::time::Duration::from_secs(
                    self.web.as_ref().map_or(60, |(_, p)| p.timeout_secs),
                ),
        );
        let Ok(Ok(reply)) = tokio::time::timeout_at(
            deadline,
            self.web_private_inner(permit, reservation, request, deadline),
        )
        .await
        else {
            return Reply::Denied;
        };
        reply
    }

    async fn web_private_inner(
        &self,
        permit: &ModelPermit,
        reservation: &RequestReservation,
        request: Request,
        deadline: Instant,
    ) -> Result<Reply, String> {
        request.validate()?;
        let (grant, configured) = self.web.as_ref().ok_or("no campaign web allowance")?;
        if !configured.enabled || !self.allowed_controls.contains(&request.control()) {
            return Err("web control denied".into());
        }
        let search = matches!(request, Request::WebSearch { .. });
        let command = match request {
            Request::WebSearch { command } | Request::WebFetch { command } => command,
            _ => return Err("not web".into()),
        };
        let calls = match &command.request {
            WebRequest::Search { .. } => 1,
            WebRequest::Fetch { urls, .. } => urls.len() as u8,
        };
        let estimate = &reservation.estimate;
        if command.caller_id != reservation.identity.work_id
            || reservation.identity.class != RequestClass::Work
            || estimate.provider != grant.inference_provider
            || estimate.provider.eq_ignore_ascii_case("openrouter")
            || calls > configured.max_fetch_urls
            || estimate.input_tokens < configured.input_tokens
            || estimate.other_micro_usd
                < configured
                    .server_call_micro_usd
                    .checked_mul(u64::from(calls))
                    .ok_or("fee overflow")?
            || !(1..=8192).contains(&estimate.output_tokens)
        {
            return Err("web request exceeds admitted model/fee bounds".into());
        }
        let deadline =
            deadline.min(Instant::now() + std::time::Duration::from_secs(configured.timeout_secs));
        let _capacity = tokio::time::timeout_at(
            deadline,
            self.store
                .host_capacity
                .model
                .acquire(&reservation.identity.campaign_id),
        )
        .await
        .map_err(|_| "web capacity deadline")?
        .map_err(|_| "web capacity unavailable")?;
        let (tokens, cost) = estimate.upper_bound().map_err(|_| "invalid web estimate")?;
        let mut policy = configured.clone();
        policy.max_requests = grant.max_requests.min(configured.max_requests);
        policy.max_server_calls = grant.max_server_calls.min(configured.max_server_calls);
        policy.input_tokens = estimate.input_tokens;
        policy.output_tokens = estimate.output_tokens;
        policy.input_micro_usd_per_million = None;
        policy.output_micro_usd_per_million = None;
        policy.max_request_cost_micro_usd = cost;
        policy.turn_tokens = tokens
            .checked_mul(u64::from(policy.max_requests))
            .ok_or("token overflow")?;
        policy.turn_cost_micro_usd = cost
            .checked_mul(u64::from(policy.max_requests))
            .ok_or("cost overflow")?;
        let scope = serde_json::to_string(&("campaign", &reservation.identity.campaign_id))
            .map_err(|_| "web scope")?;
        let (store, nonce, binding, cmd, key) = (
            self.store.clone(),
            permit.0,
            reservation.clone(),
            command.clone(),
            scope.clone(),
        );
        let replay = tokio::task::spawn_blocking(move || -> Result<_, String> {
            let state = store
                .model_permits
                .lock()
                .map_err(|_| "permit authority unavailable")?;
            let grant = state.grants.get(&nonce).ok_or("unknown permit")?;
            if !grant.active
                || grant.paused
                || grant.closed.load(std::sync::atomic::Ordering::Acquire)
                || grant.request != binding
                || state.current.get(&binding.identity.work_id) != Some(&nonce)
                || Instant::now() >= deadline
            {
                return Err("revoked web authority".into());
            }
            let tx = store.database.begin_write().map_err(|e| e.to_string())?;
            RuntimeStore::admitted_funding_in(&tx, &grant.funding)?;
            drop(tx);
            store.web_reserve(&key, &binding.estimate.model, &policy, &cmd, calls)
        })
        .await
        .map_err(|_| "web authority unavailable")??;
        let result = if let Some(result) = replay {
            result?
        } else {
            let accounting = PermittedAccounting {
                store: self.store.clone(),
                permit: permit.0,
                request_id: command.request_id.clone(),
                deadline,
            };
            let context = AccountingContext {
                accountant: &accounting,
                request: reservation.clone(),
            };
            let limits = WebLimits {
                max_tool_calls: calls,
                max_output_tokens: estimate.output_tokens,
                ..Default::default()
            };
            let (usage, result) = match tokio::time::timeout_at(
                deadline,
                self.model
                    .web_lookup_accounted(&command.request, &limits, deadline, &context),
            )
            .await
            {
                Ok(Ok(mut completion)) => {
                    if completion.usage == RequestUsage::Unknown
                        && completion.result.status == WebStatus::Grounded
                    {
                        completion.result.status = WebStatus::Unverified;
                    }
                    (completion.usage, Ok(completion.result))
                }
                _ => (
                    RequestUsage::Unknown,
                    Err("web retrieval denied or failed; spend may be unknown".into()),
                ),
            };
            let (store, key, cmd, saved) =
                (self.store.clone(), scope, command.clone(), result.clone());
            tokio::task::spawn_blocking(move || store.web_finish(&key, &cmd, usage, saved))
                .await
                .map_err(|_| "web storage unavailable")???
        };
        if result
            .observed_search_uses
            .unwrap_or(0)
            .saturating_add(result.observed_fetch_uses.unwrap_or(0))
            > u64::from(calls)
        {
            return Err("web server use bound exceeded".into());
        }
        Ok(if search {
            Reply::WebSearch { command, result }
        } else {
            Reply::WebFetch { command, result }
        })
    }
}
