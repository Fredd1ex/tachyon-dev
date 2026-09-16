//! Same-user operator endpoints and exact-work durable questions. No actor wait.
use super::*;
use tachyon_api::{
    types::{ApiRequest, ApiResponse},
    work::Attention,
};

impl RuntimeStore {
    pub(crate) fn context_group_handles_in(
        tx: &WriteTransaction,
        campaign: &str,
        handles: &[String],
    ) -> Result<Vec<tachyon_api::context::GroupContextRef>, String> {
        Ok(load(tx, campaign)?
            .map(|root| {
                root.groups
                    .into_iter()
                    .filter(|(_, group)| {
                        !group.spec.work.is_empty()
                            && group.spec.work.iter().all(|w| handles.contains(&w.work_id))
                    })
                    .map(|(id, group)| tachyon_api::context::GroupContextRef {
                        group_id: id,
                        revision: group.revision,
                    })
                    .collect()
            })
            .unwrap_or_default())
    }

    pub(crate) fn work_attention(
        &self,
        campaign: &str,
        work: &str,
    ) -> Result<Vec<Attention>, String> {
        let tx = self.database.begin_write().map_err(err)?;
        let Some(root) = load(&tx, campaign)? else {
            if Self::admitted_work_in(&tx, work)?.admission.campaign_id != campaign {
                return Err(err("work campaign mismatch"));
            }
            return Ok(Vec::new());
        };
        if !root.work.contains_key(work) {
            return Err(err("unknown work"));
        }
        Ok(root.attention.get(work).cloned().unwrap_or_default())
    }

    pub(crate) fn continuation_questions_in(
        tx: &WriteTransaction,
        campaign: &str,
        work: &str,
    ) -> Result<Vec<Attention>, String> {
        Ok(load(tx, campaign)?
            .and_then(|root| root.attention.get(work).cloned())
            .unwrap_or_default())
    }

    pub(crate) fn attention_request(&self, request: &ApiRequest) -> Result<ApiResponse, String> {
        let (campaign, after, limit) = match request {
            ApiRequest::CampaignAttentionList { id, after, limit } => {
                (id, after.as_deref(), *limit)
            }
            ApiRequest::CampaignAttentionAnswer { id, .. } => (id, None, 32),
            _ => return Err(err("not an attention request")),
        };
        if campaign.is_empty()
            || campaign.len() > 256
            || !(1..=32).contains(&limit)
            || after.is_some_and(|s| s.len() > 4096)
        {
            return Err(err("invalid attention bounds"));
        }
        let tx = self.database.begin_write().map_err(err)?;
        let mut root = load(&tx, campaign)?.ok_or_else(|| err("unknown campaign"))?;
        let now = now_ms()?;
        if let ApiRequest::CampaignAttentionAnswer {
            work_id,
            request_id,
            generation,
            instruction_revision,
            answer,
            ..
        } = request
        {
            if answer.trim().is_empty() || answer.len() > 4096 || answer.contains('\0') {
                return Err(err("invalid answer"));
            }
            let member = root.work.get(work_id).ok_or_else(|| err("unknown work"))?;
            let work = Self::admitted_work_in(&tx, work_id)?;
            let question = root
                .attention
                .get_mut(work_id)
                .and_then(|qs| qs.iter_mut().find(|q| &q.request_id == request_id))
                .ok_or_else(|| err("unknown question"))?;
            if work.admission.campaign_id != *campaign
                || work.admission.generation != *generation
                || question.generation != *generation
                || question.instruction_revision != *instruction_revision
                || Self::latest_instruction_revision_in(&tx, &work.admission)?
                    != *instruction_revision
            {
                return Err(err("stale question identity/revision"));
            }
            if let Some(previous) = &question.answer {
                if previous != answer {
                    return Err(err("answer conflict"));
                }
                return Ok(ApiResponse::CampaignAttentionAnswered {
                    attention: question.clone(),
                });
            }
            if member.terminal
                || member.cancellation_requested
                || now >= question.deadline_ms
                || !member
                    .wait
                    .as_ref()
                    .is_some_and(|w| matches!(&w.mode, WaitMode::Input(id) if id == request_id))
            {
                return Err(err("question no longer pending"));
            }
            question.answer = Some(answer.clone());
            let attention = question.clone();
            save(&tx, &root)?;
            tx.commit().map_err(err)?;
            return Ok(ApiResponse::CampaignAttentionAnswered { attention });
        }
        let mut questions = BTreeMap::new();
        for (work_id, items) in &root.attention {
            let member = &root.work[work_id];
            if member.terminal || member.cancellation_requested {
                continue;
            }
            let work = Self::admitted_work_in(&tx, work_id)?;
            let revision = Self::latest_instruction_revision_in(&tx, &work.admission)?;
            for q in items {
                let key = serde_json::to_string(&(work_id, &q.request_id)).map_err(err)?;
                if q.answer.is_none()
                    && now < q.deadline_ms
                    && q.instruction_revision == revision
                    && q.generation == work.admission.generation
                    && member.wait.as_ref().is_some_and(
                        |w| matches!(&w.mode, WaitMode::Input(id) if id == &q.request_id),
                    )
                    && after.is_none_or(|a| key.as_str() > a)
                {
                    if questions.len() <= limit
                        || questions
                            .last_key_value()
                            .is_some_and(|(last, _)| &key < last)
                    {
                        questions.insert(key, q.clone());
                        if questions.len() > limit + 1 {
                            questions.pop_last();
                        }
                    }
                }
            }
        }
        let more = questions.len() > limit;
        if more {
            questions.pop_last();
        }
        let next_cursor = more.then(|| questions.last_key_value().unwrap().0.clone());
        Ok(ApiResponse::CampaignAttentionList {
            questions: questions.into_values().collect(),
            next_cursor,
        })
    }
}
