use std::sync::Arc;

use crate::runtime::OrchestrationHandler;
use crate::{Action, Event, LogLevel};

/// Decisions are the same as public Actions; we emit them directly from the replay core.
pub type Decision = Action;

pub trait ReplayEngine: Send + Sync {
    /// Replays one turn and returns updated history, pure decisions, logs,
    /// optional output, and claimed ids snapshot for diagnostics.
    fn replay(
        &self,
        history: Vec<Event>,
        turn_index: u64,
        handler: Arc<dyn OrchestrationHandler>,
        input: String,
    ) -> (
        Vec<Event>,
        Vec<Decision>,
        Vec<(LogLevel, String)>,
        Option<Result<String, String>>,
        crate::ClaimedIdsSnapshot,
    );
}

pub struct DefaultReplayEngine;

impl Default for DefaultReplayEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl DefaultReplayEngine {
    pub fn new() -> Self {
        Self
    }
}

impl ReplayEngine for DefaultReplayEngine {
    fn replay(
        &self,
        history: Vec<Event>,
        turn_index: u64,
        handler: Arc<dyn OrchestrationHandler>,
        input: String,
    ) -> (
        Vec<Event>,
        Vec<Decision>,
        Vec<(LogLevel, String)>,
        Option<Result<String, String>>,
        crate::ClaimedIdsSnapshot,
    ) {
        let orchestrator = |ctx: crate::OrchestrationContext| {
            let h = handler.clone();
            let inp = input.clone();
            async move { h.invoke(ctx, inp).await }
        };
        let (hist_after, decisions, logs, out_opt, claims) =
            crate::run_turn_with_claims(history, turn_index, orchestrator);
        (hist_after, decisions, logs, out_opt, claims)
    }
}
