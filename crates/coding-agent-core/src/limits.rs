#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AgentLimits {
    max_model_steps: u32,
    max_tool_calls: u32,
    max_provider_bytes: usize,
    max_tool_result_bytes: usize,
}

impl AgentLimits {
    pub fn try_new(
        max_model_steps: u32,
        max_tool_calls: u32,
        max_provider_bytes: usize,
        max_tool_result_bytes: usize,
    ) -> Result<Self, AgentLimitsError> {
        if max_model_steps == 0
            || max_tool_calls == 0
            || max_provider_bytes == 0
            || max_tool_result_bytes == 0
        {
            return Err(AgentLimitsError::ZeroBudget);
        }
        Ok(Self {
            max_model_steps,
            max_tool_calls,
            max_provider_bytes,
            max_tool_result_bytes,
        })
    }

    pub const fn max_model_steps(self) -> u32 {
        self.max_model_steps
    }

    pub const fn max_tool_calls(self) -> u32 {
        self.max_tool_calls
    }

    pub const fn max_provider_bytes(self) -> usize {
        self.max_provider_bytes
    }

    pub const fn max_tool_result_bytes(self) -> usize {
        self.max_tool_result_bytes
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum AgentLimitsError {
    #[error("agent budgets must all be non-zero")]
    ZeroBudget,
}
