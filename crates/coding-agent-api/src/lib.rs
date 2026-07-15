//! HTTP and server-sent-event contracts for the coding agent.

mod backend;
mod contract;
mod error;

pub use backend::{
    ApiBackend, AuthContext, CancelResult, CreateResult, LiveEventItem, LiveEventStream,
    QuitAcceptance, RequestSecurity, ServiceStateStream, SessionExchange, SseBackend,
};
pub use contract::*;
pub use error::{ApiError, ApiErrorResponse, ApiResult};
