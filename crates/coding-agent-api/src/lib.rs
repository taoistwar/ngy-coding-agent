//! HTTP and server-sent-event contracts for the coding agent.

mod backend;
mod contract;
mod error;
mod router;

pub use backend::{
    ApiBackend, AuthContext, CancelResult, CreateResult, LiveEventItem, LiveEventStream,
    QuitAcceptance, RequestSecurity, ServiceStateStream, SessionExchange, SseBackend,
};
pub use contract::*;
pub use error::{ApiError, ApiErrorResponse, ApiResult};
pub use router::{api_openapi, build_api_router};
