//! HTTP and server-sent-event contracts for the coding agent.

mod backend;
mod contract;
mod delivery_wire;
mod error;
mod router;
mod scheduler_wire;
mod sse;

pub use backend::{
    ApiBackend, AuthContext, CancelResult, CreateResult, DeliveryBackend, LiveEventItem,
    LiveEventStream, QuitAcceptance, RequestSecurity, SchedulerStateStream, ServiceStateStream,
    SessionExchange, SseBackend,
};
pub use contract::*;
pub use delivery_wire::*;
pub use error::{ApiError, ApiErrorResponse, ApiResult, DeliveryApiErrorKind};
pub use router::{api_openapi, build_api_router, build_api_router_with_delivery};
pub use scheduler_wire::*;
