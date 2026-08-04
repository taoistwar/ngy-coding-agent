include!("support.rs");

mod actor_lifecycle;
mod admission;
mod claim_ingress;
mod claim_races;
#[cfg(feature = "test-support")]
mod critical_stop_deadline;
#[cfg(feature = "test-support")]
mod critical_stop_retry;
#[cfg(feature = "test-support")]
mod final_stop;
#[cfg(feature = "test-support")]
mod mutation_replay;
#[cfg(feature = "test-support")]
mod pending_replay;
mod process_cleanup;
#[cfg(feature = "test-support")]
mod quiesce_barriers;
#[cfg(feature = "test-support")]
mod quiesce_ordering;
#[cfg(feature = "test-support")]
mod quiesce_ownership;
#[cfg(feature = "test-support")]
mod quiesce_typed_replay;
#[cfg(feature = "test-support")]
mod record_review_completion;
#[cfg(feature = "test-support")]
mod record_review_deadlines;
#[cfg(feature = "test-support")]
mod record_review_ordering;
#[cfg(feature = "test-support")]
mod recovery_projection;
#[cfg(feature = "test-support")]
mod rollback_stop;
#[cfg(feature = "test-support")]
mod runner_preparation;
#[cfg(feature = "test-support")]
mod scheduler_refresh;
#[cfg(feature = "test-support")]
mod stop_fail_closed;
#[cfg(feature = "test-support")]
mod stop_predecessors;
mod storage_signals;
#[cfg(feature = "test-support")]
mod terminal_projection;
#[cfg(feature = "test-support")]
mod terminal_write_completion;
#[cfg(feature = "test-support")]
mod terminal_write_retry;
