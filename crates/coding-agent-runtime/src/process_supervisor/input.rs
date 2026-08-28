mod fixture;
mod model;
mod writer;

pub use fixture::{
    MAX_EXACT_CHILD_INPUT_BYTES_FOR_TEST, ProcessStdinTestObservation, ProcessStdinTestOutcome,
    ProcessStdinTestScenario, exercise_process_stdin_for_test,
};
pub(crate) use model::ExactChildInput;
pub(super) use writer::{
    SupervisedExactInputWriter, abort_and_join, child_stdin, complete, spawn_writer,
    wait_for_completion,
};
