#![allow(dead_code)]

use std::io;
use std::process::{Command, ExitStatus, Output, Stdio};

pub fn command_output(command: &mut Command) -> io::Result<Output> {
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let child = {
        let _spawn_guard = coding_agent_runtime::acquire_process_spawn_lock();
        command.spawn()?
    };
    child.wait_with_output()
}

pub fn command_status(command: &mut Command) -> io::Result<ExitStatus> {
    let mut child = {
        let _spawn_guard = coding_agent_runtime::acquire_process_spawn_lock();
        command.spawn()?
    };
    child.wait()
}
