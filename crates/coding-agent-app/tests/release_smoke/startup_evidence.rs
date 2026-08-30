use std::fs;
use std::io;
use std::path::Path;
use std::time::{Duration, Instant};

use coding_agent_app::{RuntimeDescriptor, RuntimeDescriptorError};

const MAX_RUNTIME_DIAGNOSTIC_ENTRIES: usize = 256;
const DELIVERY_PROBE_WORKSPACE_PREFIX: &str = ".coding-agent-delivery-probe-";
const DELIVERY_PROBE_OBSERVATION_INTERVAL: Duration = Duration::from_millis(100);
const INSTANCE_LOCK_NAME: &str = "instance.lock";
const PROCESS_LIVENESS_DIRECTORY_NAME: &str = "process-liveness";

#[derive(Clone, Copy, Default)]
struct StickyPresence {
    seen: bool,
    observed: bool,
    observation_failed: bool,
}

impl StickyPresence {
    fn observe(&mut self, result: io::Result<bool>) {
        if let Ok(present) = result {
            self.observed = true;
            self.seen |= present;
        } else {
            self.observation_failed = true;
        }
    }

    fn label(self) -> &'static str {
        if self.seen {
            "yes"
        } else if self.observation_failed {
            "unknown"
        } else if self.observed {
            "no"
        } else {
            "unknown"
        }
    }
}

#[derive(Clone, Copy, Default)]
pub(super) struct StartupEvidence {
    instance_lock: StickyPresence,
    process_liveness: StickyPresence,
    delivery_probe: StickyPresence,
    database: StickyPresence,
    descriptor_path: StickyPresence,
    descriptor_parseable: StickyPresence,
    descriptor_for_child: StickyPresence,
    next_delivery_probe_observation: Option<Instant>,
}

impl StartupEvidence {
    pub(super) fn observe_milestones_at(
        &mut self,
        runtime_dir: &Path,
        database_path: &Path,
        observed_at: Instant,
    ) {
        self.instance_lock
            .observe(runtime_dir.join(INSTANCE_LOCK_NAME).try_exists());
        self.process_liveness.observe(
            runtime_dir
                .join(PROCESS_LIVENESS_DIRECTORY_NAME)
                .try_exists(),
        );
        if self
            .next_delivery_probe_observation
            .is_none_or(|next_observation| observed_at >= next_observation)
        {
            self.delivery_probe
                .observe(delivery_probe_workspace_exists(runtime_dir));
            self.next_delivery_probe_observation =
                Some(observed_at + DELIVERY_PROBE_OBSERVATION_INTERVAL);
        }
        self.database.observe(database_path.try_exists());
    }

    pub(super) fn observe_descriptor(&mut self, descriptor_path: &Path, child_pid: u32) {
        self.descriptor_path.observe(descriptor_path.try_exists());
        match RuntimeDescriptor::read(descriptor_path) {
            Ok(descriptor) => {
                self.descriptor_parseable.observe(Ok(true));
                self.descriptor_for_child
                    .observe(Ok(descriptor.pid().get() == child_pid));
            }
            Err(RuntimeDescriptorError::Io(error)) => {
                self.descriptor_parseable.observe(Err(error));
            }
            Err(_) => {
                self.descriptor_parseable.observe(Ok(false));
            }
        }
    }
}

pub(super) fn read_descriptor_before_deadline(
    descriptor_path: &Path,
    deadline: Instant,
) -> Option<RuntimeDescriptor> {
    let descriptor = RuntimeDescriptor::read(descriptor_path).ok()?;
    (Instant::now() < deadline).then_some(descriptor)
}

pub(super) fn format_startup_evidence(
    before_deadline: &StartupEvidence,
    during_grace: &StartupEvidence,
) -> String {
    format!(
        "instance_lock_seen_before_deadline={}, process_liveness_seen_before_deadline={}, delivery_probe_seen_before_deadline={}, database_seen_before_deadline={}, instance_lock_seen_during_grace={}, process_liveness_seen_during_grace={}, delivery_probe_seen_during_grace={}, database_seen_during_grace={}, descriptor_path_seen_during_grace={}, descriptor_parseable_during_grace={}, descriptor_for_child_seen_during_grace={}",
        before_deadline.instance_lock.label(),
        before_deadline.process_liveness.label(),
        before_deadline.delivery_probe.label(),
        before_deadline.database.label(),
        during_grace.instance_lock.label(),
        during_grace.process_liveness.label(),
        during_grace.delivery_probe.label(),
        during_grace.database.label(),
        during_grace.descriptor_path.label(),
        during_grace.descriptor_parseable.label(),
        during_grace.descriptor_for_child.label(),
    )
}

fn delivery_probe_workspace_exists(runtime_dir: &Path) -> io::Result<bool> {
    let mut entries = match fs::read_dir(runtime_dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error),
    };
    for _ in 0..MAX_RUNTIME_DIAGNOSTIC_ENTRIES {
        let Some(entry) = entries.next() else {
            return Ok(false);
        };
        let entry = entry?;
        let name = entry.file_name();
        let Some(suffix) = name
            .to_str()
            .and_then(|name| name.strip_prefix(DELIVERY_PROBE_WORKSPACE_PREFIX))
        else {
            continue;
        };
        let has_canonical_name = suffix.len() == 32
            && suffix
                .bytes()
                .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'));
        if has_canonical_name && entry.file_type()?.is_dir() {
            return Ok(true);
        }
    }
    match entries.next().transpose()? {
        Some(_) => Err(io::Error::other("runtime diagnostic entry budget exceeded")),
        None => Ok(false),
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write as _;
    use std::num::{NonZeroU16, NonZeroU32};

    use coding_agent_app::{PrivateFile, SecuritySeed};
    use coding_agent_domain::UtcTimestamp;

    use super::*;

    #[test]
    fn latches_transient_milestones_and_reports_only_safe_labels() {
        let temporary = tempfile::tempdir().expect("create startup evidence fixture");
        let runtime_dir = temporary.path().join("runtime-secret-path");
        let database_path = temporary.path().join("database-secret-path.sqlite3");
        let descriptor_path = runtime_dir.join("instance.json");
        fs::create_dir(&runtime_dir).expect("create startup evidence runtime directory");

        let first_observation = Instant::now();
        let mut before_deadline = StartupEvidence::default();
        before_deadline.observe_milestones_at(&runtime_dir, &database_path, first_observation);
        fs::write(runtime_dir.join(INSTANCE_LOCK_NAME), b"")
            .expect("create startup evidence instance lock");
        fs::create_dir(runtime_dir.join(PROCESS_LIVENESS_DIRECTORY_NAME))
            .expect("create startup evidence process-liveness directory");
        let probe_path = runtime_dir.join(format!(
            "{DELIVERY_PROBE_WORKSPACE_PREFIX}{}",
            "a".repeat(32)
        ));
        fs::create_dir(&probe_path).expect("create transient delivery probe workspace");
        before_deadline.observe_milestones_at(
            &runtime_dir,
            &database_path,
            first_observation + DELIVERY_PROBE_OBSERVATION_INTERVAL,
        );
        fs::remove_dir(&probe_path).expect("remove transient delivery probe workspace");
        fs::write(&database_path, b"").expect("create startup evidence database");
        before_deadline.observe_milestones_at(
            &runtime_dir,
            &database_path,
            first_observation + DELIVERY_PROBE_OBSERVATION_INTERVAL * 2,
        );

        let seed = SecuritySeed::generate().expect("generate descriptor fixture secret");
        let fixture_secret = seed.launcher_secret().as_str().to_owned();
        let descriptor = RuntimeDescriptor::new(
            uuid::Uuid::new_v4(),
            NonZeroU32::new(41_231).expect("fixture process ID is nonzero"),
            NonZeroU16::new(43_121).expect("fixture port is nonzero"),
            UtcTimestamp::parse_rfc3339("2026-08-30T00:00:00Z").expect("fixed fixture timestamp"),
            seed.launcher_secret().clone(),
        )
        .expect("construct late descriptor fixture");
        descriptor
            .publish(&descriptor_path)
            .expect("publish late descriptor fixture");

        let mut during_grace = StartupEvidence::default();
        during_grace.observe_milestones_at(&runtime_dir, &database_path, Instant::now());
        during_grace.observe_descriptor(&descriptor_path, descriptor.pid().get() + 1);
        assert_eq!(during_grace.descriptor_for_child.label(), "no");
        during_grace.observe_descriptor(&descriptor_path, descriptor.pid().get());
        let report = format_startup_evidence(&before_deadline, &during_grace);
        let temporary_path_text = temporary.path().to_string_lossy();

        assert!(report.contains("instance_lock_seen_before_deadline=yes"));
        assert!(report.contains("process_liveness_seen_before_deadline=yes"));
        assert!(report.contains("delivery_probe_seen_before_deadline=yes"));
        assert!(report.contains("database_seen_before_deadline=yes"));
        assert!(report.contains("descriptor_path_seen_during_grace=yes"));
        assert!(report.contains("descriptor_parseable_during_grace=yes"));
        assert!(report.contains("descriptor_for_child_seen_during_grace=yes"));
        for forbidden in [
            temporary_path_text.as_ref(),
            "runtime-secret-path",
            "database-secret-path.sqlite3",
            fixture_secret.as_str(),
            "arbitrary stderr",
        ] {
            assert!(
                !report.contains(forbidden),
                "startup evidence must contain fixed labels only"
            );
        }

        let expired_deadline = Instant::now();
        assert!(
            read_descriptor_before_deadline(&descriptor_path, expired_deadline).is_none(),
            "a descriptor observed at or after the deadline must never be accepted"
        );
        let open_deadline = Instant::now() + Duration::from_secs(30);
        assert!(
            read_descriptor_before_deadline(&descriptor_path, open_deadline).is_some(),
            "the same valid descriptor remains acceptable before the deadline"
        );
    }

    #[test]
    fn reports_observation_failures_and_invalid_descriptors_conservatively() {
        let mut presence = StickyPresence::default();
        presence.observe(Ok(false));
        assert_eq!(presence.label(), "no");
        presence.observe(Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "sensitive observation error",
        )));
        assert_eq!(presence.label(), "unknown");
        presence.observe(Ok(true));
        assert_eq!(presence.label(), "yes");

        let temporary = tempfile::tempdir().expect("create invalid descriptor fixture");
        let descriptor_path = temporary.path().join("instance.json");
        let mut descriptor_file = PrivateFile::create_new(&descriptor_path)
            .expect("create private invalid descriptor fixture")
            .into_file();
        descriptor_file
            .write_all(b"not a runtime descriptor")
            .expect("write invalid descriptor fixture");
        descriptor_file
            .flush()
            .expect("flush invalid descriptor fixture");
        let mut evidence = StartupEvidence::default();
        evidence.observe_descriptor(&descriptor_path, 41_231);
        assert_eq!(evidence.descriptor_path.label(), "yes");
        assert_eq!(evidence.descriptor_parseable.label(), "no");
        assert_eq!(evidence.descriptor_for_child.label(), "unknown");
        let report = format_startup_evidence(&StartupEvidence::default(), &evidence);
        assert!(!report.contains("not a runtime descriptor"));
        assert!(!report.contains("sensitive observation error"));
        assert!(!report.contains(temporary.path().to_string_lossy().as_ref()));
    }
}
