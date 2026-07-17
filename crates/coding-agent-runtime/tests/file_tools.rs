use std::sync::Arc;

use coding_agent_runtime::{
    FileEntryKind, FileToolError, FileToolLimits, FileTools, RelativePath, RootCapability,
};
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;

#[test]
fn read_uses_inclusive_ranges_complete_lines_and_a_full_file_digest() {
    let fixture = fixture(limits(1_024, 80, 1_024, 4, 100, 100, 100));
    let bytes = "alpha\r\nβeta\ngamma\ndelta\n".as_bytes();
    std::fs::write(fixture.root_path.join("lines.txt"), bytes).unwrap();

    let result = fixture
        .tools
        .read_file(&RelativePath::parse("lines.txt").unwrap(), 2, 4)
        .unwrap();

    assert_eq!(
        result
            .lines
            .iter()
            .map(|line| (line.number, line.text.as_str()))
            .collect::<Vec<_>>(),
        vec![(2, "βeta"), (3, "gamma")]
    );
    assert_eq!(result.sha256, hex_digest(bytes));
    assert_eq!(result.file_bytes, bytes.len() as u64);
    assert_eq!(result.total_lines, 4);
    assert_eq!(result.returned_bytes, "βeta".len() + "gamma".len());
    assert!(result.truncated);
    assert_eq!(result.next_line, Some(4));
}

#[test]
fn read_rejects_directories_binary_non_utf8_and_hard_oversize() {
    let fixture = fixture(limits(8, 100, 100, 4, 100, 100, 100));
    std::fs::write(fixture.root_path.join("binary"), b"a\0b").unwrap();
    std::fs::write(fixture.root_path.join("non-utf8"), [0xff, 0xfe]).unwrap();
    std::fs::write(fixture.root_path.join("large"), b"123456789").unwrap();

    assert!(matches!(
        fixture
            .tools
            .read_file(&RelativePath::parse("").unwrap(), 1, 1),
        Err(FileToolError::FileNotRegular)
    ));
    assert!(matches!(
        fixture
            .tools
            .read_file(&RelativePath::parse("binary").unwrap(), 1, 10),
        Err(FileToolError::Binary)
    ));
    assert!(matches!(
        fixture
            .tools
            .read_file(&RelativePath::parse("non-utf8").unwrap(), 1, 10),
        Err(FileToolError::NotUtf8)
    ));
    assert!(matches!(
        fixture
            .tools
            .read_file(&RelativePath::parse("large").unwrap(), 1, 10),
        Err(FileToolError::FileTooLarge)
    ));
    assert!(matches!(
        fixture
            .tools
            .read_file(&RelativePath::parse("binary").unwrap(), 0, 1),
        Err(FileToolError::InvalidLineRange)
    ));
}

#[test]
fn read_result_budget_charges_each_empty_numbered_line() {
    let fixture = fixture(limits(1_024, 64, 1_024, 4, 100, 100, 100));
    std::fs::write(fixture.root_path.join("empty-lines.txt"), "\n".repeat(100)).unwrap();

    let result = fixture
        .tools
        .read_file(&RelativePath::parse("empty-lines.txt").unwrap(), 1, 100)
        .unwrap();

    assert_eq!(result.lines.len(), 2);
    assert_eq!(result.returned_bytes, 0);
    assert_eq!(result.next_line, Some(3));
    assert!(result.truncated);
}

#[test]
fn list_is_deterministic_depth_bounded_and_result_bounded() {
    let fixture = fixture(limits(1_024, 1_024, 1_024, 4, 100, 100, 100));
    std::fs::create_dir_all(fixture.root_path.join("src").join("nested")).unwrap();
    std::fs::write(fixture.root_path.join("z.txt"), b"z").unwrap();
    std::fs::write(fixture.root_path.join("a.txt"), b"a").unwrap();
    std::fs::write(fixture.root_path.join("src").join("lib.rs"), b"lib").unwrap();
    std::fs::write(
        fixture.root_path.join("src").join("nested").join("deep.rs"),
        b"deep",
    )
    .unwrap();

    let result = fixture
        .tools
        .list_files(&RelativePath::parse("").unwrap(), 2, 100)
        .unwrap();
    let entries = result
        .entries
        .iter()
        .map(|entry| (entry.path.as_slash_str(), entry.kind))
        .collect::<Vec<_>>();

    assert_eq!(
        entries,
        vec![
            ("a.txt", FileEntryKind::File),
            ("src", FileEntryKind::Directory),
            ("src/lib.rs", FileEntryKind::File),
            ("src/nested", FileEntryKind::Directory),
            ("z.txt", FileEntryKind::File),
        ]
    );
    assert!(!result.truncated);
    assert!(result.visited_entries >= result.entries.len());

    let bounded = fixture
        .tools
        .list_files(&RelativePath::parse("").unwrap(), 3, 2)
        .unwrap();
    assert_eq!(bounded.entries.len(), 2);
    assert!(bounded.truncated);
}

#[test]
fn list_hides_policy_paths_and_omits_links_and_special_entries() {
    let fixture = fixture(limits(1_024, 1_024, 1_024, 5, 100, 100, 100));
    let outside = tempfile::tempdir().unwrap();
    std::fs::write(outside.path().join("secret"), b"outside").unwrap();
    std::fs::create_dir_all(fixture.root_path.join(".git")).unwrap();
    std::fs::write(fixture.root_path.join(".git").join("config"), b"secret").unwrap();
    std::fs::create_dir_all(fixture.root_path.join("src").join("target")).unwrap();
    std::fs::write(
        fixture
            .root_path
            .join("src")
            .join("target")
            .join("artifact"),
        b"build",
    )
    .unwrap();
    std::fs::create_dir_all(fixture.root_path.join("assets")).unwrap();
    std::fs::write(
        fixture.root_path.join("assets").join("target"),
        b"real file",
    )
    .unwrap();
    create_file_link(
        &outside.path().join("secret"),
        &fixture.root_path.join("outside-link"),
    )
    .unwrap();

    let result = fixture
        .tools
        .list_files(&RelativePath::parse("").unwrap(), 5, 100)
        .unwrap();
    let paths = result
        .entries
        .iter()
        .map(|entry| entry.path.as_slash_str())
        .collect::<Vec<_>>();

    assert_eq!(paths, vec!["assets", "assets/target", "src"]);
    assert!(result.omitted_entries >= 3);
}

#[test]
fn list_rejects_argument_and_hard_traversal_limits() {
    let fixture = fixture(limits(1_024, 1_024, 1_024, 2, 2, 2, 2));
    for name in ["a", "b", "c"] {
        std::fs::write(fixture.root_path.join(name), name).unwrap();
    }

    assert!(matches!(
        fixture
            .tools
            .list_files(&RelativePath::parse("").unwrap(), 0, 1),
        Err(FileToolError::InvalidLimit)
    ));
    assert!(matches!(
        fixture
            .tools
            .list_files(&RelativePath::parse("").unwrap(), 3, 1),
        Err(FileToolError::InvalidLimit)
    ));
    assert!(matches!(
        fixture
            .tools
            .list_files(&RelativePath::parse("").unwrap(), 1, 3),
        Err(FileToolError::InvalidLimit)
    ));
    assert!(matches!(
        fixture
            .tools
            .list_files(&RelativePath::parse("").unwrap(), 1, 2),
        Err(FileToolError::DirectoryTooLarge)
    ));
}

#[test]
fn search_is_literal_deterministic_globbed_and_unicode_positioned() {
    let fixture = fixture(limits(4_096, 1_024, 4_096, 5, 100, 100, 100));
    std::fs::create_dir_all(fixture.root_path.join("src")).unwrap();
    std::fs::write(fixture.root_path.join("main.rs"), "root needle.*\n").unwrap();
    std::fs::write(
        fixture.root_path.join("src").join("lib.rs"),
        "α needle.* needle.*\n",
    )
    .unwrap();
    std::fs::write(
        fixture.root_path.join("src").join("notes.txt"),
        "needle.* ignored\n",
    )
    .unwrap();
    std::fs::write(
        fixture.root_path.join("src").join("binary.rs"),
        b"needle.*\0binary",
    )
    .unwrap();

    let result = fixture
        .tools
        .search_text(
            "needle.*",
            &RelativePath::parse("").unwrap(),
            Some("**/*.rs"),
            100,
        )
        .unwrap();

    assert_eq!(
        result
            .matches
            .iter()
            .map(|found| (
                found.path.as_slash_str(),
                found.line_number,
                found.column,
                found.preview.as_str(),
            ))
            .collect::<Vec<_>>(),
        vec![
            ("main.rs", 1, 6, "root needle.*"),
            ("src/lib.rs", 1, 3, "α needle.* needle.*"),
            ("src/lib.rs", 1, 12, "α needle.* needle.*"),
        ]
    );
    assert_eq!(result.skipped_files, 1);
    assert!(!result.truncated);

    let bounded = fixture
        .tools
        .search_text(
            "needle.*",
            &RelativePath::parse("").unwrap(),
            Some("**/*.rs"),
            1,
        )
        .unwrap();
    assert_eq!(bounded.matches.len(), 1);
    assert!(bounded.truncated);
}

#[test]
fn search_validates_inputs_and_accepts_a_direct_file_root() {
    let fixture = fixture(limits(1_024, 1_024, 1_024, 5, 100, 100, 100));
    std::fs::write(fixture.root_path.join("one.rs"), "find me\n").unwrap();

    let direct = fixture
        .tools
        .search_text(
            "find",
            &RelativePath::parse("one.rs").unwrap(),
            Some("*.rs"),
            10,
        )
        .unwrap();
    assert_eq!(direct.matches.len(), 1);

    for glob in ["", "../*.rs", r"src\*.rs", "/abs/*.rs", ".git/**", "[bad"] {
        assert!(matches!(
            fixture
                .tools
                .search_text("find", &RelativePath::parse("").unwrap(), Some(glob), 10,),
            Err(FileToolError::InvalidGlob)
        ));
    }
    for query in ["", "line\nbreak", "carriage\rreturn"] {
        assert!(matches!(
            fixture
                .tools
                .search_text(query, &RelativePath::parse("").unwrap(), None, 10,),
            Err(FileToolError::InvalidQuery)
        ));
    }

    for glob in [
        "?".repeat(1_025),
        std::iter::repeat_n("a", 65).collect::<Vec<_>>().join("/"),
        "*".repeat(129),
    ] {
        assert!(matches!(
            fixture
                .tools
                .search_text("find", &RelativePath::parse("").unwrap(), Some(&glob), 10,),
            Err(FileToolError::InvalidGlob)
        ));
    }
}

#[test]
fn search_enforces_the_configured_hard_depth_limit() {
    let fixture = fixture(limits(1_024, 1_024, 1_024, 2, 100, 100, 100));
    std::fs::create_dir_all(fixture.root_path.join("a/b/c")).unwrap();
    std::fs::write(fixture.root_path.join("a/b/c/value.txt"), b"needle").unwrap();

    assert!(matches!(
        fixture
            .tools
            .search_text("needle", &RelativePath::parse("").unwrap(), None, 100,),
        Err(FileToolError::TraversalLimitExceeded)
    ));
}

#[test]
fn search_fails_closed_at_the_cumulative_input_budget() {
    let limits = FileToolLimits::try_new(8, 12, 1_024, 1_024, 2, 100, 100, 100).unwrap();
    let fixture = fixture(limits);
    std::fs::write(fixture.root_path.join("a.txt"), b"12345678").unwrap();
    std::fs::write(fixture.root_path.join("b.txt"), b"abcdefgh").unwrap();

    assert!(matches!(
        fixture
            .tools
            .search_text("missing", &RelativePath::parse("").unwrap(), None, 100,),
        Err(FileToolError::SearchLimitExceeded)
    ));
}

#[test]
fn file_operations_observe_a_pre_cancelled_token() {
    let fixture = fixture(limits(1_024, 1_024, 1_024, 2, 100, 100, 100));
    std::fs::write(fixture.root_path.join("value.txt"), b"needle").unwrap();
    let cancellation = CancellationToken::new();
    cancellation.cancel();

    assert!(matches!(
        fixture.tools.search_text_cancellable(
            "needle",
            &RelativePath::parse("").unwrap(),
            None,
            100,
            &cancellation,
        ),
        Err(FileToolError::Cancelled)
    ));
    assert!(matches!(
        fixture.tools.read_file_cancellable(
            &RelativePath::parse("value.txt").unwrap(),
            1,
            1,
            &cancellation,
        ),
        Err(FileToolError::Cancelled)
    ));
}

#[test]
fn direct_links_have_stable_non_plain_path_errors() {
    let fixture = fixture(limits(1_024, 1_024, 1_024, 4, 100, 100, 100));
    std::fs::write(fixture.root_path.join("target.txt"), b"text").unwrap();
    std::fs::create_dir(fixture.root_path.join("target-dir")).unwrap();
    create_file_link(
        &fixture.root_path.join("target.txt"),
        &fixture.root_path.join("file-link"),
    )
    .unwrap();
    create_dir_link(
        &fixture.root_path.join("target-dir"),
        &fixture.root_path.join("dir-link"),
    )
    .unwrap();

    assert!(matches!(
        fixture
            .tools
            .read_file(&RelativePath::parse("file-link").unwrap(), 1, 10),
        Err(FileToolError::FileNotRegular)
    ));
    assert!(matches!(
        fixture
            .tools
            .list_files(&RelativePath::parse("dir-link").unwrap(), 1, 10),
        Err(FileToolError::DirectoryNotFound)
    ));
    assert!(matches!(
        fixture
            .tools
            .search_text("text", &RelativePath::parse("file-link").unwrap(), None, 10,),
        Err(FileToolError::FileNotRegular)
    ));
}

#[cfg(unix)]
#[test]
fn unix_list_and_search_see_regular_files_without_blocking_on_a_fifo() {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;
    use std::sync::mpsc;
    use std::time::Duration;

    let fixture = fixture(limits(1_024, 1_024, 1_024, 4, 100, 100, 100));
    std::fs::write(fixture.root_path.join("regular.txt"), b"needle").unwrap();
    let fifo = fixture.root_path.join("pipe");
    let fifo = CString::new(fifo.as_os_str().as_bytes()).unwrap();
    assert_eq!(unsafe { libc::mkfifo(fifo.as_ptr(), 0o600) }, 0);
    let (sender, receiver) = mpsc::channel();

    let worker = std::thread::spawn(move || {
        let listed = fixture
            .tools
            .list_files(&RelativePath::parse("").unwrap(), 1, 100)
            .unwrap()
            .entries
            .into_iter()
            .map(|entry| entry.path.as_slash_str().to_owned())
            .collect::<Vec<_>>();
        let searched = fixture
            .tools
            .search_text("needle", &RelativePath::parse("").unwrap(), None, 100)
            .unwrap()
            .matches
            .into_iter()
            .map(|found| found.path.as_slash_str().to_owned())
            .collect::<Vec<_>>();
        sender.send((listed, searched)).unwrap();
    });

    let (listed, searched) = receiver
        .recv_timeout(Duration::from_secs(2))
        .expect("list/search blocked while inspecting a FIFO");
    worker.join().unwrap();
    assert_eq!(listed, vec!["regular.txt"]);
    assert_eq!(searched, vec!["regular.txt"]);
}

#[test]
fn repeated_and_concurrent_root_walks_have_independent_directory_cursors() {
    let fixture = fixture(limits(1_024, 1_024, 1_024, 2, 100, 100, 100));
    for name in ["a", "b", "c"] {
        std::fs::write(fixture.root_path.join(name), name).unwrap();
    }
    let tools = Arc::new(fixture.tools);

    std::thread::scope(|scope| {
        for _ in 0..8 {
            let tools = tools.clone();
            scope.spawn(move || {
                for _ in 0..20 {
                    let result = tools
                        .list_files(&RelativePath::parse("").unwrap(), 1, 100)
                        .unwrap();
                    assert_eq!(result.entries.len(), 3);
                }
            });
        }
    });
}

struct Fixture {
    _temp: tempfile::TempDir,
    root_path: std::path::PathBuf,
    tools: FileTools,
}

fn fixture(limits: FileToolLimits) -> Fixture {
    let temp = tempfile::tempdir().unwrap();
    let root_path = temp.path().to_path_buf();
    let root = RootCapability::open(&root_path).unwrap();
    Fixture {
        _temp: temp,
        root_path,
        tools: FileTools::new(root, limits),
    }
}

fn limits(
    max_file_bytes: usize,
    max_read_result_bytes: usize,
    max_result_bytes: usize,
    max_depth: u32,
    max_visited_entries: usize,
    max_directory_entries: usize,
    max_result_items: usize,
) -> FileToolLimits {
    FileToolLimits::try_new(
        max_file_bytes,
        max_file_bytes.saturating_mul(max_visited_entries),
        max_read_result_bytes,
        max_result_bytes,
        max_depth,
        max_visited_entries,
        max_directory_entries,
        max_result_items,
    )
    .unwrap()
}

fn hex_digest(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[cfg(unix)]
fn create_file_link(target: &std::path::Path, link: &std::path::Path) -> std::io::Result<()> {
    std::os::unix::fs::symlink(target, link)
}

#[cfg(windows)]
fn create_file_link(target: &std::path::Path, link: &std::path::Path) -> std::io::Result<()> {
    std::os::windows::fs::symlink_file(target, link)
}

#[cfg(unix)]
fn create_dir_link(target: &std::path::Path, link: &std::path::Path) -> std::io::Result<()> {
    std::os::unix::fs::symlink(target, link)
}

#[cfg(windows)]
fn create_dir_link(target: &std::path::Path, link: &std::path::Path) -> std::io::Result<()> {
    std::os::windows::fs::symlink_dir(target, link)
}
