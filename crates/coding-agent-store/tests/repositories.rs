mod support;

use std::path::PathBuf;
use std::time::Duration;

use coding_agent_domain::{CanonicalPath, UtcTimestamp};
use coding_agent_store::RegisterRepositoryOutcome;

#[tokio::test]
async fn registering_the_same_workspace_reuses_the_row() {
    let fixture = support::store_fixture().await;
    let input = fixture.canonical_repository_input("repo").await;
    let first = fixture
        .store
        .register_repository(input.clone())
        .await
        .unwrap();
    let second = fixture.store.register_repository(input).await.unwrap();
    assert!(matches!(first, RegisterRepositoryOutcome::Created(_)));
    assert!(matches!(second, RegisterRepositoryOutcome::Existing(_)));
    assert_eq!(fixture.store.list_repositories().await.unwrap().len(), 1);
}

#[tokio::test]
async fn display_path_does_not_define_identity_and_is_refreshed() {
    let fixture = support::store_fixture().await;
    let original = fixture.canonical_repository_input("repo").await;
    let created = match fixture
        .store
        .register_repository(original.clone())
        .await
        .unwrap()
    {
        RegisterRepositoryOutcome::Created(repository) => repository,
        RegisterRepositoryOutcome::Existing(_) => panic!("first registration must create a row"),
    };

    tokio::time::sleep(Duration::from_millis(2)).await;
    let mut reopened = original.clone();
    reopened.selected_path = fixture
        .canonical_path("repositories/repo/different-selection")
        .await;
    reopened.display_name = "a display-only rename".to_owned();
    let existing = match fixture
        .store
        .register_repository(reopened.clone())
        .await
        .unwrap()
    {
        RegisterRepositoryOutcome::Existing(repository) => repository,
        RegisterRepositoryOutcome::Created(_) => panic!("identity pair must reuse the row"),
    };

    assert_eq!(existing.id, created.id);
    assert_eq!(existing.selected_path, reopened.selected_path);
    assert_eq!(existing.display_name, original.display_name);
    assert!(existing.last_opened_at > created.last_opened_at);
}

#[tokio::test]
async fn repositories_are_ordered_by_last_opened_then_id() {
    let fixture = support::store_fixture().await;
    let first_input = fixture.canonical_repository_input("first").await;
    let second_input = fixture.canonical_repository_input("second").await;

    let first = match fixture
        .store
        .register_repository(first_input.clone())
        .await
        .unwrap()
    {
        RegisterRepositoryOutcome::Created(repository) => repository,
        RegisterRepositoryOutcome::Existing(_) => panic!("first identity must be new"),
    };
    tokio::time::sleep(Duration::from_millis(2)).await;
    let second = match fixture
        .store
        .register_repository(second_input)
        .await
        .unwrap()
    {
        RegisterRepositoryOutcome::Created(repository) => repository,
        RegisterRepositoryOutcome::Existing(_) => panic!("second identity must be new"),
    };
    tokio::time::sleep(Duration::from_millis(2)).await;
    fixture
        .store
        .register_repository(first_input)
        .await
        .unwrap();

    let recent = fixture.store.list_repositories().await.unwrap();
    assert_eq!(recent[0].id, first.id);
    assert_eq!(recent[1].id, second.id);

    sqlx::query("UPDATE repositories SET last_opened_at = ?")
        .bind("2026-01-01T00:00:00.000000000Z")
        .execute(fixture.store.pool())
        .await
        .unwrap();

    let tied = fixture.store.list_repositories().await.unwrap();
    let mut expected_ids = vec![first.id, second.id];
    expected_ids.sort_by_key(ToString::to_string);
    assert_eq!(
        tied.iter()
            .map(|repository| repository.id)
            .collect::<Vec<_>>(),
        expected_ids
    );
}

#[tokio::test]
async fn repository_rows_store_lowercase_uuids_rfc3339_times_and_identity_keys() {
    let fixture = support::store_fixture().await;
    let input = fixture.canonical_repository_input("FormatRepo").await;
    let repository = match fixture
        .store
        .register_repository(input.clone())
        .await
        .unwrap()
    {
        RegisterRepositoryOutcome::Created(repository) => repository,
        RegisterRepositoryOutcome::Existing(_) => panic!("first registration must create a row"),
    };

    let (id, created_at, last_opened_at, git_key, cargo_key): (
        String,
        String,
        String,
        String,
        String,
    ) = sqlx::query_as(
        "SELECT id, created_at, last_opened_at, git_identity_key, cargo_identity_key \
         FROM repositories WHERE id = ?",
    )
    .bind(repository.id.to_string())
    .fetch_one(fixture.store.pool())
    .await
    .unwrap();

    assert_eq!(id, repository.id.to_string());
    assert_eq!(id, id.to_lowercase());
    uuid::Uuid::parse_str(&id).unwrap();
    assert_eq!(
        UtcTimestamp::parse_rfc3339(&created_at)
            .unwrap()
            .to_string(),
        created_at
    );
    assert_eq!(
        UtcTimestamp::parse_rfc3339(&last_opened_at)
            .unwrap()
            .to_string(),
        last_opened_at
    );

    #[cfg(windows)]
    {
        assert_eq!(
            git_key,
            input.git_root.to_string().replace('/', "\\").to_lowercase()
        );
        assert_eq!(
            cargo_key,
            input
                .cargo_workspace_root
                .to_string()
                .replace('/', "\\")
                .to_lowercase()
        );
    }

    #[cfg(not(windows))]
    {
        assert_eq!(git_key, input.git_root.to_string());
        assert_eq!(cargo_key, input.cargo_workspace_root.to_string());
    }
}

#[cfg(windows)]
#[tokio::test]
async fn windows_identity_reuses_case_variant_paths() {
    let fixture = support::store_fixture().await;
    let input = fixture.canonical_repository_input("CaseRepo").await;
    let first = match fixture
        .store
        .register_repository(input.clone())
        .await
        .unwrap()
    {
        RegisterRepositoryOutcome::Created(repository) => repository,
        RegisterRepositoryOutcome::Existing(_) => panic!("first registration must create a row"),
    };

    let mut variant = input;
    variant.git_root = lowercase_path(&variant.git_root);
    variant.cargo_workspace_root = lowercase_path(&variant.cargo_workspace_root);
    variant.selected_path = fixture
        .canonical_path("repositories/CaseRepo/reopened")
        .await;
    let existing = match fixture.store.register_repository(variant).await.unwrap() {
        RegisterRepositoryOutcome::Existing(repository) => repository,
        RegisterRepositoryOutcome::Created(_) => panic!("Windows paths are case-insensitive"),
    };

    assert_eq!(existing.id, first.id);
    assert_eq!(fixture.store.list_repositories().await.unwrap().len(), 1);
}

#[cfg(windows)]
fn lowercase_path(path: &CanonicalPath) -> CanonicalPath {
    let lowered = path.to_string().to_lowercase();
    assert_ne!(
        lowered,
        path.to_string(),
        "fixture path needs a case variant"
    );
    CanonicalPath::try_from_canonical(PathBuf::from(lowered)).unwrap()
}

#[cfg(not(windows))]
#[tokio::test]
async fn unix_identity_preserves_path_case() {
    let fixture = support::store_fixture().await;
    let input = fixture.canonical_repository_input("CaseRepo").await;
    let first = fixture
        .store
        .register_repository(input.clone())
        .await
        .unwrap();

    let mut variant = input;
    variant.git_root = uppercase_path(&variant.git_root);
    variant.cargo_workspace_root = uppercase_path(&variant.cargo_workspace_root);
    let second = fixture.store.register_repository(variant).await.unwrap();

    assert!(matches!(first, RegisterRepositoryOutcome::Created(_)));
    assert!(matches!(second, RegisterRepositoryOutcome::Created(_)));
    assert_eq!(fixture.store.list_repositories().await.unwrap().len(), 2);
}

#[cfg(not(windows))]
fn uppercase_path(path: &CanonicalPath) -> CanonicalPath {
    CanonicalPath::try_from_canonical(PathBuf::from(path.to_string().to_uppercase())).unwrap()
}
