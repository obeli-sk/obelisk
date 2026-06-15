use concepts::component_id::ComponentDigest;
use concepts::prefixed_ulid::DeploymentId;
use concepts::storage::{
    ComponentMetadataRecord, DbExternalApi, DbPoolCloseable, DeploymentComponentRecord,
    DeploymentRecord, DeploymentStatus, PersistedFunctionMetadata,
};
use concepts::time::ClockFn;
use concepts::{
    ComponentType, FunctionFqn, FunctionMetadata, ParameterTypes, RETURN_TYPE_DUMMY, StrVariant,
};
use obeli_db_tests::Database;
use rstest::rstest;
use test_db_macro::expand_enum_database;
use test_utils::set_up;
use test_utils::sim_clock::SimClock;

fn mk_function(ffqn: &str, submittable: bool) -> FunctionMetadata {
    FunctionMetadata {
        ffqn: ffqn.parse::<FunctionFqn>().unwrap(),
        parameter_types: ParameterTypes::default(),
        return_type: RETURN_TYPE_DUMMY,
        extension: None,
        submittable,
    }
}

fn mk_deployment_record(
    deployment_id: DeploymentId,
    now: chrono::DateTime<chrono::Utc>,
) -> DeploymentRecord {
    DeploymentRecord {
        deployment_id,
        description: None,
        digest: DeploymentRecord::compute_digest("{}"),
        created_at: now,
        last_active_at: None,
        status: DeploymentStatus::Inactive,
        config_json: "{}".to_string(),
        obelisk_version: "0.0.0-test".to_string(),
        created_by: None,
    }
}

async fn insert_deployment(
    api_conn: &dyn DbExternalApi,
    deployment_id: DeploymentId,
    now: chrono::DateTime<chrono::Utc>,
) {
    api_conn
        .insert_deployment(mk_deployment_record(deployment_id, now))
        .await
        .unwrap();
}

#[expand_enum_database]
#[rstest]
#[tokio::test]
async fn deployment_components_roundtrip(database: Database) {
    set_up();
    let sim_clock = SimClock::default();
    let (_guard, db_pool, db_close) = database.set_up().await;
    let api_conn = db_pool.external_api_conn().await.unwrap();

    let deployment_id = DeploymentId::generate();
    insert_deployment(api_conn.as_ref(), deployment_id, sim_clock.now()).await;

    let digest_a: ComponentDigest =
        "sha256:1111111111111111111111111111111111111111111111111111111111111111"
            .parse()
            .unwrap();
    let digest_b: ComponentDigest =
        "sha256:2222222222222222222222222222222222222222222222222222222222222222"
            .parse()
            .unwrap();

    api_conn
        .upsert_component_metadata(vec![
            ComponentMetadataRecord {
                component_digest: digest_a.clone(),
                imports: vec![PersistedFunctionMetadata::from(mk_function(
                    "testing:pkg/imported-a.fn",
                    false,
                ))],
                exports: vec![PersistedFunctionMetadata::from(mk_function(
                    "testing:pkg/exported-a.fn",
                    true,
                ))],
                wit: "package testing:pkg;".to_string(),
                wit_origin: "wasm".to_string(),
            },
            ComponentMetadataRecord {
                component_digest: digest_b.clone(),
                imports: vec![],
                exports: vec![PersistedFunctionMetadata::from(mk_function(
                    "testing:pkg/exported-b.fn",
                    true,
                ))],
                wit: "package testing:pkg-b;".to_string(),
                wit_origin: "synthesized".to_string(),
            },
        ])
        .await
        .unwrap();
    api_conn
        .insert_deployment_components(
            deployment_id,
            vec![
                DeploymentComponentRecord {
                    deployment_id,
                    component_name: StrVariant::from("b_workflow"),
                    component_digest: digest_b.clone(),
                    component_type: ComponentType::Workflow,
                },
                DeploymentComponentRecord {
                    deployment_id,
                    component_name: StrVariant::from("a_activity"),
                    component_digest: digest_a.clone(),
                    component_type: ComponentType::Activity,
                },
            ],
        )
        .await
        .unwrap();

    let components = api_conn
        .list_deployment_components(deployment_id)
        .await
        .unwrap();

    assert_eq!(2, components.len());
    assert_eq!(
        ComponentType::Activity,
        components[0].component_id.component_type
    );
    assert_eq!("a_activity", components[0].component_id.name.as_ref());
    assert_eq!(digest_a, components[0].component_id.component_digest);
    assert_eq!(1, components[0].imports.len());
    assert_eq!(1, components[0].exports.len());
    assert_eq!("package testing:pkg;", components[0].wit);

    assert_eq!(
        ComponentType::Workflow,
        components[1].component_id.component_type
    );
    assert_eq!("b_workflow", components[1].component_id.name.as_ref());
    assert_eq!(digest_b, components[1].component_id.component_digest);
    assert!(components[1].imports.is_empty());
    assert_eq!(1, components[1].exports.len());
    assert_eq!("package testing:pkg-b;", components[1].wit);

    drop(api_conn);
    db_close.close().await;
}

#[expand_enum_database]
#[rstest]
#[tokio::test]
async fn component_metadata_deduplicates_by_digest_across_deployments(database: Database) {
    set_up();
    let sim_clock = SimClock::default();
    let (_guard, db_pool, db_close) = database.set_up().await;
    let api_conn = db_pool.external_api_conn().await.unwrap();

    let dep_a = DeploymentId::generate();
    let dep_b = DeploymentId::generate();
    insert_deployment(api_conn.as_ref(), dep_a, sim_clock.now()).await;
    insert_deployment(api_conn.as_ref(), dep_b, sim_clock.now()).await;

    let digest: ComponentDigest =
        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            .parse()
            .unwrap();

    api_conn
        .upsert_component_metadata(vec![ComponentMetadataRecord {
            component_digest: digest.clone(),
            imports: vec![PersistedFunctionMetadata::from(mk_function(
                "testing:pkg/imported.fn",
                false,
            ))],
            exports: vec![PersistedFunctionMetadata::from(mk_function(
                "testing:pkg/exported.fn",
                true,
            ))],
            wit: "package testing:pkg;".to_string(),
            wit_origin: "wasm".to_string(),
        }])
        .await
        .unwrap();

    api_conn
        .insert_deployment_components(
            dep_a,
            vec![DeploymentComponentRecord {
                deployment_id: dep_a,
                component_name: StrVariant::from("component_a"),
                component_digest: digest.clone(),
                component_type: ComponentType::Activity,
            }],
        )
        .await
        .unwrap();
    api_conn
        .insert_deployment_components(
            dep_b,
            vec![DeploymentComponentRecord {
                deployment_id: dep_b,
                component_name: StrVariant::from("component_b"),
                component_digest: digest.clone(),
                component_type: ComponentType::Activity,
            }],
        )
        .await
        .unwrap();

    let a_components = api_conn.list_deployment_components(dep_a).await.unwrap();
    let b_components = api_conn.list_deployment_components(dep_b).await.unwrap();

    assert_eq!(1, a_components.len());
    assert_eq!(1, b_components.len());
    assert_eq!(digest, a_components[0].component_id.component_digest);
    assert_eq!(digest, b_components[0].component_id.component_digest);
    assert_eq!("component_a", a_components[0].component_id.name.as_ref());
    assert_eq!("component_b", b_components[0].component_id.name.as_ref());
    assert_eq!(1, a_components[0].exports.len());
    assert_eq!(1, b_components[0].exports.len());
    assert_eq!(
        a_components[0].exports[0].ffqn.to_string(),
        b_components[0].exports[0].ffqn.to_string()
    );
    assert_eq!(a_components[0].wit, b_components[0].wit);

    drop(api_conn);
    db_close.close().await;
}

#[expand_enum_database]
#[rstest]
#[tokio::test]
async fn deployment_component_insert_is_idempotent(database: Database) {
    set_up();
    let sim_clock = SimClock::default();
    let (_guard, db_pool, db_close) = database.set_up().await;
    let api_conn = db_pool.external_api_conn().await.unwrap();

    let deployment_id = DeploymentId::generate();
    insert_deployment(api_conn.as_ref(), deployment_id, sim_clock.now()).await;

    let digest: ComponentDigest =
        "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            .parse()
            .unwrap();
    api_conn
        .upsert_component_metadata(vec![ComponentMetadataRecord {
            component_digest: digest.clone(),
            imports: vec![],
            exports: vec![PersistedFunctionMetadata::from(mk_function(
                "testing:pkg/exported.fn",
                true,
            ))],
            wit: "package testing:pkg;".to_string(),
            wit_origin: "wasm".to_string(),
        }])
        .await
        .unwrap();

    let record = DeploymentComponentRecord {
        deployment_id,
        component_name: StrVariant::from("same_name"),
        component_digest: digest.clone(),
        component_type: ComponentType::Activity,
    };
    api_conn
        .insert_deployment_components(deployment_id, vec![record.clone()])
        .await
        .unwrap();
    api_conn
        .insert_deployment_components(deployment_id, vec![record])
        .await
        .unwrap();

    let components = api_conn
        .list_deployment_components(deployment_id)
        .await
        .unwrap();
    assert_eq!(1, components.len());
    assert_eq!("same_name", components[0].component_id.name.as_ref());

    drop(api_conn);
    db_close.close().await;
}

#[expand_enum_database]
#[rstest]
#[tokio::test]
async fn list_deployment_components_orders_by_type_then_name(database: Database) {
    set_up();
    let sim_clock = SimClock::default();
    let (_guard, db_pool, db_close) = database.set_up().await;
    let api_conn = db_pool.external_api_conn().await.unwrap();

    let deployment_id = DeploymentId::generate();
    insert_deployment(api_conn.as_ref(), deployment_id, sim_clock.now()).await;

    let digests = [
        (
            "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
            "z_activity",
            ComponentType::Activity,
        ),
        (
            "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
            "a_workflow",
            ComponentType::Workflow,
        ),
        (
            "sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
            "a_activity_stub",
            ComponentType::ActivityStub,
        ),
        (
            "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
            "b_activity",
            ComponentType::Activity,
        ),
    ];
    let metadata: Vec<_> = digests
        .iter()
        .map(|(digest, _, _)| ComponentMetadataRecord {
            component_digest: digest.parse().unwrap(),
            imports: vec![],
            exports: vec![],
            wit: format!("package {};", &digest[7..11]),
            wit_origin: "wasm".to_string(),
        })
        .collect();
    api_conn.upsert_component_metadata(metadata).await.unwrap();
    let records: Vec<_> = digests
        .into_iter()
        .map(|(digest, name, component_type)| DeploymentComponentRecord {
            deployment_id,
            component_name: StrVariant::from(name),
            component_digest: digest.parse().unwrap(),
            component_type,
        })
        .collect();
    api_conn
        .insert_deployment_components(deployment_id, records)
        .await
        .unwrap();

    let components = api_conn
        .list_deployment_components(deployment_id)
        .await
        .unwrap();
    let ordered: Vec<_> = components
        .into_iter()
        .map(|c| {
            (
                c.component_id.component_type.to_string(),
                c.component_id.name.to_string(),
            )
        })
        .collect();
    assert_eq!(
        vec![
            ("activity".to_string(), "b_activity".to_string()),
            ("activity".to_string(), "z_activity".to_string()),
            ("activity_stub".to_string(), "a_activity_stub".to_string()),
            ("workflow".to_string(), "a_workflow".to_string()),
        ],
        ordered
    );

    drop(api_conn);
    db_close.close().await;
}
