use rust_dtf::providers::HistoryStore;
use rust_dtf::providers::fs::FsHistoryStore;
use rust_dtf::providers::in_memory::InMemoryHistoryStore;
use rust_dtf::runtime::registry::ActivityRegistry;
use rust_dtf::runtime::{self};
use rust_dtf::{Action, Event, OrchestrationContext, OrchestrationRegistry, run_turn};
use std::sync::Arc;

// Helper to create runtime with registries for tests
#[allow(dead_code)]
async fn create_test_runtime(activity_registry: ActivityRegistry) -> Arc<runtime::Runtime> {
    // Create a minimal orchestration registry for basic tests
    let orchestration_registry = OrchestrationRegistry::builder().build();
    runtime::Runtime::start(Arc::new(activity_registry), orchestration_registry).await
}

// 1) Single-turn emission: ensure exactly one action per scheduled future and matching schedule event recorded.
#[test]
fn action_emission_single_turn() {
    // Await the scheduled activity once so it is polled and records its action, then remain pending
    let orchestrator = |ctx: OrchestrationContext| async move {
        let _ = ctx.schedule_activity("A", "1").into_activity().await;
        unreachable!()
    };

    let history: Vec<Event> = Vec::new();
    let (hist_after, actions, _logs, out) = run_turn(history, orchestrator);
    assert!(out.is_none(), "must not complete in first turn");
    assert_eq!(actions.len(), 1, "exactly one action expected");
    match &actions[0] {
        Action::CallActivity { name, input, .. } => {
            assert_eq!(name, "A");
            assert_eq!(input, "1");
        }
        _ => panic!("unexpected action kind"),
    }
    // History should already contain ActivityScheduled
    assert!(matches!(hist_after[0], Event::ActivityScheduled { .. }));
}

// 2) Correlation: out-of-order completion in history still resolves the correct future by id.
#[test]
fn correlation_out_of_order_completion() {
    // Prepare history: schedule A(1), then an unrelated timer, then the activity completion
    let history = vec![
        Event::ActivityScheduled {
            id: 1,
            name: "A".into(),
            input: "1".into(),
        },
        Event::TimerFired { id: 42, fire_at_ms: 0 },
        Event::ActivityCompleted {
            id: 1,
            result: "ok".into(),
        },
    ];

    let orchestrator = |ctx: OrchestrationContext| async move { ctx.schedule_activity("A", "1").into_activity().await };

    let (_hist_after, actions, _logs, out) = run_turn(history, orchestrator);
    assert!(
        actions.is_empty(),
        "should resolve from existing completion, no new actions"
    );
    assert_eq!(out.unwrap(), Ok("ok".to_string()));
}

// 3) Deterministic replay on a tiny flow (activity only)
#[tokio::test]
async fn deterministic_replay_activity_only() {
    let orchestrator = |ctx: OrchestrationContext| async move {
        let a = ctx.schedule_activity("A", "2").into_activity().await.unwrap();
        format!("a={a}")
    };

    let activity_registry = ActivityRegistry::builder()
        .register("A", |input: String| async move {
            Ok(input.parse::<i32>().unwrap_or(0).saturating_add(1).to_string())
        })
        .build();

    let orchestration_registry = OrchestrationRegistry::builder()
        .register("TestOrchestration", move |ctx, _input| async move {
            Ok(orchestrator(ctx).await)
        })
        .build();

    let rt = runtime::Runtime::start(Arc::new(activity_registry), orchestration_registry).await;
    let h = rt
        .clone()
        .start_orchestration("inst-unit-1", "TestOrchestration", "")
        .await;
    let (final_history, output) = h.unwrap().await.unwrap();
    assert_eq!(output.as_ref().unwrap(), "a=3");

    // Replay must produce same output and no new actions
    let (_h2, acts2, _logs2, out2) = run_turn(final_history.clone(), orchestrator);
    assert!(acts2.is_empty());
    assert_eq!(out2.unwrap(), output.clone().unwrap());
    rt.shutdown().await;
}

// 4) HistoryStore admin APIs (in-memory)
#[tokio::test]
async fn history_store_admin_apis() {
    let tmp = tempfile::tempdir().unwrap();
    let store = FsHistoryStore::new(tmp.path(), true);
    store.create_instance("i1").await.unwrap();
    store.create_instance("i2").await.unwrap();
    store
        .append("i1", vec![Event::TimerCreated { id: 1, fire_at_ms: 10 }])
        .await
        .unwrap();
    store
        .append(
            "i2",
            vec![Event::ExternalSubscribed {
                id: 1,
                name: "Go".into(),
            }],
        )
        .await
        .unwrap();
    let instances = store.list_instances().await;
    assert!(instances.contains(&"i1".into()) && instances.contains(&"i2".into()));
    let dump = store.dump_all_pretty().await;
    assert!(dump.contains("instance=i1") && dump.contains("instance=i2"));
    store.reset().await;
    assert!(store.list_instances().await.is_empty());
}

#[tokio::test]
async fn providers_create_remove_and_duplicate_checks() {
    // In-memory provider create/remove
    let mem = InMemoryHistoryStore::default();
    mem.create_instance("dup").await.unwrap();
    // Duplicate create should error
    assert!(mem.create_instance("dup").await.is_err());
    // Append to non-existent should fail
    assert!(mem.append("missing", vec![]).await.is_err());
    // Remove existing ok; remove missing should error
    mem.remove_instance("dup").await.unwrap();
    assert!(mem.remove_instance("dup").await.is_err());

    // Filesystem provider create/remove
    let tmp = tempfile::tempdir().unwrap();
    let fs = rust_dtf::providers::fs::FsHistoryStore::new(tmp.path(), true);
    fs.create_instance("i1").await.unwrap();
    assert!(fs.create_instance("i1").await.is_err());
    assert!(fs.append("missing", vec![]).await.is_err());
    fs.remove_instance("i1").await.unwrap();
    assert!(fs.remove_instance("i1").await.is_err());
}

#[tokio::test]
async fn runtime_duplicate_orchestration_deduped_single_execution() {
    // Start runtime and attempt to start the same instance twice concurrently
    let activity_registry = ActivityRegistry::builder().build();

    let orchestration_registry = OrchestrationRegistry::builder()
        .register("TestOrch", |ctx, _| async move {
            // Slow a bit to allow duplicate enqueue to happen
            ctx.schedule_timer(20).into_timer().await;
            Ok("ok".to_string())
        })
        .build();

    let rt = runtime::Runtime::start(Arc::new(activity_registry), orchestration_registry).await;
    let inst = "dup-orch";

    // Fire two start requests for the same instance
    let h1 = rt.clone().start_orchestration(inst, "TestOrch", "").await.unwrap();
    let h2 = rt.clone().start_orchestration(inst, "TestOrch", "").await.unwrap();

    // Both handles should resolve to the same single execution/result
    let (hist1, out1) = h1.await.unwrap();
    let (hist2, out2) = h2.await.unwrap();
    assert_eq!(out1.as_ref().unwrap(), "ok");
    assert_eq!(out2.as_ref().unwrap(), "ok");

    // Ensure there is only one terminal event in history
    let term_count = hist1
        .iter()
        .filter(|e| {
            matches!(
                e,
                Event::OrchestrationCompleted { .. } | Event::OrchestrationFailed { .. }
            )
        })
        .count();
    assert_eq!(term_count, 1, "should have exactly one terminal event");

    // Second handle observed the same execution: same length and only one start/terminal
    assert_eq!(hist1.len(), hist2.len());
    let started_count = hist1
        .iter()
        .filter(|e| matches!(e, Event::OrchestrationStarted { .. }))
        .count();
    assert_eq!(started_count, 1);

    rt.shutdown().await;
}

#[tokio::test]
async fn orchestration_descriptor_root_and_child() {
    // Root orchestrations
    let activity_registry = ActivityRegistry::builder().build();
    let parent = |ctx: OrchestrationContext, _| async move {
        let _ = ctx
            .schedule_sub_orchestration("ChildDsc", "x")
            .into_sub_orchestration()
            .await;
        Ok("done".into())
    };
    let child = |_ctx: OrchestrationContext, _input: String| async move { Ok("child".into()) };
    let reg = OrchestrationRegistry::builder()
        .register("ParentDsc", parent)
        .register("ChildDsc", child)
        .build();
    let rt = runtime::Runtime::start(Arc::new(activity_registry), reg).await;
    let _h = rt
        .clone()
        .start_orchestration("inst-desc", "ParentDsc", "seed")
        .await
        .unwrap();
    // wait for completion
    let _ = rt
        .wait_for_orchestration("inst-desc", std::time::Duration::from_secs(2))
        .await;
    // Root descriptor
    let d = rt.get_orchestration_descriptor("inst-desc").await.unwrap();
    assert_eq!(d.name, "ParentDsc");
    assert!(!d.version.is_empty());
    assert!(d.parent_instance.is_none());
    assert!(d.parent_id.is_none());
    // Child descriptor
    let dchild = rt.get_orchestration_descriptor("inst-desc::sub::1").await.unwrap();
    assert_eq!(dchild.name, "ChildDsc");
    assert!(!dchild.version.is_empty());
    assert_eq!(dchild.parent_instance.as_deref(), Some("inst-desc"));
    assert_eq!(dchild.parent_id, Some(1));
    rt.shutdown().await;
}

#[tokio::test]
async fn orchestration_status_apis() {
    use rust_dtf::OrchestrationStatus;

    // Registry with two orchestrations: one completes after a short timer, one fails immediately
    let activity_registry = ActivityRegistry::builder().build();
    let orchestration_registry = OrchestrationRegistry::builder()
        .register("ShortTimer", |ctx, _| async move {
            ctx.schedule_timer(10).into_timer().await;
            Ok("ok".to_string())
        })
        .register("AlwaysFails", |_ctx, _| async move { Err("boom".to_string()) })
        .build();

    let rt = runtime::Runtime::start(Arc::new(activity_registry), orchestration_registry).await;

    // NotFound for unknown instance
    let s = rt.get_orchestration_status("no-such").await;
    assert!(matches!(s, OrchestrationStatus::NotFound));

    // Start a running orchestration; should be Running immediately
    let inst_running = "inst-status-running";
    let handle_running = rt
        .clone()
        .start_orchestration(inst_running, "ShortTimer", "")
        .await
        .unwrap();
    let s1 = rt.get_orchestration_status(inst_running).await;
    assert!(matches!(s1, OrchestrationStatus::Running));

    // After completion, should be Completed with output
    let (_h, out) = handle_running.await.unwrap();
    assert_eq!(out.as_ref().unwrap(), "ok");
    let s2 = rt.get_orchestration_status(inst_running).await;
    assert!(matches!(s2, OrchestrationStatus::Completed { .. }));
    if let OrchestrationStatus::Completed { output } = s2 {
        assert_eq!(output, "ok");
    }

    // Failed orchestration
    let inst_fail = "inst-status-fail";
    let handle_fail = rt
        .clone()
        .start_orchestration(inst_fail, "AlwaysFails", "")
        .await
        .unwrap();
    let (_h2, out2) = handle_fail.await.unwrap();
    assert!(out2.is_err());
    let s3 = rt.get_orchestration_status(inst_fail).await;
    assert!(matches!(s3, OrchestrationStatus::Failed { .. }));
    if let OrchestrationStatus::Failed { error } = s3 {
        assert_eq!(error, "boom");
    }

    rt.shutdown().await;
}

// Providers: filesystem multi-execution persistence and latest read() contract
#[tokio::test]
async fn providers_fs_multi_execution_persistence_and_latest_read() {
    let tmp = tempfile::tempdir().unwrap();
    let fs = FsHistoryStore::new(tmp.path(), true);

    // Create instance and write execution #1 with CAN terminal
    fs.create_instance("pfs").await.unwrap();
    fs.append_with_execution(
        "pfs",
        1,
        vec![
            Event::OrchestrationStarted {
                name: "O".into(),
                version: "0.0.0".into(),
                input: "0".into(),
                parent_instance: None,
                parent_id: None,
            },
            Event::OrchestrationContinuedAsNew { input: "1".into() },
        ],
    )
    .await
    .unwrap();
    let e1_before = fs.read_with_execution("pfs", 1).await;

    // Create execution #2 via reset_for_continue_as_new; complete it
    let _eid2 = fs
        .create_new_execution("pfs", "O", "0.0.0", "1", None, None)
        .await
        .unwrap();
    fs.append_with_execution("pfs", 2, vec![Event::OrchestrationCompleted { output: "ok".into() }])
        .await
        .unwrap();

    // Execution list must contain both
    let execs = fs.list_executions("pfs").await;
    assert_eq!(execs, vec![1, 2]);

    // Older execution history remains unchanged
    let e1_after = fs.read_with_execution("pfs", 1).await;
    assert_eq!(e1_before, e1_after);

    // Latest read() equals latest execution history
    let latest_hist = fs.read_with_execution("pfs", 2).await;
    let current_hist = fs.read("pfs").await;
    assert_eq!(current_hist, latest_hist);
}

// Providers: in-memory multi-execution persistence and latest read() contract
#[tokio::test]
async fn providers_inmem_multi_execution_persistence_and_latest_read() {
    let mem = InMemoryHistoryStore::default();

    mem.create_instance("pmem").await.unwrap();
    mem.append_with_execution(
        "pmem",
        1,
        vec![
            Event::OrchestrationStarted {
                name: "O".into(),
                version: "0.0.0".into(),
                input: "0".into(),
                parent_instance: None,
                parent_id: None,
            },
            Event::OrchestrationContinuedAsNew { input: "1".into() },
        ],
    )
    .await
    .unwrap();
    let e1_before = mem.read_with_execution("pmem", 1).await;

    let _eid2 = mem
        .create_new_execution("pmem", "O", "0.0.0", "1", None, None)
        .await
        .unwrap();
    mem.append_with_execution("pmem", 2, vec![Event::OrchestrationCompleted { output: "ok".into() }])
        .await
        .unwrap();

    let execs = mem.list_executions("pmem").await;
    assert_eq!(execs, vec![1, 2]);

    let e1_after = mem.read_with_execution("pmem", 1).await;
    assert_eq!(e1_before, e1_after);

    let latest_hist = mem.read_with_execution("pmem", 2).await;
    let current_hist = mem.read("pmem").await;
    assert_eq!(current_hist, latest_hist);
}
