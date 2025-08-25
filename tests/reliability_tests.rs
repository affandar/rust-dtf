use rust_dtf::providers::HistoryStore;
use rust_dtf::providers::fs::FsHistoryStore;
use rust_dtf::providers::{QueueKind, WorkItem};
use rust_dtf::runtime::registry::ActivityRegistry;
use rust_dtf::runtime::{self};
use rust_dtf::{Event, OrchestrationContext, OrchestrationRegistry};
use std::sync::Arc as StdArc;
mod common;

#[tokio::test]
async fn external_duplicate_workitems_dedup_fs() {
    let td = tempfile::tempdir().unwrap();
    let store = StdArc::new(FsHistoryStore::new(td.path(), true)) as StdArc<dyn HistoryStore>;

    let orch = |ctx: OrchestrationContext, _input: String| async move {
        let v = ctx.schedule_wait("Evt").into_event().await;
        Ok(v)
    };
    let orchestration_registry = OrchestrationRegistry::builder().register("WaitEvt", orch).build();
    let activity_registry = ActivityRegistry::builder().build();
    let rt =
        runtime::Runtime::start_with_store(store.clone(), StdArc::new(activity_registry), orchestration_registry).await;

    let inst = "inst-ext-dup";
    let _h = rt.clone().start_orchestration(inst, "WaitEvt", "").await.unwrap();
    assert!(common::wait_for_subscription(store.clone(), inst, "Evt", 2_000).await);

    // enqueue duplicate externals
    let wi = WorkItem::ExternalRaised {
        instance: inst.to_string(),
        name: "Evt".to_string(),
        data: "ok".to_string(),
    };
    let _ = store.enqueue_work(QueueKind::Orchestrator, wi.clone()).await;
    let _ = store.enqueue_work(QueueKind::Orchestrator, wi.clone()).await;

    // wait for completion
    let ok = common::wait_for_history(
        store.clone(),
        inst,
        |h| {
            h.iter()
                .any(|e| matches!(e, Event::OrchestrationCompleted { output } if output == "ok"))
        },
        5_000,
    )
    .await;
    assert!(ok, "timeout waiting for completion");

    // exactly one ExternalEvent in history
    let hist = store.read(inst).await;
    let external_events: Vec<&Event> = hist
        .iter()
        .filter(|e| matches!(e, Event::ExternalEvent { name, .. } if name == "Evt"))
        .collect();
    assert_eq!(
        external_events.len(),
        1,
        "expected 1 ExternalEvent, got {}",
        external_events.len()
    );

    rt.shutdown().await;
}

#[tokio::test]
async fn timer_duplicate_workitems_dedup_fs() {
    let td = tempfile::tempdir().unwrap();
    let store = StdArc::new(FsHistoryStore::new(td.path(), true)) as StdArc<dyn HistoryStore>;

    let orch = |ctx: OrchestrationContext, _input: String| async move {
        ctx.schedule_timer(100).into_timer().await;
        Ok("t".to_string())
    };
    let orchestration_registry = OrchestrationRegistry::builder().register("OneTimer", orch).build();
    let activity_registry = ActivityRegistry::builder().build();
    let rt =
        runtime::Runtime::start_with_store(store.clone(), StdArc::new(activity_registry), orchestration_registry).await;

    let inst = "inst-timer-dup";
    let _h = rt.clone().start_orchestration(inst, "OneTimer", "").await.unwrap();

    // wait for TimerCreated and get id
    assert!(
        common::wait_for_history(
            store.clone(),
            inst,
            |h| h.iter().any(|e| matches!(e, Event::TimerCreated { .. })),
            2_000
        )
        .await
    );
    let (id, fire_at_ms) = {
        let hist = store.read(inst).await;
        let mut t_id = 0u64;
        let mut t_fire = 0u64;
        for e in hist.iter() {
            if let Event::TimerCreated { id, fire_at_ms } = e {
                t_id = *id;
                t_fire = *fire_at_ms;
                break;
            }
        }
        (t_id, t_fire)
    };

    // enqueue duplicate TimerFired for the same id
    let wi = WorkItem::TimerFired {
        instance: inst.to_string(),
        execution_id: 1,
        id,
        fire_at_ms,
    };
    let _ = store.enqueue_work(QueueKind::Orchestrator, wi.clone()).await;
    let _ = store.enqueue_work(QueueKind::Orchestrator, wi.clone()).await;

    // wait for completion
    let ok = common::wait_for_history(
        store.clone(),
        inst,
        |h| {
            h.iter()
                .any(|e| matches!(e, Event::OrchestrationCompleted { output } if output == "t"))
        },
        5_000,
    )
    .await;
    assert!(ok, "timeout waiting for completion");

    // exactly one TimerFired in history
    let hist = store.read(inst).await;
    let fired: Vec<&Event> = hist.iter().filter(|e| matches!(e, Event::TimerFired { .. })).collect();
    assert_eq!(fired.len(), 1, "expected 1 TimerFired, got {}", fired.len());

    rt.shutdown().await;
}

#[tokio::test]
async fn activity_duplicate_completion_workitems_dedup_fs() {
    let td = tempfile::tempdir().unwrap();
    let store = StdArc::new(FsHistoryStore::new(td.path(), true)) as StdArc<dyn HistoryStore>;

    // Activity sleeps to give us time to inject duplicates
    let activity_registry = ActivityRegistry::builder()
        .register("SlowEcho", |input: String| async move {
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            Ok(input)
        })
        .build();
    let orch = |ctx: OrchestrationContext, _input: String| async move {
        let out = ctx
            .schedule_activity("SlowEcho", "x".to_string())
            .into_activity()
            .await
            .unwrap();
        Ok(out)
    };
    let orchestration_registry = OrchestrationRegistry::builder().register("OneSlowAct", orch).build();
    let rt =
        runtime::Runtime::start_with_store(store.clone(), StdArc::new(activity_registry), orchestration_registry).await;

    let inst = "inst-act-dup";
    let _h = rt.clone().start_orchestration(inst, "OneSlowAct", "").await.unwrap();

    // wait for ActivityScheduled to get id
    assert!(
        common::wait_for_history(
            store.clone(),
            inst,
            |h| h
                .iter()
                .any(|e| matches!(e, Event::ActivityScheduled { name, .. } if name == "SlowEcho")),
            2_000
        )
        .await
    );
    let id = {
        let hist = store.read(inst).await;
        let mut t_id = 0u64;
        for e in hist.iter() {
            if let Event::ActivityScheduled { id, name, .. } = e
                && name == "SlowEcho" {
                    t_id = *id;
                    break;
                }
        }
        t_id
    };

    // enqueue duplicate ActivityCompleted with result matching the worker to avoid mismatches
    let wi = WorkItem::ActivityCompleted {
        instance: inst.to_string(),
        execution_id: 1,
        id,
        result: "x".to_string(),
    };
    let _ = store.enqueue_work(QueueKind::Orchestrator, wi.clone()).await;
    let _ = store.enqueue_work(QueueKind::Orchestrator, wi.clone()).await;

    // wait for completion and assert single ActivityCompleted recorded
    let ok = common::wait_for_history(
        store.clone(),
        inst,
        |h| {
            h.iter()
                .any(|e| matches!(e, Event::OrchestrationCompleted { output } if output == "x"))
        },
        5_000,
    )
    .await;
    assert!(ok, "timeout waiting for completion");

    let hist = store.read(inst).await;
    let acts: Vec<&Event> = hist
        .iter()
        .filter(|e| matches!(e, Event::ActivityCompleted { id: cid, .. } if *cid == id))
        .collect();
    assert_eq!(
        acts.len(),
        1,
        "expected 1 ActivityCompleted for id={}, got {}",
        id,
        acts.len()
    );

    rt.shutdown().await;
}
// merged file: imports above already declared; avoid reimporting

// Simulate crash windows by interleaving dequeue and persistence.
// We approximate by injecting duplicates around the same window; idempotence + peek-lock should ensure correctness.

#[tokio::test]
async fn crash_after_dequeue_before_append_completion_fs() {
    let td = tempfile::tempdir().unwrap();
    let store = StdArc::new(FsHistoryStore::new(td.path(), true)) as StdArc<dyn HistoryStore>;

    let orch = |ctx: OrchestrationContext, _input: String| async move {
        // Wait for external then complete with payload
        let v = ctx.schedule_wait("Evt").into_event().await;
        Ok(v)
    };
    let orchestration_registry = OrchestrationRegistry::builder().register("WaitEvt", orch).build();
    let activity_registry = ActivityRegistry::builder().build();
    let rt =
        runtime::Runtime::start_with_store(store.clone(), StdArc::new(activity_registry), orchestration_registry).await;

    // Start orchestration and wait for subscription
    let inst = "inst-crash-before-append";
    let _h = rt.clone().start_orchestration(inst, "WaitEvt", "").await.unwrap();
    assert!(common::wait_for_subscription(store.clone(), inst, "Evt", 2_000).await);

    // Enqueue the external work item
    let wi = WorkItem::ExternalRaised {
        instance: inst.to_string(),
        name: "Evt".to_string(),
        data: "ok".to_string(),
    };
    let _ = store.enqueue_work(QueueKind::Orchestrator, wi.clone()).await;
    // Simulate crash-before-append by enqueuing duplicate before runtime gets to append
    let _ = store.enqueue_work(QueueKind::Orchestrator, wi.clone()).await;

    // Wait for completion, ensure a single ExternalEvent recorded
    let ok = common::wait_for_history(
        store.clone(),
        inst,
        |h| {
            h.iter()
                .any(|e| matches!(e, Event::OrchestrationCompleted { output } if output == "ok"))
        },
        5_000,
    )
    .await;
    assert!(ok, "timeout waiting for completion");
    let hist = store.read(inst).await;
    let evs: Vec<&Event> = hist
        .iter()
        .filter(|e| matches!(e, Event::ExternalEvent { .. }))
        .collect();
    assert_eq!(evs.len(), 1);

    rt.shutdown().await;
}

#[tokio::test]
async fn crash_after_append_before_ack_timer_fs() {
    let td = tempfile::tempdir().unwrap();
    let store = StdArc::new(FsHistoryStore::new(td.path(), true)) as StdArc<dyn HistoryStore>;

    let orch = |ctx: OrchestrationContext, _input: String| async move {
        ctx.schedule_timer(50).into_timer().await;
        Ok("t".to_string())
    };
    let orchestration_registry = OrchestrationRegistry::builder().register("OneTimer", orch).build();
    let activity_registry = ActivityRegistry::builder().build();
    let rt =
        runtime::Runtime::start_with_store(store.clone(), StdArc::new(activity_registry), orchestration_registry).await;

    let inst = "inst-crash-after-append";
    let _h = rt.clone().start_orchestration(inst, "OneTimer", "").await.unwrap();

    assert!(
        common::wait_for_history(
            store.clone(),
            inst,
            |h| h.iter().any(|e| matches!(e, Event::TimerCreated { .. })),
            2_000
        )
        .await
    );
    // Get timer id
    let (id, fire_at_ms) = {
        let hist = store.read(inst).await;
        let mut t_id = 0u64;
        let mut t_fire = 0u64;
        for e in hist.iter() {
            if let Event::TimerCreated { id, fire_at_ms } = e {
                t_id = *id;
                t_fire = *fire_at_ms;
                break;
            }
        }
        (t_id, t_fire)
    };

    // Inject duplicate TimerFired simulating a crash after append-before-ack
    let wi = WorkItem::TimerFired {
        instance: inst.to_string(),
        execution_id: 1,
        id,
        fire_at_ms,
    };
    let _ = store.enqueue_work(QueueKind::Orchestrator, wi.clone()).await;
    let _ = store.enqueue_work(QueueKind::Orchestrator, wi.clone()).await;

    let ok = common::wait_for_history(
        store.clone(),
        inst,
        |h| {
            h.iter()
                .any(|e| matches!(e, Event::OrchestrationCompleted { output } if output == "t"))
        },
        5_000,
    )
    .await;
    assert!(ok, "timeout waiting for completion");
    let hist = store.read(inst).await;
    let fired: Vec<&Event> = hist.iter().filter(|e| matches!(e, Event::TimerFired { .. })).collect();
    assert_eq!(fired.len(), 1);

    rt.shutdown().await;
}
