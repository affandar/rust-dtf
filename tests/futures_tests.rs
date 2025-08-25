use rust_dtf::providers::HistoryStore;
use rust_dtf::providers::fs::FsHistoryStore;
use rust_dtf::runtime::registry::ActivityRegistry;
use rust_dtf::runtime::{self};
use rust_dtf::{Event, OrchestrationContext, OrchestrationRegistry};
use std::sync::Arc as StdArc;

mod common;

#[tokio::test]
async fn select2_two_externals_history_order_wins_fs() {
    let td = tempfile::tempdir().unwrap();
    let store = StdArc::new(FsHistoryStore::new(td.path(), true)) as StdArc<dyn HistoryStore>;

    let orchestrator = |ctx: OrchestrationContext, _input: String| async move {
        let a = ctx.schedule_wait("A");
        let b = ctx.schedule_wait("B");
        let (idx, out) = ctx.select2(a, b).await;
        match (idx, out) {
            (0, rust_dtf::DurableOutput::External(v)) => Ok(format!("A:{v}")),
            (1, rust_dtf::DurableOutput::External(v)) => Ok(format!("B:{v}")),
            _ => unreachable!("select2 should return External outputs here"),
        }
    };

    let acts = ActivityRegistry::builder().build();
    let reg = OrchestrationRegistry::builder()
        .register("ABSelect2", orchestrator)
        .build();
    let rt1 = runtime::Runtime::start_with_store(store.clone(), StdArc::new(acts), reg).await;

    let _h = rt1
        .clone()
        .start_orchestration("inst-ab2", "ABSelect2", "")
        .await
        .unwrap();

    assert!(
        common::wait_for_history(
            store.clone(),
            "inst-ab2",
            |h| {
                let mut seen_a = false;
                let mut seen_b = false;
                for e in h.iter() {
                    if let Event::ExternalSubscribed { name, .. } = e {
                        if name == "A" {
                            seen_a = true;
                        }
                        if name == "B" {
                            seen_b = true;
                        }
                    }
                }
                seen_a && seen_b
            },
            3_000
        )
        .await,
        "timeout waiting for subscriptions"
    );
    rt1.shutdown().await;

    let wi_b = rust_dtf::providers::WorkItem::ExternalRaised {
        instance: "inst-ab2".to_string(),
        name: "B".to_string(),
        data: "vb".to_string(),
    };
    let wi_a = rust_dtf::providers::WorkItem::ExternalRaised {
        instance: "inst-ab2".to_string(),
        name: "A".to_string(),
        data: "va".to_string(),
    };
    let _ = store
        .enqueue_work(rust_dtf::providers::QueueKind::Orchestrator, wi_b)
        .await;
    let _ = store
        .enqueue_work(rust_dtf::providers::QueueKind::Orchestrator, wi_a)
        .await;

    let acts2 = ActivityRegistry::builder().build();
    let reg2 = OrchestrationRegistry::builder()
        .register("ABSelect2", orchestrator)
        .build();
    let rt2 = runtime::Runtime::start_with_store(store.clone(), StdArc::new(acts2), reg2).await;

    assert!(
        common::wait_for_history(
            store.clone(),
            "inst-ab2",
            |h| { h.iter().any(|e| matches!(e, Event::OrchestrationCompleted { .. })) },
            5_000
        )
        .await,
        "timeout waiting for completion"
    );
    let hist = store.read("inst-ab2").await;
    let output = match hist.last().unwrap() {
        Event::OrchestrationCompleted { output } => output.clone(),
        _ => String::new(),
    };
    let idx_b = hist
        .iter()
        .position(|e| matches!(e, Event::ExternalEvent { name, .. } if name == "B"))
        .unwrap();
    let idx_a = hist
        .iter()
        .position(|e| matches!(e, Event::ExternalEvent { name, .. } if name == "A"))
        .unwrap();
    assert!(idx_b < idx_a, "expected ExternalEvent B before A in history: {hist:#?}");
    assert!(output.starts_with("B:"), "expected B to win, got {output}");
    rt2.shutdown().await;
}

#[tokio::test]
async fn select_two_externals_history_order_wins_fs() {
    let td = tempfile::tempdir().unwrap();
    let store = StdArc::new(FsHistoryStore::new(td.path(), true)) as StdArc<dyn HistoryStore>;

    let orchestrator = |ctx: OrchestrationContext, _input: String| async move {
        let a = ctx.schedule_wait("A");
        let b = ctx.schedule_wait("B");
        let (idx, out) = ctx.select2(a, b).await;
        match (idx, out) {
            (0, rust_dtf::DurableOutput::External(v)) => Ok(format!("A:{v}")),
            (1, rust_dtf::DurableOutput::External(v)) => Ok(format!("B:{v}")),
            _ => unreachable!("select2 should return External outputs here"),
        }
    };

    let acts = ActivityRegistry::builder().build();
    let reg = OrchestrationRegistry::builder()
        .register("ABSelect", orchestrator)
        .build();
    let rt1 = runtime::Runtime::start_with_store(store.clone(), StdArc::new(acts), reg).await;

    let _h = rt1
        .clone()
        .start_orchestration("inst-ab", "ABSelect", "")
        .await
        .unwrap();

    assert!(
        common::wait_for_history(
            store.clone(),
            "inst-ab",
            |h| {
                let mut seen_a = false;
                let mut seen_b = false;
                for e in h.iter() {
                    if let Event::ExternalSubscribed { name, .. } = e {
                        if name == "A" {
                            seen_a = true;
                        }
                        if name == "B" {
                            seen_b = true;
                        }
                    }
                }
                seen_a && seen_b
            },
            3_000
        )
        .await,
        "timeout waiting for subscriptions"
    );
    rt1.shutdown().await;

    let wi_b = rust_dtf::providers::WorkItem::ExternalRaised {
        instance: "inst-ab".to_string(),
        name: "B".to_string(),
        data: "vb".to_string(),
    };
    let wi_a = rust_dtf::providers::WorkItem::ExternalRaised {
        instance: "inst-ab".to_string(),
        name: "A".to_string(),
        data: "va".to_string(),
    };
    let _ = store
        .enqueue_work(rust_dtf::providers::QueueKind::Orchestrator, wi_b)
        .await;
    let _ = store
        .enqueue_work(rust_dtf::providers::QueueKind::Orchestrator, wi_a)
        .await;

    let acts2 = ActivityRegistry::builder().build();
    let reg2 = OrchestrationRegistry::builder()
        .register("ABSelect", orchestrator)
        .build();
    let rt2 = runtime::Runtime::start_with_store(store.clone(), StdArc::new(acts2), reg2).await;

    assert!(
        common::wait_for_history(
            store.clone(),
            "inst-ab",
            |h| { h.iter().any(|e| matches!(e, Event::OrchestrationCompleted { .. })) },
            5_000
        )
        .await,
        "timeout waiting for completion"
    );
    let hist = store.read("inst-ab").await;
    let output = match hist.last().unwrap() {
        Event::OrchestrationCompleted { output } => output.clone(),
        _ => String::new(),
    };
    let idx_b = hist
        .iter()
        .position(|e| matches!(e, Event::ExternalEvent { name, .. } if name == "B"))
        .unwrap();
    let idx_a = hist
        .iter()
        .position(|e| matches!(e, Event::ExternalEvent { name, .. } if name == "A"))
        .unwrap();
    assert!(idx_b < idx_a, "expected ExternalEvent B before A in history: {hist:#?}");
    assert!(output.starts_with("B:"), "expected B to win, got {output}");
    rt2.shutdown().await;
}

#[tokio::test]
async fn select_three_mixed_history_winner_fs() {
    // A (external), T (timer), B (external): enqueue B first, then A; timer much later
    let td = tempfile::tempdir().unwrap();
    let store = StdArc::new(FsHistoryStore::new(td.path(), true)) as StdArc<dyn HistoryStore>;

    let orchestrator = |ctx: OrchestrationContext, _input: String| async move {
        let a = ctx.schedule_wait("A");
        let t = ctx.schedule_timer(500);
        let b = ctx.schedule_wait("B");
        let (idx, out) = ctx.select(vec![a, t, b]).await;
        match (idx, out) {
            (0, rust_dtf::DurableOutput::External(v)) => Ok(format!("A:{v}")),
            (1, rust_dtf::DurableOutput::Timer) => Ok("T".to_string()),
            (2, rust_dtf::DurableOutput::External(v)) => Ok(format!("B:{v}")),
            _ => unreachable!(),
        }
    };

    let acts = ActivityRegistry::builder().build();
    let reg = OrchestrationRegistry::builder()
        .register("ATBSelect", orchestrator)
        .build();
    let rt1 = runtime::Runtime::start_with_store(store.clone(), StdArc::new(acts), reg).await;

    let _h = rt1
        .clone()
        .start_orchestration("inst-atb", "ATBSelect", "")
        .await
        .unwrap();
    assert!(
        common::wait_for_history(
            store.clone(),
            "inst-atb",
            |h| {
                let mut seen_a = false;
                let mut seen_b = false;
                for e in h.iter() {
                    if let Event::ExternalSubscribed { name, .. } = e {
                        if name == "A" {
                            seen_a = true;
                        }
                        if name == "B" {
                            seen_b = true;
                        }
                    }
                }
                seen_a && seen_b
            },
            10_000
        )
        .await
    );
    rt1.shutdown().await;

    let wi_b = rust_dtf::providers::WorkItem::ExternalRaised {
        instance: "inst-atb".to_string(),
        name: "B".to_string(),
        data: "vb".to_string(),
    };
    let wi_a = rust_dtf::providers::WorkItem::ExternalRaised {
        instance: "inst-atb".to_string(),
        name: "A".to_string(),
        data: "va".to_string(),
    };
    let _ = store
        .enqueue_work(rust_dtf::providers::QueueKind::Orchestrator, wi_b)
        .await;
    let _ = store
        .enqueue_work(rust_dtf::providers::QueueKind::Orchestrator, wi_a)
        .await;

    let acts2 = ActivityRegistry::builder().build();
    let reg2 = OrchestrationRegistry::builder()
        .register("ATBSelect", orchestrator)
        .build();
    let rt2 = runtime::Runtime::start_with_store(store.clone(), StdArc::new(acts2), reg2).await;

    assert!(
        common::wait_for_history(
            store.clone(),
            "inst-atb",
            |h| { h.iter().any(|e| matches!(e, Event::OrchestrationCompleted { .. })) },
            5_000
        )
        .await
    );
    let hist = store.read("inst-atb").await;
    let output = match hist.last().unwrap() {
        Event::OrchestrationCompleted { output } => output.clone(),
        _ => String::new(),
    };
    let idx_b = hist
        .iter()
        .position(|e| matches!(e, Event::ExternalEvent { name, .. } if name == "B"))
        .unwrap();
    let idx_a = hist
        .iter()
        .position(|e| matches!(e, Event::ExternalEvent { name, .. } if name == "A"))
        .unwrap();
    assert!(idx_b < idx_a, "expected B before A in history: {hist:#?}");
    assert!(output.starts_with("B:"), "expected B to win, got {output}");
    rt2.shutdown().await;
}

#[tokio::test]
async fn join_returns_history_order_fs() {
    let td = tempfile::tempdir().unwrap();
    let store = StdArc::new(FsHistoryStore::new(td.path(), true)) as StdArc<dyn HistoryStore>;

    let orchestrator = |ctx: OrchestrationContext, _input: String| async move {
        let a = ctx.schedule_wait("A");
        let b = ctx.schedule_wait("B");
        let outs = ctx.join(vec![a, b]).await; // order should match history
        // Map outputs to a compact string
        let s: String = outs
            .into_iter()
            .map(|o| match o {
                rust_dtf::DurableOutput::External(v) => v,
                _ => String::new(),
            })
            .collect::<Vec<_>>()
            .join(",");
        Ok(s)
    };

    let acts = ActivityRegistry::builder().build();
    let reg = OrchestrationRegistry::builder()
        .register("JoinAB", orchestrator)
        .build();
    let rt1 = runtime::Runtime::start_with_store(store.clone(), StdArc::new(acts), reg).await;

    let _h = rt1
        .clone()
        .start_orchestration("inst-join", "JoinAB", "")
        .await
        .unwrap();
    assert!(
        common::wait_for_history(
            store.clone(),
            "inst-join",
            |h| {
                let mut seen_a = false;
                let mut seen_b = false;
                for e in h.iter() {
                    if let Event::ExternalSubscribed { name, .. } = e {
                        if name == "A" {
                            seen_a = true;
                        }
                        if name == "B" {
                            seen_b = true;
                        }
                    }
                }
                seen_a && seen_b
            },
            10_000
        )
        .await
    );
    rt1.shutdown().await;

    // Enqueue B then A so history order is B, then A
    let wi_b = rust_dtf::providers::WorkItem::ExternalRaised {
        instance: "inst-join".to_string(),
        name: "B".to_string(),
        data: "vb".to_string(),
    };
    let wi_a = rust_dtf::providers::WorkItem::ExternalRaised {
        instance: "inst-join".to_string(),
        name: "A".to_string(),
        data: "va".to_string(),
    };
    let _ = store
        .enqueue_work(rust_dtf::providers::QueueKind::Orchestrator, wi_b)
        .await;
    let _ = store
        .enqueue_work(rust_dtf::providers::QueueKind::Orchestrator, wi_a)
        .await;

    let acts2 = ActivityRegistry::builder().build();
    let reg2 = OrchestrationRegistry::builder()
        .register("JoinAB", orchestrator)
        .build();
    let rt2 = runtime::Runtime::start_with_store(store.clone(), StdArc::new(acts2), reg2).await;

    assert!(
        common::wait_for_history(
            store.clone(),
            "inst-join",
            |h| { h.iter().any(|e| matches!(e, Event::OrchestrationCompleted { .. })) },
            5_000
        )
        .await
    );
    let hist = store.read("inst-join").await;
    let output = match hist.last().unwrap() {
        Event::OrchestrationCompleted { output } => output.clone(),
        _ => String::new(),
    };
    // Ensure output is vb,va to reflect history order B before A
    assert_eq!(output, "vb,va");
    rt2.shutdown().await;
}
