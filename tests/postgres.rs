//! Run against a dedicated test database, never the collection database.
use et_data_service::{
    api::{router, Api},
    store::Store,
};
use market_history::{AggTrade, History, LiveSeries, Symbol};

fn trade(id: u64, time: u64) -> AggTrade {
    AggTrade {
        id,
        time,
        price: "0.00000123".parse().unwrap(),
        quantity: "0.1".parse().unwrap(),
        buyer_is_maker: id % 2 == 0,
    }
}

#[tokio::test]
#[ignore = "requires ET_DATA_TEST_DATABASE_URL pointing to a dedicated PostgreSQL database"]
async fn persistence_replay_rollup_pagination_and_http_handoff() {
    let url = std::env::var("ET_DATA_TEST_DATABASE_URL").expect("dedicated test database URL");
    let store = Store::connect(&url).await.unwrap();
    let key = format!("TEST{}USDT", et_data_service::now_ms());
    let symbol = Symbol {
        symbol: key.clone(),
        base_asset: "TEST".into(),
        quote_asset: "USDT".into(),
        price_precision: 8,
        volume_precision: 4,
        min_tick_size: "0.00000001".parse().unwrap(),
    };
    store
        .configure(&[(symbol, None)], "integration test")
        .await
        .unwrap();
    let batch = vec![trade(100, 1000), trade(101, 60_000), trade(102, 120_000)];
    store.apply(&key, &batch, 0).await.unwrap();
    store.apply(&key, &batch, 0).await.unwrap();
    let bars = store.history(&key, "5m", None, 240).await.unwrap();
    assert_eq!(bars.bars.len(), 1);
    assert_eq!(bars.bars[0].volume.to_string(), "0.3");
    assert_eq!(bars.last_aggregate_id, Some(102));
    // A gap must roll back the entire batch, including the valid first record.
    assert!(store
        .apply(&key, &[trade(103, 180_000), trade(105, 240_000)], 0)
        .await
        .is_err());
    assert_eq!(store.checkpoint(&key).await.unwrap(), Some(102));
    let page = store.history(&key, "1m", None, 2).await.unwrap();
    assert!(page.has_more);
    assert_eq!(page.bars[0].timestamp, 60_000);
    let older = store.history(&key, "1m", Some(60_000), 2).await.unwrap();
    assert_eq!(older.bars.len(), 1);
    assert_eq!(older.bars[0].timestamp, 0);
    assert!(!older.has_more);
    // Open a fresh pool: progress and exact footprint must survive a restart.
    let reopened = Store::connect(&url).await.unwrap();
    assert_eq!(reopened.checkpoint(&key).await.unwrap(), Some(102));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server_store = reopened.clone();
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            router(Api {
                store: server_store,
                token: Some("test-only".into()),
                depth: Default::default(),
            }),
        )
        .await
        .unwrap();
    });
    let client = reqwest::Client::new();
    assert_eq!(
        client
            .get(format!("http://{address}/v1/symbols"))
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    let query = format!("http://{address}/v1/history?symbol={key}&period=5m&through_id=103");
    let pending = tokio::spawn(async move {
        client
            .get(query)
            .bearer_auth("test-only")
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json::<History>()
            .await
            .unwrap()
    });
    tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    assert!(
        !pending.is_finished(),
        "snapshot must wait for its handoff ID"
    );
    store.apply(&key, &[trade(103, 180_000)], 0).await.unwrap();
    let snapshot = pending.await.unwrap();
    assert_eq!(snapshot.bars[0].volume.to_string(), "0.4");
    let mut live = LiveSeries::from_history(&snapshot).unwrap();
    assert!(!live.apply(&trade(103, 180_000)).unwrap());
    assert!(live.apply(&trade(104, 240_000)).unwrap());
    assert_eq!(live.current.unwrap().volume.to_string(), "0.5");
    // Disabling a symbol hides it from the catalog but preserves its history.
    sqlx::query("UPDATE symbols SET enabled=false WHERE symbol=$1")
        .bind(&key)
        .execute(&store.pool)
        .await
        .unwrap();
    assert!(!store
        .enabled()
        .await
        .unwrap()
        .iter()
        .any(|s| s.symbol == key));
    assert!(!store
        .history(&key, "1m", None, 240)
        .await
        .unwrap()
        .bars
        .is_empty());
    server.abort();
    // Remove only this test's uniquely named fixture.
    sqlx::query("DELETE FROM candles_1m WHERE symbol=$1")
        .bind(&key)
        .execute(&store.pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM collector_progress WHERE symbol=$1")
        .bind(&key)
        .execute(&store.pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM symbols WHERE symbol=$1")
        .bind(&key)
        .execute(&store.pool)
        .await
        .unwrap();
}
