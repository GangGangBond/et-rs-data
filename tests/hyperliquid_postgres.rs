use et_data_service::{
    hyperliquid::{evidence, save_snapshot, save_trades, selection, Trade},
    now_ms,
    store::Store,
};
use serde_json::json;

#[tokio::test]
#[ignore = "requires a dedicated PostgreSQL test database"]
async fn quality_ranking_uses_complete_cycles_and_keeps_inactive_unknown_holders_for_exploration() {
    let store = Store::connect(&std::env::var("ET_DATA_TEST_DATABASE_URL").unwrap())
        .await
        .unwrap();
    let now = now_ms() as i64;
    let coin = format!("QUALITY{now}");
    sqlx::query("INSERT INTO hl_symbols(coin,spot_symbol,context,context_ms) VALUES($1,$2,$3,$4)")
        .bind(&coin)
        .bind(format!("{coin}USDT"))
        .bind(sqlx::types::Json(json!({"markPx":"10000"})))
        .bind(now)
        .execute(&store.pool)
        .await
        .unwrap();
    let users: Vec<_> = (20..24).map(|i| format!("0x{:040x}", now + i)).collect();
    // Stable 20/30, one lucky 1/1, unknown, and excellent but sub-threshold inventory.
    for (i, user) in users.iter().enumerate() {
        sqlx::query("INSERT INTO hl_addresses(address,first_seen_ms,last_seen_ms,attempted_ms) VALUES($1,$2,$2,$3)")
            .bind(user).bind(now - 5 * 86_400_000).bind(if i == 2 { None } else { Some(now) })
            .execute(&store.pool).await.unwrap();
        let quantity = ["2", "100", "3", "0.1"][i];
        save_snapshot(&store, user, json!({"time":now,"assetPositions":[{"position":{"coin":coin,"szi":quantity,"entryPx":"10000","liquidationPx":"9000","marginUsed":"1000","leverage":{"type":"cross","value":10}}}]}), now as u64).await.unwrap();
        let (wins, total) = [(20, 30), (1, 1), (0, 0), (30, 30)][i];
        let mut tx = store.pool.begin().await.unwrap();
        for n in 0..total {
            for close in [false, true] {
                let tid = n * 2 + i64::from(close) + 1;
                let time = now - 86_400_000 + tid * 1000;
                let fill = json!({"address":user,"coin":coin,"time":time,"tid":tid,"side":if close {"A"} else {"B"},"px":"10000","sz":"1","startPosition":if close {"1"} else {"0"},"closedPnl":if close {if n < wins {"30"} else {"-30"}} else {"0"},"fee":"1","feeToken":"USDC","dir":if close {"Close Long"} else {"Open Long"}});
                sqlx::query(
                    "INSERT INTO hl_fills(address,coin,time_ms,tid,fill) VALUES($1,$2,$3,$4,$5)",
                )
                .bind(user)
                .bind(&coin)
                .bind(time)
                .bind(tid)
                .bind(sqlx::types::Json(fill))
                .execute(&mut *tx)
                .await
                .unwrap();
            }
        }
        // A profitable close with no witnessed opening must never raise the win rate.
        let missing = json!({"address":user,"coin":coin,"time":now-1000,"tid":999,"side":"A","px":"10000","sz":"1","startPosition":"1","closedPnl":"1000","fee":"1","feeToken":"USDC","dir":"Close Long"});
        sqlx::query("INSERT INTO hl_fills(address,coin,time_ms,tid,fill) VALUES($1,$2,$3,999,$4)")
            .bind(user)
            .bind(&coin)
            .bind(now - 1000)
            .bind(sqlx::types::Json(missing))
            .execute(&mut *tx)
            .await
            .unwrap();
        sqlx::query("INSERT INTO hl_fill_sync(address,next_ms,updated_ms) VALUES($1,$2,$2)")
            .bind(user)
            .bind(now)
            .execute(&mut *tx)
            .await
            .unwrap();
        selection::refresh_performance(&mut tx, user, now)
            .await
            .unwrap();
        selection::refresh_performance(&mut tx, user, now)
            .await
            .unwrap();
        tx.commit().await.unwrap();
    }
    let counts: (i64, i64) = sqlx::query_as(
        "SELECT count(*),count(*) FILTER(WHERE pnl>0) FROM hl_closed_cycles WHERE address=$1",
    )
    .bind(&users[0])
    .fetch_one(&store.pool)
    .await
    .unwrap();
    assert_eq!(counts, (30, 20));
    selection::refresh(&store, 1, 10000.0).await.unwrap();
    let selected: Vec<String> =
        sqlx::query_scalar("SELECT address FROM hl_address_selection WHERE selected")
            .fetch_all(&store.pool)
            .await
            .unwrap();
    assert_eq!(selected, vec![users[0].clone()]);
    selection::refresh(&store, 2, 10000.0).await.unwrap();
    let selected: Vec<String> = sqlx::query_scalar(
        "SELECT address FROM hl_address_selection WHERE selected ORDER BY address",
    )
    .fetch_all(&store.pool)
    .await
    .unwrap();
    assert_eq!(selected, vec![users[0].clone(), users[2].clone()]);
    // Cycles leaving the actual 30-day window cease to affect ranking.
    sqlx::query("UPDATE hl_closed_cycles SET time_ms=$2 WHERE address=$1")
        .bind(&users[0])
        .bind(now - 31 * 86_400_000)
        .execute(&store.pool)
        .await
        .unwrap();
    selection::refresh(&store, 1, 10000.0).await.unwrap();
    let selected: Vec<String> =
        sqlx::query_scalar("SELECT address FROM hl_address_selection WHERE selected")
            .fetch_all(&store.pool)
            .await
            .unwrap();
    assert_eq!(selected, vec![users[1].clone()]);
    sqlx::query("DELETE FROM hl_addresses WHERE address=ANY($1)")
        .bind(&users)
        .execute(&store.pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM hl_symbols WHERE coin=$1")
        .bind(&coin)
        .execute(&store.pool)
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires ET_DATA_TEST_DATABASE_URL pointing to a dedicated PostgreSQL database"]
async fn confirmed_positions_survive_age_and_clear_only_on_new_account_state() {
    let url = std::env::var("ET_DATA_TEST_DATABASE_URL").expect("dedicated test database");
    let store = Store::connect(&url).await.unwrap();
    let now = now_ms();
    let coin = format!("TESTHL{now}");
    let symbol = format!("{coin}USDT");
    let buyer = format!("0x{:040x}", now);
    let seller = format!("0x{:040x}", now + 1);
    sqlx::query("UPDATE hl_status SET enabled=true,connected=true WHERE id=1")
        .execute(&store.pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO hl_symbols(coin,spot_symbol,context,context_ms) VALUES($1,$2,$3,$4)")
        .bind(&coin)
        .bind(&symbol)
        .bind(sqlx::types::Json(
            json!({"markPx":"100","openInterest":"1000","funding":"0.0001"}),
        ))
        .bind(now as i64)
        .execute(&store.pool)
        .await
        .unwrap();
    let trade:Trade=serde_json::from_value(json!({"coin":coin,"time":now,"tid":1,"side":"A","px":"100.001","sz":"0.12345678","users":[buyer,seller]})).unwrap();
    save_trades(&store, &[trade.clone(), trade.clone()])
        .await
        .unwrap();
    save_trades(&store, &[trade]).await.unwrap();
    let state = |quantity: &str, liquidation: serde_json::Value| json!({"time":now,"assetPositions":[{"position":{"coin":coin,"szi":quantity,"entryPx":"100.5","liquidationPx":liquidation,"marginUsed":"10","leverage":{"type":"cross","value":10}}}]});
    save_snapshot(&store, &buyer, state("2", json!(null)), now)
        .await
        .unwrap();
    save_snapshot(&store, &seller, state("-3", json!("150")), now)
        .await
        .unwrap();
    selection::refresh(&store, 256, 0.0).await.unwrap();
    let activity_count: i64 = sqlx::query_scalar(
        "SELECT sum(trades)::bigint FROM hl_address_activity_1m WHERE address=$1",
    )
    .bind(&buyer)
    .fetch_one(&store.pool)
    .await
    .unwrap();
    assert_eq!(activity_count, 1);
    // Persisted address fills are separate from public trades and survive flat snapshots.
    for (user, pnl) in [(&buyer, "10"), (&seller, "-5")] {
        for (tid, close) in [(1, false), (2, true)] {
            let raw = json!({"address":user,"coin":coin,"time":now as i64-3_600_000+tid,"tid":tid,"side":if close {"A"}else{"B"},"px":"100","sz":"1","startPosition":if close {"1"}else{"0"},"closedPnl":if close {pnl}else{"0"},"fee":"1","feeToken":"USDC","dir":if close {"Close Long"}else{"Open Long"}});
            sqlx::query(
                "INSERT INTO hl_fills(address,coin,time_ms,tid,fill) VALUES($1,$2,$3,$4,$5)",
            )
            .bind(user)
            .bind(&coin)
            .bind(now as i64 - 3_600_000 + tid)
            .bind(tid)
            .bind(sqlx::types::Json(raw))
            .execute(&store.pool)
            .await
            .unwrap();
        }
        sqlx::query("INSERT INTO hl_fill_sync(address,next_ms,updated_ms) VALUES($1,$2,$2)")
            .bind(user)
            .bind(now as i64)
            .execute(&store.pool)
            .await
            .unwrap();
    }
    let result = evidence(&store, &symbol).await.unwrap();
    assert_eq!(result.positions.len(), 2);
    assert_eq!(result.history.fills.len(), 4);
    assert_eq!(result.history.sampled_addresses, 2);
    let summary = et_data_service::hyperliquid::evidence_with_details(&store, &symbol, false)
        .await
        .unwrap();
    assert_eq!(summary.largest_positions.len(), 2);
    assert!(summary.positions.is_empty() && summary.history.fills.is_empty());
    let cycles = market_history::hyperliquid::closed_trades(&result.history.fills);
    assert_eq!(cycles.len(), 2);
    assert_eq!(cycles.iter().filter(|t| t.pnl > 0.0).count(), 1);
    assert_eq!(result.activity.trade_count, 1);
    assert_eq!(result.activity.sell_quantity, 0.12345678);
    assert_eq!(result.activity.buy_quantity, 0.0);
    assert_eq!(result.discovered_addresses, 2);
    assert_eq!(result.fresh_addresses, 2);
    assert_eq!(result.long_quantity, 2.0);
    assert_eq!(result.short_quantity, 3.0);
    assert_eq!(result.missing_liquidation_positions, 1);
    // A stale payload cannot replace an observed valid position.
    let bad = json!({"time":now-120_000,"assetPositions":[]});
    assert!(save_snapshot(&store, &buyer, bad, now).await.is_err());
    assert_eq!(evidence(&store, &symbol).await.unwrap().long_quantity, 2.0);
    // Flat account state removes the old position and records an actual empty snapshot.
    save_snapshot(
        &store,
        &buyer,
        json!({"time":now,"assetPositions":[]}),
        now + 1,
    )
    .await
    .unwrap();
    assert_eq!(evidence(&store, &symbol).await.unwrap().long_quantity, 0.0);
    sqlx::query("UPDATE hl_positions SET observed_ms=$2,exchange_ms=$2 WHERE address=$1")
        .bind(&seller)
        .bind(now as i64 - 901_000)
        .execute(&store.pool)
        .await
        .unwrap();
    sqlx::query("UPDATE hl_addresses SET snapshot_ms=$2,exchange_ms=$2 WHERE address=$1")
        .bind(&seller)
        .bind(now as i64 - 901_000)
        .execute(&store.pool)
        .await
        .unwrap();
    let stale = evidence(&store, &symbol).await.unwrap();
    assert_eq!(stale.short_quantity, 3.0);
    assert_eq!(stale.stale_addresses, 1);
    assert_eq!(stale.positions.len(), 1);
    assert_eq!(stale.positions[0].observed_ms, now as i64 - 901_000);
    assert!(!stale.liquidation_buckets.is_empty());
    sqlx::query("UPDATE hl_addresses SET snapshot_ms=$2,exchange_ms=$2 WHERE address=$1")
        .bind(&buyer)
        .bind(now as i64 - 901_000)
        .execute(&store.pool)
        .await
        .unwrap();
    // Losing market context freshness must not erase the last confirmed distribution.
    sqlx::query("UPDATE hl_symbols SET context_ms=$2 WHERE coin=$1")
        .bind(&coin)
        .bind(now as i64 - 901_000)
        .execute(&store.pool)
        .await
        .unwrap();
    let delayed = evidence(&store, &symbol).await.unwrap();
    assert_eq!(delayed.short_quantity, 3.0);
    assert!(delayed.mark_price.is_none());
    assert!(!delayed.liquidation_buckets.is_empty());
    assert_eq!(delayed.fresh_addresses, 0);
    assert_eq!(delayed.stale_addresses, 2);
    assert_eq!(delayed.oldest_snapshot_ms, Some(now as i64 - 901_000));
    assert_eq!(delayed.newest_snapshot_ms, Some(now as i64 - 901_000));
    save_snapshot(&store, &seller, state("-1", json!("150")), now + 1)
        .await
        .unwrap();
    assert_eq!(evidence(&store, &symbol).await.unwrap().short_quantity, 1.0);
    // A newer explicit flat snapshot, unlike elapsed time, removes the position.
    save_snapshot(
        &store,
        &seller,
        json!({"time":now,"assetPositions":[]}),
        now + 2,
    )
    .await
    .unwrap();
    let flat = evidence(&store, &symbol).await.unwrap();
    assert_eq!(flat.short_quantity, 0.0);
    assert!(flat.positions.is_empty());
    assert!(flat.liquidation_buckets.is_empty());
    assert!(!evidence(&store, "UNSUPPORTEDUSDT").await.unwrap().supported);
    sqlx::query("DELETE FROM hl_trades WHERE coin=$1")
        .bind(&coin)
        .execute(&store.pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM hl_addresses WHERE address IN ($1,$2)")
        .bind(&buyer)
        .bind(&seller)
        .execute(&store.pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM hl_symbols WHERE coin=$1")
        .bind(&coin)
        .execute(&store.pool)
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires a dedicated PostgreSQL test database"]
async fn selection_excludes_two_sided_makers_but_keeps_inactive_large_holdings() {
    let store = Store::connect(&std::env::var("ET_DATA_TEST_DATABASE_URL").unwrap())
        .await
        .unwrap();
    let now = now_ms();
    let coin = format!("SEL{now}");
    let symbol = format!("{coin}USDT");
    let second = format!("ALT{now}");
    let maker = format!("0x{:040x}", now + 10);
    let other = format!("0x{:040x}", now + 11);
    let holder = format!("0x{:040x}", now + 12);
    let alt_holder = format!("0x{:040x}", now + 13);
    for c in [&coin, &second] {
        sqlx::query(
            "INSERT INTO hl_symbols(coin,spot_symbol,context,context_ms) VALUES($1,$2,$3,$4)",
        )
        .bind(c)
        .bind(format!("{c}USDT"))
        .bind(sqlx::types::Json(json!({"markPx":"10000"})))
        .bind(now as i64)
        .execute(&store.pool)
        .await
        .unwrap();
    }
    let trades:Vec<Trade>=(0..600).map(|i|{
        let buy=i%2==0;
        serde_json::from_value(json!({"coin":coin,"time":now-800_000+i*1000,"tid":i+1,"side":if buy{"A"}else{"B"},"px":"10000","sz":"10","users":if buy{[maker.clone(),other.clone()]}else{[other.clone(),maker.clone()]}})).unwrap()
    }).collect();
    save_trades(&store, &trades).await.unwrap();
    save_trades(&store, &trades).await.unwrap();
    for (user, c, quantity) in [
        (&maker, &coin, "1"),
        (&holder, &coin, "20"),
        (&alt_holder, &second, "10"),
    ] {
        // The large holders last traded days ago: they must still be selected.
        sqlx::query("INSERT INTO hl_addresses(address,first_seen_ms,last_seen_ms) VALUES($1,$2,$2) ON CONFLICT DO NOTHING").bind(user).bind(now as i64-5*86_400_000).execute(&store.pool).await.unwrap();
        save_snapshot(&store,user,json!({"time":now,"assetPositions":[{"position":{"coin":c,"szi":quantity,"entryPx":"10000","liquidationPx":"9000","marginUsed":"1000","leverage":{"type":"cross","value":10}}}]}),now).await.unwrap();
    }
    selection::refresh(&store, 4, 10000.0).await.unwrap();
    let reason: Option<String> =
        sqlx::query_scalar("SELECT excluded_reason FROM hl_address_selection WHERE address=$1")
            .bind(&maker)
            .fetch_one(&store.pool)
            .await
            .unwrap();
    assert_eq!(reason.as_deref(), Some("maker_two_sided_high_turnover"));
    for user in [&holder, &alt_holder] {
        let selected: bool =
            sqlx::query_scalar("SELECT selected FROM hl_address_selection WHERE address=$1")
                .bind(user)
                .fetch_one(&store.pool)
                .await
                .unwrap();
        assert!(
            selected,
            "inactive large holder must not be retired just for inactivity"
        );
    }
    sqlx::query("UPDATE hl_status SET enabled=true WHERE id=1")
        .execute(&store.pool)
        .await
        .unwrap();
    let evidence = evidence(&store, &symbol).await.unwrap();
    assert_eq!(evidence.long_quantity, 20.0);
    assert_eq!(evidence.selection.maker_excluded_addresses, 1);
    assert_eq!(evidence.activity.trade_count, 600);
    assert_eq!(evidence.activity.buy_quantity, 3000.0);
    assert_eq!(evidence.activity.sell_quantity, 3000.0);
    // Excluded accounts keep raw trades; a maker sample filter must never change Delta.
    let count: i64 = sqlx::query_scalar(
        "SELECT sum(trades)::bigint FROM hl_address_activity_1m WHERE address=$1",
    )
    .bind(&maker)
    .fetch_one(&store.pool)
    .await
    .unwrap();
    assert_eq!(count, 600);
    sqlx::query("DELETE FROM hl_trades WHERE coin=$1")
        .bind(&coin)
        .execute(&store.pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM hl_addresses WHERE address=ANY($1)")
        .bind(vec![maker, other, holder, alt_holder])
        .execute(&store.pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM hl_symbols WHERE coin IN ($1,$2)")
        .bind(&coin)
        .bind(&second)
        .execute(&store.pool)
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires a dedicated PostgreSQL test database"]
async fn explorer_returns_all_positions_beyond_the_overlay_top_twenty() {
    let store = Store::connect(&std::env::var("ET_DATA_TEST_DATABASE_URL").unwrap())
        .await
        .unwrap();
    let now = now_ms();
    let coin = format!("ALL{now}");
    let symbol = format!("{coin}USDT");
    sqlx::query("UPDATE hl_status SET enabled=true,min_position_usd=0 WHERE id=1")
        .execute(&store.pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO hl_symbols(coin,spot_symbol,context,context_ms) VALUES($1,$2,$3,$4)")
        .bind(&coin)
        .bind(&symbol)
        .bind(sqlx::types::Json(json!({"markPx":"100"})))
        .bind(now as i64)
        .execute(&store.pool)
        .await
        .unwrap();
    let mut addresses = Vec::new();
    for i in 1..=25 {
        let user = format!("0x{:040x}", now + 1000 + i);
        addresses.push(user.clone());
        sqlx::query(
            "INSERT INTO hl_addresses(address,first_seen_ms,last_seen_ms) VALUES($1,$2,$2)",
        )
        .bind(&user)
        .bind(now as i64)
        .execute(&store.pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO hl_address_selection(address,selected,evaluated_ms) VALUES($1,true,$2)",
        )
        .bind(&user)
        .bind(now as i64)
        .execute(&store.pool)
        .await
        .unwrap();
        save_snapshot(&store,&user,json!({"time":now,"assetPositions":[{"position":{"coin":coin,"szi":i.to_string(),"entryPx":"100","liquidationPx":null,"marginUsed":"10","leverage":{"type":"cross","value":10}}}]}),now).await.unwrap();
    }
    let result = evidence(&store, &symbol).await.unwrap();
    assert_eq!(result.positions.len(), 25);
    assert_eq!(result.largest_positions.len(), 20);
    assert_eq!(result.long_quantity, (1..=25).sum::<u64>() as f64);
    assert!(result.history.error.is_some());
    sqlx::query("DELETE FROM hl_addresses WHERE address=ANY($1)")
        .bind(&addresses)
        .execute(&store.pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM hl_symbols WHERE coin=$1")
        .bind(&coin)
        .execute(&store.pool)
        .await
        .unwrap();
}
