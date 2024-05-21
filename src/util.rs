use scylla::{Session, SessionBuilder};

pub(crate) static INSERT_LABELED: &str = r#"
    INSERT INTO ml_demo.labeled (version, event_id, timestamp, partition, offset, label) VALUES (?, ?, ?, ?, ?, ?);
"#;

pub(crate) static NEXT_LABELED: &str = r#"
    SELECT event_id, timestamp, partition, offset, label FROM ml_demo.labeled WHERE version=? AND event_id > ? ORDER BY event_id ASC LIMIT 1
"#;

pub(crate) static MAX_LABELED_ID: &str = r#"
    SELECT MAX(event_id) FROM ml_demo.labeled WHERE version=?
"#;

pub(crate) static LABELED_BY_ID: &str = r#"
    SELECT event_id, timestamp, partition, offset, label FROM ml_demo.labeled WHERE version=? AND event_id=?
"#;

pub(crate) static MAX_TRAINED_ID: &str = r#"
    SELECT MAX(event_id) FROM ml_demo.trained WHERE version=?
"#;

pub(crate) static INSERT_TRAINED: &str = r#"
    INSERT INTO ml_demo.trained (version, event_id, timestamp, loss) VALUES (?, ?, ?, ?);
"#;

pub(crate) static INSERT_INFERRED: &str = r#"
    INSERT INTO ml_demo.inference (id, time, inference) VALUES (?, ?, ?);
"#;

// pub(crate) static SELECT_INFERRED: &str = r#"
//     SELECT time, ml_demo.inference FROM inference WHERE id=?
// "#;

pub(crate) async fn create_session() -> anyhow::Result<Session> {
    let endpoint = std::env::var("SCYLLA_ENDPOINT").unwrap();
    SessionBuilder::new().known_node(endpoint).build().await.map_err(From::from)
}

// pub async fn select_key_value(
//     session: &Session,
//     id: i64
// ) -> Result<Vec<KeyValue>> {
//     session
//         .query(SELECT_KEY_VALUE, (id,))
//         .await?
//         .rows
//         .unwrap_or_default()
//         .into_typed::<KeyValue>()
//         .map(|v| v.map_err(From::from))
//         .collect()
// }
