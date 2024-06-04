use scylla::{Session, SessionBuilder};

pub(crate) static INSERT_LABELED: &str = r#"
INSERT INTO ml_demo.labeled (version, event_id, timestamp, partition, offset_from, offset_to, label)
VALUES (?, ?, ?, ?, ?, ?, ?);
"#;

// pub(crate) static NEXT_LABELED: &str = r#"
//     SELECT event_id, timestamp, partition, offset, label FROM ml_demo.labeled WHERE version=? AND event_id >= ? ORDER BY event_id ASC LIMIT ?
// "#;

// pub(crate) static MAX_LABELED_ID: &str = r#"
//     SELECT MAX(event_id) FROM ml_demo.labeled WHERE version=?
// "#;

pub(crate) static LABEL_MAX: &str = r#"
SELECT event_id, timestamp, partition, offset_from, offset_to, label
FROM ml_demo.labeled
WHERE version=?
ORDER BY event_id DESC
LIMIT 1
"#;

pub(crate) static LABELED_BY_ID: &str = r#"
SELECT event_id, timestamp, partition, offset_from, offset_to, label
FROM ml_demo.labeled
WHERE version=? AND event_id=?
"#;

// pub(crate) static MAX_TRAINED_ID: &str = r#"
//     SELECT MAX(event_id) FROM ml_demo.trained WHERE version=?
// "#;

pub(crate) static INSERT_TRAINED: &str = r#"
INSERT INTO ml_demo.trained (version, event_id, timestamp, partition, offset, input, label)
VALUES (?, ?, ?, ?, ?, ?, ?);
"#;

pub(crate) static TRAIN_IN_IDS: &str = r#"
SELECT event_id, timestamp, partition, offset, input, label
FROM ml_demo.trained
WHERE version=? AND event_id IN ?
ORDER BY event_id ASC
"#;
// SELECT version, event_id, timestamp, partition, offset, input, label FROM ml_demo.trained WHERE version=1 AND event_id IN (1,2,3)

pub(crate) static INSERT_TRAIN_LOSS: &str = r#"
INSERT INTO ml_demo.train_loss (version, event_id, timestamp, loss)
VALUES (?, ?, ?, ?);
"#;

pub(crate) static TRAIN_TOP_LOSS: &str = r#"
SELECT event_id, timestamp, loss FROM ml_demo.train_loss
WHERE version=?
ORDER BY loss DESC
LIMIT ?
"#;
// , event_id ASC
// SELECT event_id, timestamp, loss FROM ml_demo.train_loss WHERE version=1 ORDER BY loss DESC LIMIT 3;

pub(crate) static UPDATE_TRAIN_LOSS: &str = r#"
BEGIN BATCH
DELETE FROM ml_demo.train_loss WHERE version=? AND loss=? AND event_id=?;
INSERT INTO ml_demo.train_loss (version, event_id, timestamp, loss) VALUES (?, ?, ?, ?);
APPLY BATCH;
"#;

/*
BEGIN BATCH
  DELETE FROM ml_demo.train_loss WHERE version=1 AND loss=1.0 AND event_id=1;
  INSERT INTO ml_demo.train_loss (version, event_id, timestamp, partition, offset, loss, input, label) VALUES (1, 1, 1, 1, 1, 1.7, [1.0, 2.0], [3.0, 4.0]);
APPLY BATCH;
 */

// pub(crate) static INSERT_INFERRED: &str = r#"
//     INSERT INTO ml_demo.inference (id, time, inference) VALUES (?, ?, ?);
// "#;

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
