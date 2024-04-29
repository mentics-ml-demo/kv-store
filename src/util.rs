use scylla::{Session, SessionBuilder};

pub(crate) static INSERT_INFERRED: &str = r#"
    INSERT INTO ml_demo.inference (id, time, inference) VALUES (?, ?, ?);
"#;

pub(crate) static SELECT_INFERRED: &str = r#"
    SELECT time, inference FROM ml_demo.inference WHERE id=?
"#;

pub(crate) static MAX_LABEL_ID: &str = r#"
    SELECT MAX(id) FROM ml_demo.label WHERE version=?
"#;

pub(crate) static INSERT_LABELLED: &str = r#"
    INSERT INTO ml_demo.labelled (id, time, inference) VALUES (?, ?, ?);
"#;

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
