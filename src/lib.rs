mod util;

use scylla::{prepared_statement::PreparedStatement, Session};
use shared_types::*;

pub struct KVStore {
    session: Session,
    // prep_infer: PreparedStatement,
    prep_label: PreparedStatement,
    prep_max_label_id: PreparedStatement,
}

impl KVStore {
    pub async fn new() -> anyhow::Result<KVStore> {
        let session = util::create_session().await?;
        Self::setup_db(&session).await?;
        // let prep_infer = session.prepare(util::INSERT_INFERRED).await?;
        let prep_max_label_id = session.prepare(util::MAX_LABEL_ID).await?;
        let prep_label = session.prepare(util::INSERT_LABELLED).await?;
        // Ok(KVStore { session, prep_infer, prep_max_label_id, prep_label })
        Ok(KVStore { session, prep_label, prep_max_label_id })
    }

    /// TODO: not happy about exposing QueryResult to callers
    /// Could do result: QueryResult
    // pub async fn write_inference(&self, infd: &Inferred) -> anyhow::Result<()> {
    //     self.session.execute(&self.prep_infer, (
    //         infd.id as i64, infd.timestamp, infd.inference.value
    //     )).await?;
    //     Ok(())
    //     // .map_err(|e| format!("Error {:?} writing inference {:?}", e, inf))
    // }

    pub async fn write_label(&self, version: VersionType, labelled: &Labelled) -> anyhow::Result<()> {
        // let s = format!("{:?}", labelled.label.value);
        self.session.execute(&self.prep_label, (
            version as i32, labelled.event_id as i64, labelled.timestamp, labelled.label.value.to_vec()
        )).await?;
        Ok(())
    }

    // ----

    async fn setup_db(session: &Session) -> anyhow::Result<()> {
        session.query(r#"
            CREATE KEYSPACE IF NOT EXISTS ml_demo
            WITH REPLICATION = {
                'class': 'SimpleStrategy',
                'replication_factor': 1
            };
        "#, &[]).await?;

        // session.query(r#"
        //     CREATE TABLE IF NOT EXISTS ml_demo.inferred (
        //         id bigint,
        //         timestamp bigint,
        //         inference float,
        //         PRIMARY KEY(id)
        //     );
        // "#, &[]).await?;

        session.query(r#"
            CREATE TABLE IF NOT EXISTS ml_demo.labelled (
                version int,
                event_id bigint,
                timestamp bigint,
                label list<float>,
                PRIMARY KEY(version, event_id)
            );
        "#, &[]).await?;

        Ok(())
    }

    pub async fn max_labelled_event_id(&self, version: u32) -> anyhow::Result<u64> {
        Ok(self.session
                .execute(&self.prep_max_label_id, (version as i32,)).await?
                .maybe_first_row_typed::<(i64,)>()?
                .map(|row| row.0 as u64)
                .unwrap_or(0))
    }
}
