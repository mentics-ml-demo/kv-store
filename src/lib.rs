mod util;

use anyhow::Context;
use scylla::{prepared_statement::PreparedStatement, QueryResult, Session};
use shared_types::Inferred;

pub struct KVStore {
    session: Session,
    prep_infer: PreparedStatement,
}

impl KVStore {
    pub async fn new() -> anyhow::Result<KVStore> {
        let session = util::create_session().await?;
        Self::setup_db(&session).await?;
        let prep_infer = session.prepare(util::INSERT_INFERRED).await?;
        Ok(KVStore { session, prep_infer })
    }

    /// TODO: not happy about exposing QueryResult to callers
    /// Could do result: QueryResult
    pub async fn write_inference(&self, inf: &Inferred) -> anyhow::Result<()> {
        self.session.execute(&self.prep_infer, (
            inf.id as i64, inf.timestamp, inf.inference.value
        )).await?;
        Ok(())
        // .map_err(|e| format!("Error {:?} writing inference {:?}", e, inf))
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

        session.query(r#"
            CREATE TABLE IF NOT EXISTS ml_demo.inference (
                id bigint,
                time bigint,
                inference float,
                PRIMARY KEY(id)
            );
        "#, &[]).await?;
        Ok(())
    }
}
