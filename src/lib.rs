mod util;

use anyhow::{anyhow, bail};
use scylla::{cql_to_rust::FromRowError, prepared_statement::PreparedStatement, Session};
use shared_types::*;
use util::*;

pub struct KVStore {
    version: VersionType,
    session: Session,
    prep_labeled: PreparedStatement,
    prep_labeled_next: PreparedStatement,
    prep_labeled_max_id: PreparedStatement,
    prep_labeled_by_id: PreparedStatement,
    prep_trained: PreparedStatement,
    prep_max_trained_id: PreparedStatement,
    prep_infered: PreparedStatement,
}


type LabelStoredRow = (i64, i64, i32, i64, Vec<f32>);

// fn tuple_to_labeled(tup: LabelStoredRow) -> anyhow::Result<LabelStored> {
//     let value = to_arr(tup.4)?;
//     Ok(LabelStored { event_id: tup.0 as u64, timestamp: tup.1, partition: tup.2, offset: tup.3, label: Label { value } })
//     // tup.4.try_into()
//     // .map_or_else(
//     //     |e|   bail!("Failed to convert labeled row to labeled: {:?}", e),
//     //     |arr: LabelType| )
//     // )
// }

fn tuple_to_label_stored(res_tup: Result<LabelStoredRow,scylla::cql_to_rust::FromRowError>) -> anyhow::Result<LabelStored> {
    match res_tup {
        Ok(tup) => {
            let value = to_arr(tup.4)?;
            Ok(LabelStored { event_id: tup.0 as u64, timestamp: tup.1, partition: tup.2, offset: tup.3, label: Label { value } })
        },
        Err(e) => {
            bail!("Failed to convert labeled row to labeled: {}", e)
        }
    }
}

fn to_arr<T: std::fmt::Debug, const N: usize>(v: Vec<T>) -> anyhow::Result<[T; N]> {
    v.try_into().map_err(|e| anyhow!("Failed to convert labeled row to labeled: {:?}", e))
    // .map_or_else(
    //     |e|   bail!("Failed to convert labeled row to labeled: {:?}", e),
    //     |arr: LabelType| Ok(LabelStored { event_id: tup.0 as u64, timestamp: tup.1, partition: tup.2, offset: tup.3, label: Label { value: arr } })
    // )
}

impl KVStore {
    pub async fn new(version: VersionType) -> anyhow::Result<KVStore> {
        let session = create_session().await?;
        Self::setup_db(&session).await?;
        let prep_labeled = session.prepare(INSERT_LABELED).await?;
        let prep_labeled_next = session.prepare(NEXT_LABELED).await?;
        let prep_labeled_max_id = session.prepare(MAX_LABELED_ID).await?;
        let prep_labeled_by_id = session.prepare(LABELED_BY_ID).await?;
        let prep_trained = session.prepare(INSERT_TRAINED).await?;
        let prep_max_trained_id = session.prepare(MAX_TRAINED_ID).await?;
        let prep_infered = session.prepare(INSERT_INFERRED).await?;
        Ok(KVStore { version, session, prep_labeled, prep_labeled_next, prep_labeled_max_id, prep_labeled_by_id, prep_trained, prep_max_trained_id, prep_infered })
    }

    pub async fn label_store(&self, labeled: &LabelStored) -> anyhow::Result<()> {
        self.session.execute(&self.prep_labeled, (
            self.version as i32, labeled.event_id as i64, labeled.timestamp,
            labeled.partition, labeled.offset,
            labeled.label.value.to_vec()
        )).await?;
        Ok(())
    }

    pub async fn label_next(&self, event_id: EventId, count: usize) -> anyhow::Result<Vec<LabelStored>> {
        self.session.execute(
            &self.prep_labeled_next,
            (self.version as i32, event_id as i64, count as i32)).await?
            .rows_typed::<LabelStoredRow>()?.map(tuple_to_label_stored).collect()
    }

    // pub async fn labeled_next(&self, version: u32, event_id: EventId) -> anyhow::Result<Option<LabelStored>> {
    //     let row = self.session.execute(&self.prep_labeled_next, (version as i32, event_id as i64)).await?.maybe_first_row_typed::<LabelStoredRow>()?;
    //     row.map(tuple_to_labeled).transpose()
    // }

    pub async fn label_max_event_id(&self, version: u32) -> anyhow::Result<Option<u64>> {
        Ok(self.session
                .execute(&self.prep_labeled_max_id, (version as i32,)).await?
                .maybe_first_row_typed::<(i64,)>()?
                .map(|row| row.0 as u64))
    }

    pub async fn label_by_id(&self, version: u32) -> anyhow::Result<Vec<LabelStoredRow>> {
        Ok(self.session
                .execute(&self.prep_labeled_by_id, (version as i32,)).await?
                .rows_typed::<LabelStoredRow>()?.collect::<Result<Vec<LabelStoredRow>,FromRowError>>()?)
    }

    pub async fn max_trained_event_id(&self, version: VersionType) -> anyhow::Result<Option<u64>> {
        Ok(self.session
            .execute(&self.prep_max_trained_id, (version as i32,)).await?
            .single_row_typed::<(Option<i64>,)>()?.0
            .map(|id| id as u64))
        // let qr = self.session.execute(&self.prep_max_trained_id, (version as i32,)).await?;
        // Ok(qr.single_row_typed::<(Option<i64>,)>()?.0.map(|x| x as u64))
    }

    pub async fn train_store(&self, value: TrainStored) -> anyhow::Result<()> {
        self.session.execute(&self.prep_trained, (
            self.version as i32, value.event_id as i64, value.timestamp, value.train.loss
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

        session.query(r#"
            CREATE TABLE IF NOT EXISTS ml_demo.labeled (
                version int,
                event_id bigint,
                timestamp bigint,
                offset bigint,
                partition int,
                label list<float>,
                PRIMARY KEY(version, event_id)
            );
        "#, &[]).await?;

        session.query(r#"
            CREATE TABLE IF NOT EXISTS ml_demo.trained (
                version int,
                event_id bigint,
                timestamp bigint,
                loss float,
                PRIMARY KEY(version, event_id)
            );
        "#, &[]).await?;

        session.query(r#"
        CREATE TABLE IF NOT EXISTS ml_demo.inferred (
            id bigint,
            timestamp bigint,
            inference list<float>,
            PRIMARY KEY(id)
        );
    "#, &[]).await?;

        Ok(())
    }
}
