mod util;

use anyhow::{anyhow, bail};
use scylla::{cql_to_rust::FromRowError, prepared_statement::PreparedStatement, transport::query_result::SingleRowTypedError, Session};
use shared_types::*;
use util::*;

pub struct KVStore {
    version: VersionType,
    session: Session,
    prep_label: PreparedStatement,
    // prep_labeled_next: PreparedStatement,
    // prep_labeled_max_id: PreparedStatement,
    prep_label_max: PreparedStatement,
    prep_label_by_id: PreparedStatement,
    prep_train: PreparedStatement,
    // prep_max_train_id: PreparedStatement,
    // prep_infered: PreparedStatement,
    prep_train_loss: PreparedStatement,
}

type LabelStoredRow = (i64, i64, i32, i64, i64, Vec<f32>);
type TrainStoredRow = (i64, i64, i32, i64, f32, Vec<f32>);

// fn tuple_to_labeled(tup: LabelStoredRow) -> anyhow::Result<LabelStored> {
//     let value = to_arr(tup.4)?;
//     Ok(LabelStored { event_id: tup.0 as u64, timestamp: tup.1, partition: tup.2, offset: tup.3, label: Label { value } })
//     // tup.4.try_into()
//     // .map_or_else(
//     //     |e|   bail!("Failed to convert labeled row to labeled: {:?}", e),
//     //     |arr: LabelType| )
//     // )
// }

// fn tuple_to_label_stored(res_tup: Result<LabelStoredRow,scylla::cql_to_rust::FromRowError>) -> anyhow::Result<LabelStored> {
fn tuple_to_label_stored(tup: LabelStoredRow) -> anyhow::Result<LabelStored> {
    // match res_tup {
    //     Ok(tup) => {
    //         let value = to_arr(tup.4)?;
    //         Ok(LabelStored { event_id: tup.0 as u64, timestamp: tup.1, partition: tup.2, offset: tup.3, label: Label { value } })
    //     },
    //     Err(e) => {
    //         bail!("Failed to convert labeled row to labeled: {}", e)
    //     }
    // }
    let value = to_arr(tup.5)?;
    Ok(LabelStored { event_id: tup.0 as u64, timestamp: tup.1, partition: tup.2, offset_from: tup.3, offset_to: tup.4, label: Label { value } })
}

fn to_arr<T: std::fmt::Debug, const N: usize>(v: Vec<T>) -> anyhow::Result<[T; N]> {
    v.try_into().map_err(|e| anyhow!("Failed to convert labeled row arr: {:?}", e))
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
        // let prep_labeled_next = session.prepare(NEXT_LABELED).await?;
        // let prep_labeled_max_id = session.prepare(MAX_LABELED_ID).await?;
        let prep_label_max = session.prepare(LABEL_MAX).await?;
        let prep_label_by_id = session.prepare(LABELED_BY_ID).await?;
        let prep_train = session.prepare(INSERT_TRAINED).await?;
        let prep_train_loss = session.prepare(SELECT_TRAINED_LOSS).await?;
        // let prep_infered = session.prepare(INSERT_INFERRED).await?;

        Ok(KVStore { version, session, prep_label: prep_labeled, prep_label_by_id, prep_label_max, prep_train, prep_train_loss })
    }

    pub async fn label_store(&self, labeled: &LabelStored) -> anyhow::Result<()> {
        self.session.execute(&self.prep_label, (
            self.version as i32, labeled.event_id as i64, labeled.timestamp,
            labeled.partition, labeled.offset_from, labeled.offset_to,
            labeled.label.value.to_vec()
        )).await?;
        Ok(())
    }

    pub async fn label_max(&self) -> anyhow::Result<Option<LabelStored>> {
        match self.session.execute(
            &self.prep_label_max,
            (self.version as i32,)).await?
            .single_row_typed::<LabelStoredRow>() {
            Ok(row) => {
                Some(tuple_to_label_stored(row)).transpose()
            },
            Err(SingleRowTypedError::BadNumberOfRows(_num_rows)) => {
                Ok(None)
            },
            Err(e) => {
                bail!("Failed to get label max: {:?}", e)
            }
        }
    }

    // pub async fn labeled_next(&self, version: u32, event_id: EventId) -> anyhow::Result<Option<LabelStored>> {
    //     let row = self.session.execute(&self.prep_labeled_next, (version as i32, event_id as i64)).await?.maybe_first_row_typed::<LabelStoredRow>()?;
    //     row.map(tuple_to_labeled).transpose()
    // }

    // pub async fn label_max_event_id(&self, version: u32) -> anyhow::Result<Option<u64>> {
    //     Ok(self.session
    //             .execute(&self.prep_labeled_max_id, (version as i32,)).await?
    //             .maybe_first_row_typed::<(i64,)>()?
    //             .map(|row| row.0 as u64))
    // }

    pub async fn label_by_id(&self, version: u32) -> anyhow::Result<Vec<LabelStoredRow>> {
        Ok(self.session
                .execute(&self.prep_label_by_id, (version as i32,)).await?
                .rows_typed::<LabelStoredRow>()?.collect::<Result<Vec<LabelStoredRow>,FromRowError>>()?)
    }

    // pub async fn max_trained_event_id(&self, version: VersionType) -> anyhow::Result<Option<u64>> {
    //     Ok(self.session
    //         .execute(&self.prep_max_train_id, (version as i32,)).await?
    //         .single_row_typed::<(Option<i64>,)>()?.0
    //         .map(|id| id as u64))
    //     // let qr = self.session.execute(&self.prep_max_trained_id, (version as i32,)).await?;
    //     // Ok(qr.single_row_typed::<(Option<i64>,)>()?.0.map(|x| x as u64))
    // }

    pub async fn train_store(&self, trained: TrainStored) -> anyhow::Result<()> {
        let input_arr: [f32; 2048] = bytemuck::cast(trained.input);
        let input = input_arr.to_vec();
        self.session.execute(&self.prep_train, (
            self.version as i32, trained.event_id as i64, trained.timestamp,
            trained.partition, trained.offset,
            trained.train.loss, input
        )).await?;
        Ok(())
    }

    pub async fn train_loss(&self, count: u16) -> anyhow::Result<Vec<TrainStoredRow>> {
        Ok(self.session
                .execute(&self.prep_train_loss, (self.version as i32, count as i32)).await?
                .rows_typed::<TrainStoredRow>()?.collect::<Result<Vec<TrainStoredRow>,FromRowError>>()?)
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
                partition int,
                offset_from bigint,
                offset_to bigint,
                label frozen<list<float>>,
                PRIMARY KEY(version, event_id)
            );
        "#, &[]).await?;

        session.query(r#"
            CREATE TABLE IF NOT EXISTS ml_demo.trained (
                version int,
                event_id bigint,
                timestamp bigint,
                partition int,
                offset bigint,
                loss float,
                input frozen<list<float>>,
                PRIMARY KEY(version, loss, event_id)
            ) WITH CLUSTERING ORDER BY (loss DESC, event_id ASC);
        "#, &[]).await?;

        session.query(r#"
        CREATE TABLE IF NOT EXISTS ml_demo.inferred (
            id bigint,
            timestamp bigint,
            inference frozen<list<float>>,
            PRIMARY KEY(id)
        );
    "#, &[]).await?;

/*
CREATE INDEX loss_index ON ml_demo.trained (version, loss);

 */

        Ok(())
    }
}
