// #![feature(slice_flatten)]

mod util;

use std::iter::zip;

use anyhow::{anyhow, bail};
use convert::to_model_input_flat;
use scylla::{cql_to_rust::FromRowError, frame::response::result::Row, prepared_statement::PreparedStatement, transport::query_result::SingleRowTypedError, Session};
use shared_types::*;
use util::*;

pub struct KVStore {
    version: i32,
    session: Session,

    prep_label: PreparedStatement,
    prep_label_max: PreparedStatement,
    prep_label_by_id: PreparedStatement,

    prep_train_insert: PreparedStatement,
    prep_train_in_ids: PreparedStatement,

    prep_train_loss_insert: PreparedStatement,
    prep_train_top_loss: PreparedStatement,
    prep_train_loss_update: PreparedStatement,
}

type LabelStoredRow = (i64, i64, i32, i64, i64, Vec<f32>);
type TrainStoredRow = (i64, i64, i32, i64, Vec<f32>, Vec<f32>);
type TrainLossStoredRow = (i64, i64, f32);
// type TrainFullStoredRow = (i64, i64, i32, i64, Vec<f32>, Vec<f32>, f32);
pub struct TrainFull {
    pub event_id: EventId,
    pub timestamp: Timestamp,
    pub partition: PartitionId,
    pub offset: OffsetId,
    pub input_flat: Vec<f32>,
    pub label_flat: Vec<f32>,
    pub loss: f32,
}


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
        let prep_label = session.prepare(INSERT_LABELED).await?;
        let prep_label_max = session.prepare(LABEL_MAX).await?;
        let prep_label_by_id = session.prepare(LABELED_BY_ID).await?;
        let prep_train_insert = session.prepare(INSERT_TRAINED).await?;
        let prep_train_in_ids = session.prepare(TRAIN_IN_IDS).await?;
        let prep_train_loss_insert = session.prepare(INSERT_TRAIN_LOSS).await?;
        let prep_train_top_loss = session.prepare(TRAIN_TOP_LOSS).await?;
        let prep_train_loss_update = session.prepare(UPDATE_TRAIN_LOSS).await?;

        Ok(KVStore { version: version as i32, session,
            prep_label, prep_label_by_id, prep_label_max,
            prep_train_insert, prep_train_in_ids,
            prep_train_loss_insert, prep_train_top_loss, prep_train_loss_update })
    }

    pub async fn label_store(&self, labeled: &LabelStored) -> anyhow::Result<()> {
        self.session.execute(&self.prep_label, (
            self.version, labeled.event_id as i64, labeled.timestamp,
            labeled.partition, labeled.offset_from, labeled.offset_to,
            labeled.label.value.to_vec()
        )).await?;
        Ok(())
    }

    pub async fn label_max(&self) -> anyhow::Result<Option<LabelStored>> {
        match self.session.execute(
            &self.prep_label_max,
            (self.version,)).await?
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
        let input = to_model_input_flat(trained.input).to_vec();
        self.session.execute(&self.prep_train_insert, (
            self.version, trained.event_id as i64, trained.timestamp,
            trained.partition, trained.offset,
            input, trained.label.value.to_vec()
        )).await?;
        Ok(())
    }

    async fn train_in_ids(&self, ids: &[i64]) -> anyhow::Result<Vec<TrainStoredRow>> {
        Ok(self.session
                .execute(&self.prep_train_in_ids, (
                    self.version, ids
                    // ids.iter().map(|id| *id as i64).collect::<Vec<i64>>()
                )).await?
                .rows_typed::<TrainStoredRow>()?.collect::<Result<Vec<TrainStoredRow>,FromRowError>>()?)
    }

    pub async fn train_loss_store(&self, trained: TrainLossStored) -> anyhow::Result<()> {
        self.session.execute(&self.prep_train_loss_insert, (
            self.version, trained.event_id as i64, trained.timestamp, trained.loss
        )).await?;
        Ok(())
    }

    pub async fn train_loss_update(&self, event_id: EventId, timestamp: Timestamp, prev_loss: ModelFloat, new_loss: ModelFloat) -> anyhow::Result<()> {
        // println!("Updating loss from {} to {} for event {}", prev_loss, new_loss, event_id);
        self.session.execute(&self.prep_train_loss_update, (
            self.version, prev_loss, event_id as i64, self.version, event_id as i64, timestamp, new_loss
        )).await?;
        Ok(())
    }

    pub async fn train_top_full(&self, count: usize) -> anyhow::Result<Vec<TrainFull>> {
        let mut top_loss = self.train_top_loss(count).await?;
        top_loss.sort_unstable_by_key(|row| row.0);

        self.train_full_from_loss(top_loss).await
    }

    pub async fn train_oldest_full(&self, count: usize) -> anyhow::Result<Vec<TrainFull>> {
        let mut top_loss = self.train_top_oldest(count).await?;
        top_loss.sort_unstable_by_key(|row| row.0);

        self.train_full_from_loss(top_loss).await
    }

    async fn train_full_from_loss(&self, train_loss: Vec<TrainLossStoredRow>) -> Result<Vec<TrainFull>, anyhow::Error> {
        let (ids, losses): (Vec<_>, Vec<_>) = train_loss.iter().map(|row| (row.0, row.2)).unzip();
        let trains = self.train_in_ids(&ids).await?;
        assert!(train_loss.len() == trains.len());
        for i in 0..train_loss.len() {
            assert!(ids[i] == trains[i].0)
        }
        let res = zip(trains, losses).map(|(train, loss)|
            TrainFull {
                event_id: train.0 as EventId, timestamp: train.1,
                partition: train.2, offset: train.3,
                input_flat: train.4, label_flat: train.5,
                loss
            }
            // (train.0, train.1, train.2, train.3, train.4, train.5, loss)
        ).collect::<Vec<_>>();
        Ok(res)
    }

    async fn train_top_loss(&self, count: usize) -> anyhow::Result<Vec<TrainLossStoredRow>> {
        Ok(self.session
                .execute(&self.prep_train_top_loss, (self.version, count as i32)).await?
                .rows_typed::<TrainLossStoredRow>()?.collect::<Result<Vec<TrainLossStoredRow>,FromRowError>>()?)
    }

    async fn train_top_oldest(&self, count: usize) -> anyhow::Result<Vec<TrainLossStoredRow>> {
        let rows1 = self.session.query("SELECT MIN(timestamp) FROM ml_demo.train_loss", &[]).await?;
        let row = rows1.single_row()?;
        let value = match row.columns[0].as_ref() {
            None => return Ok(vec![]),
            Some(value) => value
        };

        let max_timestamp = value.as_bigint().unwrap();

        let rows_raw = self.session.query(format!("SELECT event_id, timestamp, loss FROM ml_demo.train_loss WHERE timestamp <= {} LIMIT {} ALLOW FILTERING", max_timestamp + 10000, 2*count), &[]).await?;
        // 1717282813489
        // SELECT event_id, timestamp, loss FROM ml_demo.train_loss WHERE timestamp <= 1717282823489 LIMIT 64 ALLOW FILTERING;
        let mut rows = rows_raw.rows_typed::<TrainLossStoredRow>()?.collect::<Result<Vec<TrainLossStoredRow>,FromRowError>>()?;
        rows.sort_unstable_by_key(|row| row.1);
        if rows.len() < count {
            println!("WARNING: Not enough rows {} for top oldest for timestamp: {}", rows.len(), max_timestamp);
        }
        rows.truncate(count);

        Ok(rows)
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
                input frozen<list<float>>,
                label frozen<list<float>>,
                PRIMARY KEY(version, event_id)
            );
        "#, &[]).await?;

        session.query(r#"
            CREATE TABLE IF NOT EXISTS ml_demo.train_loss (
                version int,
                event_id bigint,
                timestamp bigint,
                loss float,
                PRIMARY KEY(version, loss, event_id)
            ) WITH CLUSTERING ORDER BY (loss DESC, event_id ASC);
        "#, &[]).await?;

        // session.query(r#"
        //     CREATE TABLE IF NOT EXISTS ml_demo.inferred (
        //         id bigint,
        //         timestamp bigint,
        //         inference frozen<list<float>>,
        //         PRIMARY KEY(id)
        //     );
        // "#, &[]).await?;

        Ok(())
    }

    pub async fn reset_label_data(&self) -> anyhow::Result<()> {
        self.session.query("TRUNCATE ml_demo.labeled;", &[]).await?;
        Ok(())
    }

    pub async fn reset_train_data(&self) -> anyhow::Result<()> {
        self.session.query("TRUNCATE ml_demo.trained;", &[]).await?;
        self.session.query("TRUNCATE ml_demo.train_loss;", &[]).await?;
        Ok(())
    }

    pub async fn run_query(&self, query: &str) -> anyhow::Result<Vec<Row>> {
        let result = self.session.query(query, &[]).await?;
        Ok(result.rows()?)
    }
}
