use std::{env, str::FromStr};
use anyhow::{anyhow,Context};
use itertools::Itertools;
use sqlx::{sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePool}, Decode, Pool, Sqlite};

use util::convert_slice;
use chrono_util::{ChronoFeatures, CHRONO_BYTE_SIZE};
use shared_types::{*, stored::*};
use data_info::{LabelType, Series};

pub struct KVStore {
    version: VersionType,
    db: Pool<Sqlite>,
}

impl KVStore {
    pub async fn new(version: VersionType) -> anyhow::Result<KVStore> {
        let db_url = env::var("DATABASE_URL")?;
        let options = SqliteConnectOptions::from_str(&db_url)?.journal_mode(SqliteJournalMode::Wal);
        let db = SqlitePool::connect_with(options).await?;
        // According to the doc: "When using the high-level query API (sqlx::query), statements are prepared and cached per connection."
        // So we don't need to store prepared statements.
        // https://github.com/launchbadge/sqlx/tree/main

        Ok(Self { version, db })
    }

    pub async fn store_label(&self, labeled: &LabelStored) -> anyhow::Result<u64> {
        // TODO: to_vec might allocate there, we could impl Encode to avoid it maybe
        let output: &[u8] = convert_slice(&labeled.label);
        // println!("Storing label: {}", output.len());
        let query = sqlx::query!(r#"
INSERT INTO label (version, event_id, timestamp, offset_from, offset_to, label)
VALUES (?, ?, ?, ?, ?, ?);
            "#, self.version, labeled.event_id, labeled.timestamp,
            labeled.offset_from, labeled.offset_to,
            output
        );
        Ok(query.execute(&self.db).await?.rows_affected())
        // self.session.execute(&self.prep_label, (
        //     self.version, labeled.event_id as i64, labeled.timestamp,
        //     labeled.partition, labeled.offset_from, labeled.offset_to,
        //     labeled.label.value.to_vec()
        // )).await?;
        // Ok(())
    }

    pub async fn label_max(&self) -> anyhow::Result<Option<LabelStored>> {
        let query = sqlx::query_as!(LabelStored, r#"
SELECT event_id, timestamp, offset_from, offset_to, label as "label: LabelTypeStoredDb"
FROM label
WHERE version=?
ORDER BY event_id DESC
LIMIT 1
        "#, self.version);
        query.fetch_optional(&self.db).await.with_context(|| anyhow!("Error getting label max"))
    }

//     pub async fn train_store(&self, train: TrainStored) -> anyhow::Result<u64> {
//         let input = input_to_bytes(train.input);
//         let output: &[u8] = convert_slice(&train.output[..]);
//         let query = sqlx::query!(r#"
// INSERT INTO train (version, event_id, timestamp, offset, loss, input, output)
// VALUES (?, ?, ?, ?, ?, ?, ?);
//             "#, self.version, train.event_id, train.timestamp, train.offset, train.loss,
//             input, output
//         );
//         Ok(query.execute(&self.db).await.with_context(|| anyhow!("Error storing train"))?.rows_affected())
//     }

    pub async fn store_trains(&self, trains: Vec<TrainStored>) -> anyhow::Result<u64> {
        let values = trains.iter().map(|train| {
            format!("({}, {}, {}, {}, {}, x'{}', x'{}')", self.version, train.event_id, train.timestamp, train.offset, train.loss,
            input_to_hex(&train.input), label_to_hex(&train.output))
        }).join(",");

        let query = format!(r#"
INSERT INTO train (version, event_id, timestamp, offset, loss, input, output)
VALUES {};
            "#, values
        );
        // println!("store trains query: {}", query);
        let result = sqlx::raw_sql(&query).execute(&self.db).await?;
        Ok(result.rows_affected())
    }

    pub async fn retrainers(&self, top_count: i64, old_count: i64) -> anyhow::Result<Vec<TrainStoredWithLabel>> {
        let query = sqlx::query_as!(TrainStoredWithLabel, r#"
SELECT train.event_id, train.timestamp, train.offset,
    train.loss as "loss: f32",
    input as "input: InputStoredDb",
    output as "output: LabelTypeStoredDb",
    label as "label: LabelTypeStoredDb"
FROM train
JOIN label ON label.version = train.version AND label.event_id = train.event_id
WHERE train.version=? AND (
    train.event_id IN (SELECT event_id FROM train WHERE version=? ORDER BY loss DESC LIMIT ?)
    OR
    train.event_id IN (SELECT event_id FROM train WHERE version=? ORDER BY timestamp ASC LIMIT ?)
)
            "#, self.version, self.version, top_count, self.version, old_count
        );
        query.fetch_all(&self.db).await.with_context(|| anyhow!("Error getting retrainers"))
    }

    pub async fn reset_label_data(&self) -> anyhow::Result<()> {
        sqlx::query!("DELETE FROM label").execute(&self.db).await?;
        Ok(())
    }

    pub async fn reset_train_data(&self) -> anyhow::Result<()> {
        sqlx::query!("DELETE FROM train").execute(&self.db).await?;
        Ok(())
    }

    pub async fn label_lookup(&self, start_offset_id: OffsetId, count: usize) -> anyhow::Result<Vec<LabelLookup>> {
        let cnt = count as i64;
        let query = sqlx::query_as!(LabelLookup, r#"
SELECT event_id, offset_from, label as "label: LabelTypeStoredDb"
FROM label
WHERE version = ? and offset_from >= ?
ORDER BY event_id ASC
limit ?
            "#, self.version, start_offset_id, cnt);
        Ok(query.fetch_all(&self.db).await?)
    }

    pub async fn update_retrainers(&self, cur_time: Timestamp, event_ids: Vec<i64>, outputs: Vec<LabelType>, losses: Vec<f32>) -> anyhow::Result<u64> {
         let values = event_ids.iter().zip(outputs.iter().zip(losses))
                .map(|(event_id, (output, loss))|format!("({}, x'{}', {})", event_id, label_to_hex(output), loss)).join(",");

        let query = format!(r#"
WITH vals AS (
    SELECT column1 as event_id, column2 as output, column3 as loss FROM
    (VALUES {})
)
UPDATE train SET timestamp = {}, output = vals.output, loss = vals.loss FROM vals WHERE version = {} AND train.event_id = vals.event_id;
            "#, values, cur_time, self.version
        );
        // println!("update retrainers query: {}", query);
        let result = sqlx::raw_sql(&query).execute(&self.db).await?;
        // println!("Update retrainers modified {} rows", result.rows_affected());
        Ok(result.rows_affected())
    }

    pub async fn next_safe_predict_offset(&self) -> anyhow::Result<OffsetId> {
        let query = sqlx::query!(r#"
SELECT offset_to as offset
FROM label
WHERE version = ? AND event_id = (SELECT MAX(event_id) FROM train WHERE version = ?);
                "#, self.version, self.version
        );
        let x = query.fetch_one(&self.db).await?;
        Ok(x.offset)
    }
}
/*
SELECT offset_to as offset
FROM label
WHERE version = 1 AND event_id = (SELECT MAX(event_id) FROM train WHERE version = 1);
 */

pub struct LabelLookup {
    pub event_id: EventId,
    pub offset_from: OffsetId,
    pub label: LabelType
}

pub struct LabelTypeStoredDb(LabelType);
impl sqlx::Decode<'_, sqlx::Sqlite> for LabelTypeStoredDb {
    fn decode(value: <sqlx::Sqlite as sqlx::database::HasValueRef<'_>>::ValueRef) -> Result<Self, sqlx::error::BoxDynError> {
        // println!("decoding LabelTypeStoredDb1");
        let bytes = <&[u8] as Decode<Sqlite>>::decode(value)?;
        // println!("decoding LabelTypeStoredDb2 bytes: {}", bytes.len());
        let slice: &[ModelFloat] = convert_slice(bytes);
        // println!("decoding LabelTypeStoredDb3: {}", slice.len());
        let label = slice.try_into()?;
        // println!("decoding LabelTypeStoredDb4");
        Ok(LabelTypeStoredDb(label))
    }
}

impl From<LabelTypeStoredDb> for LabelType {
    fn from(value: LabelTypeStoredDb) -> Self {
        value.0
    }
}

pub struct InputStoredDb(InputStored);
impl sqlx::Decode<'_, sqlx::Sqlite> for InputStoredDb {
    fn decode(value: <sqlx::Sqlite as sqlx::database::HasValueRef<'_>>::ValueRef) -> Result<Self, sqlx::error::BoxDynError> {
        let bytes = <&[u8] as Decode<Sqlite>>::decode(value)?;
        let chrono_slice = &bytes[0..CHRONO_BYTE_SIZE];
        let chrono: ChronoFeatures = convert_slice(chrono_slice).try_into()?;
        let series_slice = &bytes[CHRONO_BYTE_SIZE..];
        // println!("decoding InputStoredDb5");
        let series: Series = convert_slice(series_slice).try_into()?;
        // println!("decoding InputStoredDb6");
        Ok(InputStoredDb((chrono, series)))
    }
}

impl From<InputStoredDb> for InputStored {
    fn from(value: InputStoredDb) -> Self {
        value.0
    }
}

fn input_to_hex((chrono, series): &InputStored) -> String {
    // TODO: not sure how efficient this is
    let mut s = hex::encode(convert_slice(chrono));
    s.push_str(&hex::encode(convert_slice(series)));
    s
}

fn label_to_hex(label: &LabelType) -> String {
    hex::encode(convert_slice(label))
}
