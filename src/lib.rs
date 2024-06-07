use std::{env, iter::zip, str::FromStr};
use anyhow::{anyhow,Context};
use sqlx::{sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePool}, Decode, Pool, Sqlite};

use shared_types::*;
use convert::*;

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

    pub async fn label_store(&self, labeled: &LabelStored) -> anyhow::Result<u64> {
        let output = output_to_bytes(labeled.label.value);
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
SELECT event_id, timestamp, offset_from, offset_to, label
FROM label
WHERE version=?
ORDER BY event_id DESC
LIMIT 1
        "#, self.version);
        query.fetch_optional(&self.db).await.with_context(|| anyhow!("Error getting label max"))
    }

    pub async fn train_store(&self, train: TrainStored) -> anyhow::Result<u64> {
        let input = convert::input_to_bytes(train.input);
        let output = convert::output_to_bytes(train.output);
        let query = sqlx::query!(r#"
INSERT INTO train (version, event_id, timestamp, offset, loss, input, output)
VALUES (?, ?, ?, ?, ?, ?, ?);
            "#, self.version, train.event_id, train.timestamp, train.offset, train.loss,
            input, output
        );
        Ok(query.execute(&self.db).await.with_context(|| anyhow!("Error storing train"))?.rows_affected())
    }

//     pub async fn train_loss_update(&self, event_id: EventId, new_loss: ModelFloat) -> anyhow::Result<u64> {
//         let query = sqlx::query!(r#"
// UPDATE train SET loss = ? WHERE version = ? AND event_id = ?
//             "#, new_loss, self.version, event_id
//         );
//         Ok(query.execute(&self.db).await.with_context(|| anyhow!("Error updating loss"))?.rows_affected())
//     }

    pub async fn retrainers(&self, top_count: i64, old_count: i64) -> anyhow::Result<Vec<TrainStoredWithLabel>> {
        let query = sqlx::query_as!(TrainStoredWithLabel, r#"
SELECT train.event_id, train.timestamp, train.offset,
    train.loss as "loss: f32",
    input as "input: ModelInputDb",
    output as "output: ModelOutputDb",
    label as "label: ModelOutputDb"
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
SELECT event_id, offset_from, label as "label: ModelOutputDb"
FROM label
WHERE version = ? and offset_from >= ?
ORDER BY event_id ASC
limit ?
            "#, self.version, start_offset_id, cnt);
        Ok(query.fetch_all(&self.db).await?)
    }

    /*
 */

    pub async fn update_losses(&self, event_ids: Vec<i64>, new_losses: Vec<f32>) -> anyhow::Result<u64> {
        // let values = String::from("VALUES ");
        let parts: Vec<String> = zip(event_ids, new_losses).map(|(event_id, loss)|
            format!("({}, {})", event_id, loss)
        ).collect();
        let values = parts.join(",");

        let query = format!(r#"
WITH vals AS (
    SELECT column1 as event_id, column2 as loss FROM
    (VALUES {})
)
UPDATE train SET loss = vals.loss FROM vals WHERE version = {} AND train.event_id = vals.event_id;
            "#, values, self.version
        );
        let result = sqlx::raw_sql(&query).execute(&self.db).await?;
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

pub struct LabelLookup {
    pub event_id: EventId,
    pub offset_from: OffsetId,
    pub label: LabelType
}

struct ModelInputDb(ModelInput);
struct ModelOutputDb(ModelOutput);

impl sqlx::Decode<'_, sqlx::Sqlite> for ModelInputDb {
    fn decode(value: <sqlx::Sqlite as sqlx::database::HasValueRef<'_>>::ValueRef) -> Result<Self, sqlx::error::BoxDynError> {
        let bytes = <&[u8] as Decode<Sqlite>>::decode(value)?;
        Ok(ModelInputDb(input_from_bytes(bytes)))
    }
}

impl From<ModelInputDb> for ModelInput {
    fn from(value: ModelInputDb) -> Self {
        value.0
    }
}


impl sqlx::Decode<'_, sqlx::Sqlite> for ModelOutputDb {
    fn decode(value: <sqlx::Sqlite as sqlx::database::HasValueRef<'_>>::ValueRef) -> Result<Self, sqlx::error::BoxDynError> {
        let bytes = <&[u8] as Decode<Sqlite>>::decode(value)?;
        Ok(ModelOutputDb(output_from_bytes(bytes)))
    }
}

impl From<ModelOutputDb> for ModelOutput {
    fn from(value: ModelOutputDb) -> Self {
        value.0
    }
}
