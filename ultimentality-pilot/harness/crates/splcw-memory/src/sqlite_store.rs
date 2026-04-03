use std::path::Path;
use std::str::FromStr;

use anyhow::Context;
use async_trait::async_trait;
use splcw_core::{
    CapabilityGap, CapabilityGapStatus, PlanSnapshot, Receipt, Recodification, SufficientPlan,
};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use sqlx::{Row, SqliteConnection, SqlitePool};

use crate::{StateStore, VerificationWatermark};

pub struct SqliteStateStore {
    pool: SqlitePool,
}

impl SqliteStateStore {
    pub async fn connect<P: AsRef<Path>>(path: P) -> anyhow::Result<Self> {
        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .foreign_keys(true);

        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await
            .context("connect sqlite state store")?;

        let store = Self { pool };
        store.migrate().await?;
        Ok(store)
    }

    pub async fn connect_in_memory() -> anyhow::Result<Self> {
        let options = SqliteConnectOptions::from_str("sqlite::memory:")?
            .foreign_keys(true)
            .shared_cache(true);

        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await
            .context("connect in-memory sqlite state store")?;

        let store = Self { pool };
        store.migrate().await?;
        Ok(store)
    }

    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    async fn migrate(&self) -> anyhow::Result<()> {
        let mut tx = self.pool.begin().await?;
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            "#,
        )
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS plan_snapshots (
                snapshot_id TEXT PRIMARY KEY,
                plan_id TEXT NOT NULL,
                plan_version INTEGER NOT NULL,
                recorded_at TEXT NOT NULL,
                rationale TEXT NOT NULL,
                source_gap_id TEXT,
                plan_json TEXT NOT NULL
            );
            "#,
        )
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS receipts (
                receipt_id TEXT PRIMARY KEY,
                plan_id TEXT NOT NULL,
                unit TEXT NOT NULL,
                recorded_at TEXT NOT NULL,
                contradicted TEXT,
                receipt_json TEXT NOT NULL
            );
            "#,
        )
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS capability_gaps (
                gap_id TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                discovered_at TEXT NOT NULL,
                last_touched_at TEXT NOT NULL,
                title TEXT NOT NULL,
                gap_json TEXT NOT NULL
            );
            "#,
        )
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS recodifications (
                recodification_id TEXT PRIMARY KEY,
                plan_id TEXT NOT NULL,
                recorded_at TEXT NOT NULL,
                recodification_json TEXT NOT NULL
            );
            "#,
        )
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(())
    }

    async fn set_metadata_in_conn(
        conn: &mut SqliteConnection,
        key: &str,
        value: &str,
    ) -> anyhow::Result<()> {
        sqlx::query(
            r#"
            INSERT INTO metadata (key, value)
            VALUES (?1, ?2)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value;
            "#,
        )
        .bind(key)
        .bind(value)
        .execute(&mut *conn)
        .await?;

        Ok(())
    }
}

#[async_trait]
impl StateStore for SqliteStateStore {
    async fn load_plan(&self) -> anyhow::Result<Option<SufficientPlan>> {
        Ok(self
            .load_current_snapshot()
            .await?
            .map(|snapshot| snapshot.plan))
    }

    async fn load_current_snapshot(&self) -> anyhow::Result<Option<PlanSnapshot>> {
        let row = sqlx::query(
            r#"
            SELECT plan_json
            FROM plan_snapshots
            ORDER BY recorded_at DESC, rowid DESC
            LIMIT 1;
            "#,
        )
        .fetch_optional(&self.pool)
        .await?;

        row.map(|record| -> anyhow::Result<PlanSnapshot> {
            let json: String = record.try_get("plan_json")?;
            serde_json::from_str(&json).context("deserialize plan snapshot")
        })
        .transpose()
    }

    async fn current_watermark(&self) -> anyhow::Result<VerificationWatermark> {
        let current_snapshot = self.load_current_snapshot().await?;

        let receipt_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM receipts;")
            .fetch_one(&self.pool)
            .await?;
        let latest_receipt_id: Option<String> = sqlx::query_scalar(
            "SELECT receipt_id FROM receipts ORDER BY recorded_at DESC, rowid DESC LIMIT 1;",
        )
        .fetch_optional(&self.pool)
        .await?;
        let open_gap_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM capability_gaps WHERE status = 'Open';")
                .fetch_one(&self.pool)
                .await?;
        let gap_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM capability_gaps;")
            .fetch_one(&self.pool)
            .await?;
        let recodification_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM recodifications;")
            .fetch_one(&self.pool)
            .await?;

        Ok(VerificationWatermark {
            plan_id: current_snapshot
                .as_ref()
                .map(|snapshot| snapshot.plan.id.to_string()),
            plan_snapshot_id: current_snapshot
                .as_ref()
                .map(|snapshot| snapshot.snapshot_id.to_string()),
            plan_version: current_snapshot
                .as_ref()
                .map(|snapshot| snapshot.plan.version)
                .unwrap_or_default(),
            receipt_count,
            latest_receipt_id,
            open_gap_count,
            gap_count,
            recodification_count,
        })
    }

    async fn save_plan_snapshot(&self, snapshot: &PlanSnapshot) -> anyhow::Result<()> {
        let plan_json = serde_json::to_string(snapshot)?;
        let mut tx = self.pool.begin().await?;

        sqlx::query(
            r#"
            INSERT INTO plan_snapshots (
                snapshot_id,
                plan_id,
                plan_version,
                recorded_at,
                rationale,
                source_gap_id,
                plan_json
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7);
            "#,
        )
        .bind(snapshot.snapshot_id.to_string())
        .bind(snapshot.plan.id.to_string())
        .bind(snapshot.plan.version)
        .bind(snapshot.recorded_at.to_rfc3339())
        .bind(&snapshot.rationale)
        .bind(snapshot.source_gap_id.map(|id| id.to_string()))
        .bind(plan_json)
        .execute(&mut *tx)
        .await?;

        Self::set_metadata_in_conn(&mut tx, "current_plan_id", &snapshot.plan.id.to_string())
            .await?;
        Self::set_metadata_in_conn(
            &mut tx,
            "current_plan_version",
            &snapshot.plan.version.to_string(),
        )
        .await?;
        Self::set_metadata_in_conn(
            &mut tx,
            "current_plan_snapshot_id",
            &snapshot.snapshot_id.to_string(),
        )
        .await?;

        tx.commit().await?;
        Ok(())
    }

    async fn append_receipt(&self, receipt: &Receipt) -> anyhow::Result<()> {
        let receipt_json = serde_json::to_string(receipt)?;

        sqlx::query(
            r#"
            INSERT INTO receipts (
                receipt_id,
                plan_id,
                unit,
                recorded_at,
                contradicted,
                receipt_json
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6);
            "#,
        )
        .bind(receipt.id.to_string())
        .bind(receipt.plan_id.to_string())
        .bind(format!("{:?}", receipt.unit))
        .bind(receipt.recorded_at.to_rfc3339())
        .bind(receipt.contradicted.as_deref())
        .bind(receipt_json)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn list_recent_receipts(&self, limit: usize) -> anyhow::Result<Vec<Receipt>> {
        let rows = sqlx::query(
            r#"
            SELECT receipt_json
            FROM receipts
            ORDER BY recorded_at DESC, rowid DESC
            LIMIT ?1;
            "#,
        )
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| -> anyhow::Result<Receipt> {
                let json: String = row.try_get("receipt_json")?;
                serde_json::from_str(&json).context("deserialize receipt")
            })
            .collect()
    }

    async fn record_gap(&self, gap: &CapabilityGap) -> anyhow::Result<()> {
        let gap_json = serde_json::to_string(gap)?;

        sqlx::query(
            r#"
            INSERT INTO capability_gaps (
                gap_id,
                status,
                discovered_at,
                last_touched_at,
                title,
                gap_json
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6)
            ON CONFLICT(gap_id) DO UPDATE SET
                status = excluded.status,
                last_touched_at = excluded.last_touched_at,
                title = excluded.title,
                gap_json = excluded.gap_json;
            "#,
        )
        .bind(gap.id.to_string())
        .bind(format!("{:?}", gap.status))
        .bind(gap.discovered_at.to_rfc3339())
        .bind(gap.last_touched_at.to_rfc3339())
        .bind(&gap.title)
        .bind(gap_json)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn list_capability_gaps(
        &self,
        status: Option<CapabilityGapStatus>,
    ) -> anyhow::Result<Vec<CapabilityGap>> {
        let rows = if let Some(status) = status {
            sqlx::query(
                r#"
                SELECT gap_json
                FROM capability_gaps
                WHERE status = ?1
                ORDER BY last_touched_at DESC, rowid DESC;
                "#,
            )
            .bind(format!("{:?}", status))
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query(
                r#"
                SELECT gap_json
                FROM capability_gaps
                ORDER BY last_touched_at DESC, rowid DESC;
                "#,
            )
            .fetch_all(&self.pool)
            .await?
        };

        rows.into_iter()
            .map(|row| -> anyhow::Result<CapabilityGap> {
                let json: String = row.try_get("gap_json")?;
                serde_json::from_str(&json).context("deserialize capability gap")
            })
            .collect()
    }

    async fn append_recodification(&self, recodification: &Recodification) -> anyhow::Result<()> {
        let recodification_json = serde_json::to_string(recodification)?;

        sqlx::query(
            r#"
            INSERT INTO recodifications (
                recodification_id,
                plan_id,
                recorded_at,
                recodification_json
            )
            VALUES (?1, ?2, ?3, ?4);
            "#,
        )
        .bind(recodification.id.to_string())
        .bind(recodification.plan_id.to_string())
        .bind(recodification.recorded_at.to_rfc3339())
        .bind(recodification_json)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn list_recent_recodifications(
        &self,
        limit: usize,
    ) -> anyhow::Result<Vec<Recodification>> {
        let rows = sqlx::query(
            r#"
            SELECT recodification_json
            FROM recodifications
            ORDER BY recorded_at DESC, rowid DESC
            LIMIT ?1;
            "#,
        )
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| -> anyhow::Result<Recodification> {
                let json: String = row.try_get("recodification_json")?;
                serde_json::from_str(&json).context("deserialize recodification")
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use splcw_core::{CapabilityGapStatus, Invariant, PlanModule, SplcwUnit};
    use tempfile::tempdir;
    use uuid::Uuid;

    use super::*;

    fn sample_plan(version: i64) -> SufficientPlan {
        SufficientPlan {
            id: Uuid::new_v4(),
            version,
            objective: "Pilot the desktop and preserve sufficiency".into(),
            constraints: vec!["Do not lose state".into()],
            invariants: vec![Invariant {
                key: "receipts".into(),
                description: "Every action produces a receipt".into(),
            }],
            modules: vec![PlanModule {
                key: "observe".into(),
                description: "Capture state before acting".into(),
                success_checks: vec!["Fresh screenshot".into()],
                reveal_response: "Recodify observation seam".into(),
            }],
            active_module: "observe".into(),
            recodification_rule: "If insufficiency is revealed, encode it into the next plan."
                .into(),
            updated_at: Utc::now(),
        }
    }

    #[tokio::test]
    async fn sqlite_store_round_trips_plan_receipts_gaps_and_recodifications() {
        let temp = tempdir().unwrap();
        let db_path = temp.path().join("state.db");
        let store = SqliteStateStore::connect(&db_path).await.unwrap();

        let plan = sample_plan(3);
        let snapshot = PlanSnapshot {
            snapshot_id: Uuid::new_v4(),
            plan: plan.clone(),
            rationale: "Initial sufficient plan".into(),
            source_gap_id: None,
            recorded_at: Utc::now(),
        };
        store.save_plan_snapshot(&snapshot).await.unwrap();

        let loaded_plan = store.load_plan().await.unwrap().unwrap();
        assert_eq!(loaded_plan, plan);

        let receipt = Receipt {
            id: Uuid::new_v4(),
            plan_id: plan.id,
            unit: SplcwUnit::Sculptor,
            observed: "Codex window visible".into(),
            attempted: "Focused desktop session".into(),
            changed: "Window is foreground".into(),
            contradicted: None,
            enabled_next: "Can issue input".into(),
            recorded_at: Utc::now(),
        };
        store.append_receipt(&receipt).await.unwrap();
        assert_eq!(
            store.list_recent_receipts(10).await.unwrap(),
            vec![receipt.clone()]
        );

        let gap = CapabilityGap {
            id: Uuid::new_v4(),
            title: "Thread selection lacks selector stability".into(),
            revealed_by: receipt.id.to_string(),
            permanent_fix_target: "Add robust thread rail selectors".into(),
            status: CapabilityGapStatus::Open,
            discovered_at: Utc::now(),
            last_touched_at: Utc::now(),
            notes: vec!["Need non-cursor fallback".into()],
        };
        store.record_gap(&gap).await.unwrap();
        assert_eq!(
            store
                .list_capability_gaps(Some(CapabilityGapStatus::Open))
                .await
                .unwrap(),
            vec![gap.clone()]
        );

        let recodification = Recodification {
            id: Uuid::new_v4(),
            plan_id: plan.id,
            triggered_by_gap: Some(gap.id),
            prior_plan_version: 3,
            new_plan_version: 4,
            rationale: "Promote selector acquisition into the plan".into(),
            verified_by: vec!["receipt".into()],
            recorded_at: Utc::now(),
        };
        store.append_recodification(&recodification).await.unwrap();
        assert_eq!(
            store.list_recent_recodifications(10).await.unwrap(),
            vec![recodification]
        );
    }

    #[tokio::test]
    async fn in_memory_store_boots_and_migrates() {
        let store = SqliteStateStore::connect_in_memory().await.unwrap();
        assert!(store.load_current_snapshot().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn save_plan_snapshot_rolls_back_if_metadata_write_fails() {
        let temp = tempdir().unwrap();
        let db_path = temp.path().join("state.db");
        let store = SqliteStateStore::connect(&db_path).await.unwrap();

        sqlx::query("DROP TABLE metadata;")
            .execute(store.pool())
            .await
            .unwrap();

        let snapshot = PlanSnapshot {
            snapshot_id: Uuid::new_v4(),
            plan: sample_plan(9),
            rationale: "Should roll back on metadata failure".into(),
            source_gap_id: None,
            recorded_at: Utc::now(),
        };

        let error = store.save_plan_snapshot(&snapshot).await.unwrap_err();
        assert!(
            format!("{error:#}")
                .to_ascii_lowercase()
                .contains("metadata")
        );

        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM plan_snapshots;")
            .fetch_one(store.pool())
            .await
            .unwrap();
        assert_eq!(count, 0);
    }
}
