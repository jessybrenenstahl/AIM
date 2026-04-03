use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum SplcwUnit {
    Warden,
    Captive,
    Logician,
    Poet,
    Sculptor,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Invariant {
    pub key: String,
    pub description: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PlanModule {
    pub key: String,
    pub description: String,
    pub success_checks: Vec<String>,
    pub reveal_response: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SufficientPlan {
    pub id: Uuid,
    pub version: i64,
    pub objective: String,
    pub constraints: Vec<String>,
    pub invariants: Vec<Invariant>,
    pub modules: Vec<PlanModule>,
    pub active_module: String,
    pub recodification_rule: String,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PlanSnapshot {
    pub snapshot_id: Uuid,
    pub plan: SufficientPlan,
    pub rationale: String,
    pub source_gap_id: Option<Uuid>,
    pub recorded_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum CapabilityGapStatus {
    Open,
    InRecodification,
    Closed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CapabilityGap {
    pub id: Uuid,
    pub title: String,
    pub revealed_by: String,
    pub permanent_fix_target: String,
    pub status: CapabilityGapStatus,
    pub discovered_at: DateTime<Utc>,
    pub last_touched_at: DateTime<Utc>,
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Receipt {
    pub id: Uuid,
    pub plan_id: Uuid,
    pub unit: SplcwUnit,
    pub observed: String,
    pub attempted: String,
    pub changed: String,
    pub contradicted: Option<String>,
    pub enabled_next: String,
    pub recorded_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Recodification {
    pub id: Uuid,
    pub plan_id: Uuid,
    pub triggered_by_gap: Option<Uuid>,
    pub prior_plan_version: i64,
    pub new_plan_version: i64,
    pub rationale: String,
    pub verified_by: Vec<String>,
    pub recorded_at: DateTime<Utc>,
}
