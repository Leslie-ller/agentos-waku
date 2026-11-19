use serde::{Deserialize, Serialize};
use ts_rs::TS;

/// Which AgentOS persistent memory table a record belongs to.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub enum MemoryEntryKind {
    Knowledge,
    Note,
    Checkpoint,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct MemoryNoteRecord {
    pub session_id: String,
    pub session_title: Option<String>,
    pub project_path: String,
    pub note_id: String,
    pub scope: String,
    pub revision: i64,
    pub body: String,
    pub source_refs_json: String,
    pub updated_at: i64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct MemoryCheckpointRecord {
    pub checkpoint_id: String,
    pub session_id: String,
    pub session_title: Option<String>,
    pub project_path: String,
    pub payload_json: String,
    pub created_at: i64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct MemoryKnowledgeRecord {
    pub knowledge_id: String,
    pub scope: String,
    pub project_path: String,
    pub session_id: String,
    pub session_title: Option<String>,
    pub claim: String,
    pub applicability_json: String,
    pub evidence_json: String,
    pub status: String,
    pub created_at: i64,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct MemoryLibrarySnapshot {
    pub knowledge: Vec<MemoryKnowledgeRecord>,
    pub notes: Vec<MemoryNoteRecord>,
    pub checkpoints: Vec<MemoryCheckpointRecord>,
}
