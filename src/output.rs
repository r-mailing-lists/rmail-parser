use std::collections::BTreeMap;

use crate::message::Message;
use crate::threading::Thread;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct MonthArchive {
    pub list: String,
    pub description: String,
    pub month: String,
    pub messages: Vec<Message>,
    pub threads: Vec<Thread>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ListMeta {
    pub list: String,
    pub description: String,
    pub source_url: String,
    pub total_messages: usize,
    pub first_message: String,
    pub last_message: String,
    pub total_threads: usize,
    pub months_available: Vec<String>,
}

/// Per-list contributor entry written to contributors.json.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContributorEntry {
    pub name: String,
    pub slug: String,
    pub email_hash: String,
    pub message_count: usize,
    pub first_date: Option<String>,
    pub last_date: Option<String>,
    pub yearly_activity: BTreeMap<String, usize>,
}

/// Aggregated contributor entry across multiple lists (written by `aggregate`).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AggregatedContributor {
    pub name: String,
    pub slug: String,
    pub email_hash: String,
    pub message_count: usize,
    pub lists: Vec<ListCount>,
    pub first_date: Option<String>,
    pub last_date: Option<String>,
    pub yearly_activity: BTreeMap<String, usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListCount {
    pub slug: String,
    pub count: usize,
}

/// Per-list index mapping msg_id → month and thread_id → month.
#[derive(Debug, Serialize, Deserialize)]
pub struct ListIndex {
    pub messages: BTreeMap<String, String>,
    pub threads: BTreeMap<String, String>,
}
