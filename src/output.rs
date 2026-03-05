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
