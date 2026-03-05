use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::message::Message;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Thread {
    pub id: String,               // "thread-<12-char-sha256-of-root-message-id>"
    pub subject: String,          // cleaned subject from root message
    pub message_count: usize,
    pub participants: Vec<String>, // unique author names, sorted
    pub started: String,          // RFC 3339 date of root message
    pub last_reply: String,       // RFC 3339 date of last message
    pub root_message_id: String,  // id field of root message
}

/// Generates a thread ID by hashing the root message's message_id with SHA-256
/// and taking the first 12 hex characters.
fn generate_thread_id(root_message_id: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(root_message_id.as_bytes());
    let result = hasher.finalize();
    let hex_str = hex::encode(result);
    format!("thread-{}", &hex_str[..12])
}

/// Finds the root index of a message by walking up the parent_map chain.
/// Includes cycle detection via a visited set.
fn find_root(index: usize, parent_map: &HashMap<usize, usize>) -> usize {
    let mut current = index;
    let mut visited = HashSet::new();
    while let Some(&parent) = parent_map.get(&current) {
        if !visited.insert(current) {
            // Cycle detected: treat current as root
            return current;
        }
        current = parent;
    }
    current
}

/// Calculates the thread depth of a message (how many parents up to root).
/// Includes cycle detection.
fn calc_depth(index: usize, parent_map: &HashMap<usize, usize>) -> u32 {
    let mut depth: u32 = 0;
    let mut current = index;
    let mut visited = HashSet::new();
    while let Some(&parent) = parent_map.get(&current) {
        if !visited.insert(current) {
            // Cycle detected: stop counting
            break;
        }
        depth += 1;
        current = parent;
    }
    depth
}

/// Reconstructs threads from a list of messages.
///
/// This function:
/// 1. Builds a `message_id -> index` map from all messages
/// 2. For each message, finds its parent via `in_reply_to` then `references` (last one)
/// 3. Builds a parent_map: `child_index -> parent_index`
/// 4. Groups messages by finding the root of each chain (following parent_map to the top)
/// 5. Calculates thread depth for each message
/// 6. Creates Thread summaries for each group
/// 7. Sets `thread_id` and `thread_depth` on each Message in place
/// 8. Returns sorted threads (by start date)
pub fn reconstruct_threads(messages: &mut Vec<Message>) -> Vec<Thread> {
    if messages.is_empty() {
        return Vec::new();
    }

    // Step 1: Build message_id -> index map
    let mut msg_id_to_index: HashMap<&str, usize> = HashMap::new();
    for (i, msg) in messages.iter().enumerate() {
        msg_id_to_index.insert(&msg.message_id, i);
    }

    // Step 2 & 3: Build parent_map (child_index -> parent_index)
    let mut parent_map: HashMap<usize, usize> = HashMap::new();
    for (i, msg) in messages.iter().enumerate() {
        // Try in_reply_to first
        if let Some(ref reply_to) = msg.in_reply_to {
            if let Some(&parent_idx) = msg_id_to_index.get(reply_to.as_str()) {
                if parent_idx != i {
                    parent_map.insert(i, parent_idx);
                    continue;
                }
            }
        }

        // Fall back to last entry in references
        if let Some(last_ref) = msg.references.last() {
            if let Some(&parent_idx) = msg_id_to_index.get(last_ref.as_str()) {
                if parent_idx != i {
                    parent_map.insert(i, parent_idx);
                }
            }
        }
    }

    // Step 4: Group messages by root
    let mut thread_groups: HashMap<usize, Vec<usize>> = HashMap::new();
    for i in 0..messages.len() {
        let root = find_root(i, &parent_map);
        thread_groups.entry(root).or_default().push(i);
    }

    // Step 5 & 6 & 7: Calculate depths, build Thread summaries, set fields on messages
    let mut threads: Vec<Thread> = Vec::new();

    for (&root_idx, member_indices) in &thread_groups {
        let root_msg = &messages[root_idx];
        let thread_id = generate_thread_id(&root_msg.message_id);
        let subject = root_msg.subject_clean.clone();
        let started = root_msg.date.to_rfc3339();
        let root_message_id = root_msg.id.clone();

        // Collect participants and find last reply date
        let mut participants_set: HashSet<String> = HashSet::new();
        let mut last_reply_date = root_msg.date;

        for &idx in member_indices {
            let msg = &messages[idx];
            participants_set.insert(msg.from_name.clone());
            if msg.date > last_reply_date {
                last_reply_date = msg.date;
            }
        }

        let mut participants: Vec<String> = participants_set.into_iter().collect();
        participants.sort();

        let thread = Thread {
            id: thread_id.clone(),
            subject,
            message_count: member_indices.len(),
            participants,
            started,
            last_reply: last_reply_date.to_rfc3339(),
            root_message_id,
        };

        threads.push(thread);
    }

    // Now set thread_id and thread_depth on each message
    // We need to rebuild the root -> thread_id mapping
    let root_to_thread_id: HashMap<usize, String> = thread_groups
        .keys()
        .map(|&root_idx| {
            let thread_id = generate_thread_id(&messages[root_idx].message_id);
            (root_idx, thread_id)
        })
        .collect();

    for i in 0..messages.len() {
        let root = find_root(i, &parent_map);
        let depth = calc_depth(i, &parent_map);
        messages[i].thread_id = root_to_thread_id[&root].clone();
        messages[i].thread_depth = depth;
    }

    // Step 8: Sort threads by start date
    threads.sort_by(|a, b| a.started.cmp(&b.started));

    threads
}
