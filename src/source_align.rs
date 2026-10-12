use crate::source_index::{subject_key, IndexEntry};

/// The largest alignment table built: about 8,000 messages against 8,000
/// index entries, twice the busiest month there is, at two bytes a cell.
const MAX_CELLS: usize = 64_000_000;

/// Pairs each message of a raw file with its page at the source. `raw_keys`
/// holds the `subject_key` of every message, in file order; `index` is the
/// period's index in page-number order. The result has one entry per raw
/// message: its page number, or `None` when it has no partner.
///
/// When both sides have the same length and agree on every subject, the i-th
/// message is the i-th page. Otherwise the two are aligned as sequences, on
/// their longest common subsequence of subjects, so that one junk or missing
/// message costs one link and not every link after it.
pub fn align(raw_keys: &[String], index: &[IndexEntry]) -> Vec<Option<String>> {
    let index_keys: Vec<String> = index.iter().map(|e| subject_key(&e.subject)).collect();
    if raw_keys.len() == index_keys.len() && raw_keys.iter().zip(&index_keys).all(|(a, b)| a == b) {
        return index.iter().map(|e| Some(e.number.clone())).collect();
    }

    let (n, m) = (raw_keys.len(), index_keys.len());
    let mut out = vec![None; n];
    if (n + 1).saturating_mul(m + 1) > MAX_CELLS {
        eprintln!(
            "Warning: {} messages against {} index entries is too many to align, leaving them unlinked",
            n, m
        );
        return out;
    }

    // table[i * width + j] is the length of the longest common subsequence of
    // raw_keys[i..] and index_keys[j..]. A u16 holds it: MAX_CELLS keeps the
    // shorter side under 8,000.
    let width = m + 1;
    let mut table = vec![0u16; (n + 1) * width];
    for i in (0..n).rev() {
        for j in (0..m).rev() {
            table[i * width + j] = if raw_keys[i] == index_keys[j] {
                table[(i + 1) * width + j + 1] + 1
            } else {
                table[(i + 1) * width + j].max(table[i * width + j + 1])
            };
        }
    }

    // Walk forward through both, pairing equal subjects and otherwise
    // stepping over whichever side loses less by it.
    let (mut i, mut j) = (0, 0);
    while i < n && j < m {
        if raw_keys[i] == index_keys[j] {
            out[i] = Some(index[j].number.clone());
            i += 1;
            j += 1;
        } else if table[(i + 1) * width + j] >= table[i * width + j + 1] {
            i += 1;
        } else {
            j += 1;
        }
    }
    out
}
