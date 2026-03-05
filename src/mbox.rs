/// Split a raw mbox string into individual message strings.
/// Messages are separated by lines starting with "From " (the mbox format separator).
pub fn split_mbox(content: &str) -> Vec<&str> {
    let mut messages = Vec::new();
    let mut positions: Vec<usize> = Vec::new();

    // First message starts at position 0 if content starts with "From "
    if content.starts_with("From ") {
        positions.push(0);
    }

    // Find subsequent message boundaries: "\nFrom " pattern
    let bytes = content.as_bytes();
    let pattern = b"\nFrom ";
    for i in 0..bytes.len().saturating_sub(pattern.len()) {
        if &bytes[i..i + pattern.len()] == pattern {
            positions.push(i + 1); // +1 to skip the \n
        }
    }

    // Extract messages between positions
    for (idx, &pos) in positions.iter().enumerate() {
        let end = if idx + 1 < positions.len() {
            positions[idx + 1] - 1 // -1 to exclude the \n before next "From "
        } else {
            content.len()
        };
        let msg = content[pos..end].trim();
        if !msg.is_empty() {
            messages.push(msg);
        }
    }

    messages
}
