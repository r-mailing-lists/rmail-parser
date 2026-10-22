use std::fs;

use rmail_parser::pipeline::read_file_lossy;

#[test]
fn test_file_mixing_utf8_and_latin1_messages_reads_each_line_in_its_own() {
    // A list's monthly file holds each message in the bytes it arrived in,
    // so one file can mix UTF-8 messages with Latin-1 ones and be valid in
    // neither. Each line is read in the encoding it is valid in.
    let dir = std::env::temp_dir().join(format!("rmail-read-file-{}", std::process::id()));
    fs::create_dir_all(&dir).unwrap();
    let path = dir.join("2016-March.mbox");
    let mut bytes = "Função em UTF-8\n".as_bytes().to_vec();
    bytes.extend_from_slice(b"Fun\xe7\xe3o em Latin-1\n");
    bytes.extend_from_slice("Não é\n".as_bytes());
    fs::write(&path, &bytes).unwrap();

    let text = read_file_lossy(&path).unwrap();
    fs::remove_dir_all(&dir).unwrap();

    assert_eq!(text, "Função em UTF-8\nFunção em Latin-1\nNão é\n");
}
