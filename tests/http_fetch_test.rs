use std::io::{Read, Write};
use std::net::TcpListener;
use std::thread;

use rmail_parser::source_fetch::{Fetched, Fetcher, HttpFetcher};

/// Serves `body` once, as a 200 with `text/html`, on a port of its own, and
/// returns the URL of the page.
fn serve_once(body: Vec<u8>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}/021019.html", listener.local_addr().unwrap());
    thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut request = [0u8; 4096];
        let _ = stream.read(&mut request);
        let head = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );
        let _ = stream.write_all(head.as_bytes());
        let _ = stream.write_all(&body);
    });
    url
}

#[test]
fn test_a_page_over_ten_mb_is_read_whole() {
    // One message page of r-sig-mixed-models is past the 10 MB that ureq's
    // into_string() reads, and it carries a byte in another encoding.
    let mut body = vec![b'a'; 11 * 1024 * 1024];
    body[100] = 0xE9;
    let url = serve_once(body);
    match HttpFetcher::new(0).get(&url).unwrap() {
        Fetched::Body(text) => {
            assert_eq!(text.chars().count(), 11 * 1024 * 1024);
            assert_eq!(text.chars().nth(100), Some('\u{FFFD}'));
        }
        Fetched::NotFound => panic!("the page was there"),
    }
}

#[test]
fn test_a_page_over_the_limit_is_an_error_and_not_a_page() {
    let url = serve_once(vec![b'a'; 64 * 1024 * 1024 + 1]);
    let error = match HttpFetcher::new(0).get(&url) {
        Err(e) => e.to_string(),
        Ok(_) => panic!("a page over the limit was read"),
    };
    assert!(error.contains("over 64 MB"), "{}", error);
}
