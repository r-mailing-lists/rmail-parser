//! Helpers shared by the tests that exercise source pages. Nothing here
//! touches the network: `FakeFetcher` stands in for the source.
#![allow(dead_code)]

use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

use rmail_parser::attachment_fetch::AttachmentFetcher;
use rmail_parser::source_fetch::{Fetched, Fetcher};

/// Where the fake source pretends to be. Nothing is ever sent there.
pub const BASE: &str = "https://lists.example.org/pipermail/r-devel";

pub fn fixture_path(rel: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(rel)
}

pub fn fixture(rel: &str) -> String {
    fs::read_to_string(fixture_path(rel)).unwrap()
}

/// A fresh, empty directory under Cargo's own scratch space for tests, so
/// whatever a test leaves behind is removed by `cargo clean`.
pub fn scratch_dir(name: &str) -> PathBuf {
    let dir =
        Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("{}-{}", std::process::id(), name));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    dir
}

/// A source that serves canned pages and remembers what it was asked for.
/// A URL it has no page for is a 404.
#[derive(Default)]
pub struct FakeFetcher {
    pages: HashMap<String, String>,
    /// Every URL asked for, in order.
    pub asked: Vec<String>,
    /// Requests after this many fail, as if the source had gone down.
    pub down_after: Option<usize>,
}

impl FakeFetcher {
    pub fn new() -> Self {
        Self::default()
    }

    /// Serves `body` at `BASE/<path>`.
    pub fn serve(&mut self, path: &str, body: &str) {
        self.pages
            .insert(format!("{}/{}", BASE, path), body.to_string());
    }

    /// Serves `tests/fixtures/pipermail/<path>` at `BASE/<path>`.
    pub fn serve_fixture(&mut self, path: &str) {
        self.serve(path, &fixture(&format!("pipermail/{}", path)));
    }

    /// The paths asked for so far, without `BASE/` in front.
    pub fn asked_paths(&self) -> Vec<&str> {
        self.asked
            .iter()
            .map(|url| url.strip_prefix(BASE).unwrap().trim_start_matches('/'))
            .collect()
    }
}

impl Fetcher for FakeFetcher {
    fn get(&mut self, url: &str) -> anyhow::Result<Fetched> {
        self.asked.push(url.to_string());
        if let Some(limit) = self.down_after {
            if self.asked.len() > limit {
                anyhow::bail!("connection refused");
            }
        }
        Ok(match self.pages.get(url) {
            Some(body) => Fetched::Body(body.clone()),
            None => Fetched::NotFound,
        })
    }
}

/// One message as the source's text file has it. Its Message-ID is
/// `<m<n>@example.org>`.
pub fn raw_message(n: usize, subject: &str, body: &str) -> String {
    format!(
        "From user{n} @end|ng |rom ex@mp|e@org  Mon Feb  2 12:00:00 2026\n\
         From: user{n} @end|ng |rom ex@mp|e@org (User {n})\n\
         Date: Mon, 2 Feb 2026 12:{:02}:00 +0000\n\
         Subject: {subject}\n\
         Message-ID: <m{n}@example.org>\n\
         \n\
         {body}\n\
         \n",
        n % 60
    )
}

/// An index page in the source's markup, one entry per `(number, subject)`.
pub fn index_page(entries: &[(&str, &str)]) -> String {
    let mut html = String::from("<HTML><BODY>\n<ul>\n\n");
    for (number, subject) in entries {
        html.push_str(&format!(
            "<LI><A HREF=\"{number}.html\">{subject}\n\
             </A><A NAME=\"{}\">&nbsp;</A>\n\
             <I>Someone\n\
             </I>\n\n",
            number.trim_start_matches('0')
        ));
    }
    html.push_str("</ul>\n</BODY></HTML>\n");
    html
}

/// A message page in the source's markup. `message_id` goes into the reply
/// link without angle brackets; leave it empty for a page from the 2000s.
pub fn article_page(message_id: &str, body_html: &str) -> String {
    let in_reply_to = if message_id.is_empty() {
        String::new()
    } else {
        format!("%3C{}%3E", message_id.replace('@', "%40"))
    };
    format!(
        "<HTML><HEAD>\n\
         <LINK REL=\"made\" HREF=\"mailto:r-devel%40r-project.org?Subject=Re%3A%20x&In-Reply-To={in_reply_to}\">\n\
         </HEAD><BODY>\n\
         <!--beginarticle-->\n\
         <PRE>{body_html}\n\
         </PRE>\n\
         <!--endarticle-->\n\
         </BODY></HTML>\n"
    )
}

/// A source of scrubbed texts that serves bytes by URL, answers 404 for a URL
/// it has nothing for, fails the URLs it is told to, and remembers what it
/// was asked for.
#[derive(Default)]
pub struct FakeAttachments {
    files: HashMap<String, Vec<u8>>,
    failing: HashSet<String>,
    /// Every URL asked for, in order.
    pub asked: Vec<String>,
}

impl FakeAttachments {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn serve(&mut self, url: &str, bytes: &[u8]) {
        self.files.insert(url.to_string(), bytes.to_vec());
    }

    pub fn fail(&mut self, url: &str) {
        self.failing.insert(url.to_string());
    }

    pub fn stop_failing(&mut self) {
        self.failing.clear();
    }
}

impl AttachmentFetcher for FakeAttachments {
    fn get_bytes(&mut self, url: &str) -> anyhow::Result<Option<Vec<u8>>> {
        self.asked.push(url.to_string());
        if self.failing.contains(url) {
            anyhow::bail!("connection refused");
        }
        Ok(self.files.get(url).cloned())
    }
}

/// The notice Mailman leaves for an unnamed scrubbed text.
pub fn scrubbed_notice(url: &str) -> String {
    format!(
        "An embedded and charset-unspecified text was scrubbed...\nName: not available\nURL: <{}>\n",
        url
    )
}
