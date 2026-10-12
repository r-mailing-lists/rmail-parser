# rmail-parser

Parses the monthly text files of a Mailman pipermail archive into structured JSON: one file per month with every message and thread, plus list statistics and a contributor index. It is the parser behind the [R mailing list archives](https://r-mailing-lists.thecoatlessprofessor.com).

## Install

Download the Linux binary from the latest release:

```bash
curl -sL -o rmail-parser \
  https://github.com/r-mailing-lists/rmail-parser/releases/latest/download/rmail-parser-linux-x86_64
chmod +x rmail-parser
```

Or build it from a checkout with `cargo build --release`.

## Commands

| Command | What it does |
|---|---|
| `parse` | Parses raw `.mbox` files into one JSON file per month, with stats unless `--no-stats` is given. |
| `sources` | Fetches the source's index and message pages and records them in `sources/` files. |
| `stats` | Regenerates the stats files from already-parsed JSON. |
| `aggregate` | Merges per-list `contributors.json` files into one. |
| `hash-email` | Prints the de-obfuscated form and the hash of an email address. |

`rmail-parser <command> --help` lists every flag.

`rmail-parser --version` prints the version.

## Source pages

The text file pipermail offers for each period has no per-message identifiers, and it replaces most non-ASCII characters with `?` ("Bessel?s correction", "Universit?t"). The source also publishes one HTML page per message, and those pages have both. Two steps bring them into the output.

### `sources`

```bash
rmail-parser sources --raw raw/ --out sources/ \
    --base-url https://stat.ethz.ch/pipermail/r-devel/ \
    --max-requests 300 --delay-ms 1000
```

| Flag | Default | Meaning |
|---|---|---|
| `--raw` | required | Directory of raw `.mbox` files, each named after its period: `2026-October.mbox`, `2026q4.mbox` or `2026.mbox`. |
| `--out` | required | Directory the per-period files are written to. |
| `--base-url` | required | The list's archive at the source. |
| `--max-requests` | 300 | Requests one run may make. |
| `--delay-ms` | 1000 | Pause between requests, in milliseconds. |
| `--max-periods` | every period | Look at this many periods only, newest first. |

For each period, newest first, the command fetches `<period>/date.html` when the period has no file yet or its raw file has changed length, and pairs the raw file's messages with the index entries. It then fetches the page of every message whose body has a `?` between two letters. Each period gets one file:

```json
{
  "period": "2026-October",
  "articles": [
    "084714",
    "084715",
    null,
    "084716"
  ],
  "fixes": {
    "084715": "",
    "084716": "?’?ä"
  },
  "rejected": {
    "084717": "msg-084799@example.org"
  }
}
```

`articles[i]` is the page number of the i-th message in the raw file, or `null` where no page could be matched. `fixes` has an entry for every page that has been fetched: one character for each `?` in that message's body, in order, which is either the character to restore or `?` to leave it alone. An empty string means the page had nothing to restore.

`rejected` has an entry for every page that was fetched and named another Message-ID than the message it had been paired with: the page number, and the Message-ID the page names. That message loses its link, and when the period is indexed again the page is linked only to the message it names. A file without the `rejected` field was written by a version before 0.10.0 and is refused by both commands: delete it and let `sources` write it again.

With `--max-periods 2` the command looks at the two newest periods that have messages and leaves every older one exactly as it is. A daily run passes it, so that it never walks into a list's history; a run without it takes every period, newest first, until its budget is spent.

When it ends the command prints what it did: periods indexed, pages fetched, characters restored and left, pages that named another message, requests that failed, answers that were "not found", and indexes that listed more pages than the raw file has messages. For the periods it looked at it also prints how many messages there are, how many have no link, and how many rejected pairings are on file.

The command makes one request at a time, identifies itself as `rmail-parser (+https://github.com/r-mailing-lists)`, and stops when its budget is spent or three requests in a row fail. A request the source answers with "not found" counts as a failed one, and neither is ever written down: the period or the message is left as it was and asked for again on the next run. Stopping early is not an error: the command exits 0, every file it wrote is valid, and the next run picks up what is missing. A character is restored only when the source's own page has it. Nothing is guessed.

### `parse --sources`

```bash
rmail-parser parse --input raw/ --output processed/ --list r-devel \
    --sources sources/ --base-url https://stat.ethz.ch/pipermail/r-devel/
```

`--sources` and `--base-url` must be given together. Each message with a known page gains a `source_url` of the form `<base-url>/<period>/<number>.html`, and its body and snippet carry the restored characters. Without the two flags, or for a raw file that has no `sources/` file, the output is what it has always been and has no `source_url` field.

## Development

`cargo test` runs everything. No test touches the network: the fetching code sits behind a trait and is tested against saved pages in `tests/fixtures/`.

## Releasing

Pushing a tag that starts with `v` builds the Linux binary and publishes it as the latest release, which is what the archive repositories download on their next run. Tag the commit on `main` that should ship by its hash or as `origin/main`: this repository also has a tag called `main`, so the bare name is ambiguous. Do not start the workflow by hand, because a manual run names the release after the branch it was started on.
