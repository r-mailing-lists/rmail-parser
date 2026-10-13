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

`rejected` has an entry for every page that was fetched and named another Message-ID than the message it had been paired with: the page number, and the Message-ID the page names. That message loses its link, and when the period is indexed again the page is linked only to the message it names. A file without the `rejected` field was written by a version before 0.10.0. `parse --sources` refuses it, and so does `sources` when it reads it; a run limited by `--max-periods` does not read the files of older periods. Delete such a file and let `sources` write it again.

With `--max-periods 2` the command looks at the two newest periods that have messages and leaves every older one exactly as it is. A daily run passes it, so that it never walks into a list's history; a run without it takes every period, newest first, until its budget is spent.

When it ends the command prints what it did, to stderr, one line each in this order:

```
Periods indexed:               N
Pages fetched:                 N
Characters restored:           N
? left as they were:           N
Pages naming another message:  N
Characters beyond U+FFFF seen: N
Requests that failed:          N
Pages the source did not have: N
Indexes ahead of the raw file: N
Messages in periods looked at: N
Messages without a link:       N
Rejected pairings on file:     N
Older periods not looked at:   N
```

The first nine count what this run did. The next three count what the period files say after the run, for the periods the run looked at and has a file for: positions, positions with no page, and rejected pairings. `Older periods not looked at` is 0 unless `--max-periods` is given, and a limited run that stops early still counts the periods it would have left alone. The report ends with one more line: `Stopped early: <reason>`, `Every period is up to date`, `The periods looked at are up to date` (a limited run that fetched everything it looked at) or `Not everything was fetched; the next run will ask again`.

A run limited by `--max-periods` does not look at older periods again. After a closed period's raw file has been downloaded again, it may have gained or lost messages, so run `sources` once without `--max-periods` before the next `parse --sources`. Otherwise `parse --sources` applies that period's old page numbers to shifted positions when the file has gained messages, and ignores the period file, so the whole period loses its links, when it has lost some.

The command makes one request at a time, identifies itself as `rmail-parser (+https://github.com/r-mailing-lists)`, and stops when its budget is spent or three requests in a row fail. A request the source answers with "not found" counts as a failed one, and neither is ever written down: the period or the message is left as it was and asked for again on the next run. Stopping early is not an error: the command exits 0, every file it wrote is valid, and the next run picks up what is missing. A character is restored only when the source's own page has it. Nothing is guessed.

### `parse --sources`

```bash
rmail-parser parse --input raw/ --output processed/ --list r-devel \
    --sources sources/ --base-url https://stat.ethz.ch/pipermail/r-devel/
```

`--sources` and `--base-url` must be given together. Each message with a known page gains a `source_url` of the form `<base-url>/<period>/<number>.html`, and its body and snippet carry the restored characters. Without the two flags, or for a raw file that has no `sources/` file, the output is what it has always been and has no `source_url` field.

## Hiding addresses

```bash
rmail-parser parse --input raw/ --output processed/ --list r-sig-phylo --hide-addresses
```

`--hide-addresses` is for a list whose archive only its members can read: its messages may be published, the addresses in them may not. With the flag, no file a run writes holds a mailbox address. Without it, the output is what it has always been.

An address is hidden in each of the three forms these archives hold: `mailbox at domain.tld`, `mailbox @end|ng |rom domain` and `mailbox@domain.tld`.

| Where | With `--hide-addresses` |
|---|---|
| `body_plain`, `subject` | Every address becomes `[address hidden]`. Punctuation after it stays: `Jane Doe <jane at uni.edu> wrote:` becomes `Jane Doe <[address hidden]> wrote:`. |
| `subject_clean`, `body_snippet` | Made from the cleaned subject and the cleaned text. |
| `from_name` | A name that is an address as a whole becomes its mailbox name: `jane at uni.edu` becomes `jane`. A name with an address among other words gets `[address hidden]`. A name with nothing left to show becomes `unknown`. |
| Threads, `contributors.json` | Made from the cleaned messages, so participants, names and slugs follow. A name taken from `--aliases` is cleaned like a `from_name`. |

Three things are left as they are:

- The address of an R mailing list: a mailbox name that starts with `r-`, with `bioc` or with the value of `--list`, whatever the case, at `r-project.org`, `stat.math.ethz.ch`, `lists.r-project.org` or `lists.r-forge.r-project.org`.
- A web address written in a sentence, in the first form only: what follows ` at ` starts with `www.` or is directly followed by `/`, as in `the code at github.com/user/repo`.
- In the first form only, what follows one of the words `look`, `looking`, `available`, `found`, `here`, `located`, `hosted`, `is`, `are`, `was`, `it`, `and`, `up`, `now`, `left`, `indexed` and `see`, which are not mailbox names.

Anything else that reads as an address is hidden, also where it is none: `tree at edge.length`, which is R code, is. A hidden word is a small loss, and a published address cannot be taken back.

The flag never changes `from_email_hash`, which is the hash of the real address and is what joins a person's messages across lists. Nor does it change `id`, `message_id`, `in_reply_to`, `references`, `thread_id`, the dates, `month` or `source_url`. A Message-ID has the shape of an address and is kept: replies are matched by it. With `--sources`, the text is cleaned after the source page has restored it.

## Development

`cargo test` runs everything. No test touches the network: the fetching code sits behind a trait and is tested against saved pages in `tests/fixtures/`.

## Releasing

Pushing a tag that starts with `v` builds the Linux binary and publishes it as the latest release, which is what the archive repositories download on their next run. Tag the commit on `main` that should ship by its hash or as `origin/main`: this repository also has a tag called `main`, so the bare name is ambiguous. Do not start the workflow by hand, because a manual run names the release after the branch it was started on.
