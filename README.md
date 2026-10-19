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
| `attachments` | Fetches the text Mailman scrubbed out of messages and records it in `attachments/` files. |
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

### `attachments`

Where Mailman scrubbed an unnamed text out of a message, it left a notice with the address of the file. This command fetches those files into one `attachments/<period>.json.gz` per raw file.

```bash
rmail-parser attachments --raw raw/ --out attachments/ \
    --base-url https://stat.ethz.ch/pipermail/r-devel/ \
    --max-requests 3000 --delay-ms 1000
```

| Flag | Default | Meaning |
|---|---|---|
| `--raw` | required | Directory of raw `.mbox` files. |
| `--out` | required | Directory the per-period files are written to. |
| `--base-url` | required | The list's archive at the source. Only addresses exactly under `<base-url>/attachments/` are fetched, over the scheme of the base URL. |
| `--max-requests` | 3000 | Requests one run may make. |
| `--delay-ms` | 1000 | Pause between requests, in milliseconds. |

Only notices at the start of a line, followed by `Name: not available` and an address of the form `YYYYMMDD/xxxxxxxx/attachment.ext`, count, and only those whose address is exactly `<base-url>/attachments/` followed by that path are fetched: an address with anything between the two, such as `../`, is refused and never asked for. Periods are taken oldest first. Text is stored as UTF-8, as it was fetched; bytes that are not valid UTF-8 are read as Windows-1252, and the file records which. Addresses in the text are stored as `user at domain`, as Mailman writes them in its own archive. A 404 is recorded as `"not found"` and a file over 1 MiB as `"too large"`, and neither is asked for again. A failed request records nothing and is asked for again on a later run, and so does an answer that is not the file: one that came by a redirect, or an HTML page for an address that does not end in `.html` or `.htm`. The run stops after 10 failures in a row, a 404 counting as one and a file that arrives starting the count again. A source that has moved its files answers 404 to everything, so when all ten are 404s the run first asks once more for a file known to be there: the last one it fetched with its text, or else one stored with its text in a period it has loaded. If that file comes, the source is there: the ten stay recorded as `"not found"` and the run goes on. If it does not come, if no file is known, or if an error is among the ten, the run stops and the 404s among them are not stored, so they are asked for again on a later run. That request counts toward `--max-requests`, prints `source checked after 10 not found in a row: it answers` (or `it does not answer`), and is counted in the report as `Source checks`. A run that stops early prints `warning: <reason>` and still exits 0. The report is printed one count per line.

### `parse --sources`

```bash
rmail-parser parse --input raw/ --output processed/ --list r-devel \
    --sources sources/ --base-url https://stat.ethz.ch/pipermail/r-devel/
```

`--sources` and `--base-url` must be given together. Each message with a known page gains a `source_url` of the form `<base-url>/<period>/<number>.html`, and its body and snippet carry the restored characters. Without the two flags, or for a raw file that has no `sources/` file, the output is what it has always been and has no `source_url` field.

### `parse --attachments`

```bash
rmail-parser parse --input raw/ --output processed/ --list r-devel \
    --attachments attachments/
```

Each notice that Mailman left where it scrubbed a text is replaced by the text `attachments` stored for it, with its addresses written as `user at domain`, its line ends made `\n` and its trailing whitespace trimmed. The body is then trimmed as every body is, and the snippet is made again. The message gains `restored_from`: the stored URL of each text put into it, in the order of the body. A message nothing was restored into has no `restored_from` field. A notice stays when its file is not stored, was recorded as `"not found"` or `"too large"`, holds only whitespace, or was fetched from another URL. Without the flag, or for a raw file with no `attachments/` file, the output is what it has always been. A damaged `attachments/` file stops the run and is named. With `--hide-addresses`, the restored text is cleaned too.

## Hiding addresses

```bash
rmail-parser parse --input raw/ --output processed/ --list r-sig-phylo --hide-addresses
```

`--hide-addresses` is for a list whose archive only its members can read: its messages may be published, the addresses in them may not. With the flag, `parse` hides every address it can read, in the forms listed here, and writes the identifiers of messages as hashes. What it is known to leave is listed at the end. Without the flag, the output is what it has always been.

### What is read as an address

A mailbox name, a separator, and a domain. The separator is one of these:

| Separator | Example |
|---|---|
| ` at `, Mailman's text form, on one line or, where the words look like an address, broken over a line end | `jane.doe at uni.edu` |
| One of the source's four lossy forms: ` @end\|ng \|rom `, ` @ending from `, ` m@iii@g oii `, ` m@ili@g off ` | `j@ne.doe m@iii@g oii uni.edu` |
| `@` | `jane.doe@uni.edu` |
| `%40` in a link, and `%2540` in a link inside a link | `.../options/r-sig-phylo/jane.doe%40uni.edu` |
| `(at)`, `[at]`, `<at>`, `{at}` and `--at--` in any letter case, with or without a blank on either side, and ` AT ` in capitals | `jane.doe (at) uni.edu` |

A domain is a letter or a digit, followed somewhere by a full stop and then a letter, so `uni.edu`, `uni.edu.2` and `uni.__edu` are domains and `0.05`, `5.30pm` and `.Rprofile` are not. It ends where a lossy separator or `--at--` starts, also where no blank stands between. After a lossy separator anything counts as the domain, because that form has lost most full stops.

A full stop of a domain may be spelled ` dot `, ` DOT `, `(dot)` or `[dot]`: `jane at uni dot edu`. The word counts only between two parts of a domain, never before its first part. Where the domain is spelled so, the name may be too, and its parts go with it: `jane dot doe at uni dot edu` is hidden as a whole, and in `jane dot doe at uni.edu` only `doe at uni.edu` is.

` at ` and its spellings need a name before them and a domain after them. So does `@`, with one exception. A `@` at the start of a line, after a blank, or after an opening bracket or quotation mark has no name before it and is not read as an address: `@loc.names` is a slot of an R object and `@name.bsky.social` a handle, and both stay. Where anything else stands directly before the sign, as the closing quotation mark does in `"jane doe"@uni.edu`, a name is there that cannot be read, and the domain is hidden. After `%40` a domain is hidden with or without a name before it. A lossy separator is hidden with whatever stands on either side of it. After a sign, written or escaped, that has no name before it, ` dot ` is a word and no part of the domain: in `"jane doe"@uni.edu dot org` the domain is `uni.edu`.

A mail program that wraps a long line may end it with `=` and start the next with the sign. Such a sign is not at the start of a line: the name before the `=` is read as its name, and the `=` and the line end stay where they are. With no name before the `=`, the domain is hidden.

` at ` is read through a line end, `jane.doe at` with the domain on the next line or `jane.doe` with `at uni.edu` on the next, only where the words look like an address: the name has a full stop in it, or stands directly after `<`, `(`, `[` or `mailto:`, or the domain stands directly before `>`, `)` or `]`. `I took a quick stab at` with `droplevels.data.table` on the next line is none of these and stays. Of an address that is read so, the line end and the quote marks that start the next line stay where they are.

In a link, the name ends at what a link parts its pieces with (`/ ? & = : ; |` and any escape), and the rest of the link stays.

When a hidden address is directly followed by another separator and a domain, as in `contact me at jane.doe at uni.edu`, that goes with it.

### What becomes of it

| Where | With `--hide-addresses` |
|---|---|
| `body_plain`, `subject`, `subject_clean` | Every address becomes `[address hidden]`. Punctuation around it stays: `Jane Doe <jane at uni.edu> wrote:` becomes `Jane Doe <[address hidden]> wrote:`. |
| `body_snippet` | Cut from the cleaned text. |
| `from_name` | A name that is an address as a whole becomes its mailbox name: `jane at uni.edu` becomes `jane`. The name is cut at the first `+`, `=` or `%`. From a lossy form it comes without the characters that form lost: `j@ne.doe` becomes `jne.doe`. A name with an address among other words gets `[address hidden]`. A name with nothing left to show becomes `unknown`. |
| Threads, `contributors.json` | Made from the cleaned messages, so participants, names and slugs follow. A name that `parse` takes from `--aliases` is cleaned like a `from_name`. |
| `message_id`, `in_reply_to`, `references` | Each identifier is written as `<`, the first 32 hex characters of its SHA-256, `@hidden>`. A mail program often builds a Message-ID from the sender's mail domain, and sometimes from the mailbox name. Equal identifiers give equal hashes, so a reply still names the message it answers. In a value with several `<...>` each is hashed on its own, and words beside them are not written. A value without any `<...>` is hashed as a whole. An empty value stays empty. |

With `--sources`, the text is cleaned after the source page has restored it.

### What is kept on purpose

- The address of an R mailing list: a mailbox name that starts with `r-`, with `bioc` or with the value of `--list`, whatever the case, at `r-project.org`, `stat.math.ethz.ch`, `lists.r-project.org` or `lists.r-forge.r-project.org`. A name with a full stop, `+`, `=` or `%` in it is not one: a list's name has no full stop, and a list's bounce address carries a member's address after one of the three signs. In a link, where those signs end a name, a name that directly follows `+`, `=` or an escape is not a list's either. This holds for every separator, with one limit. After a lossy separator, name and domain are compared with `@` standing for an a, an s or a full stop and `|` for an i, an l or an f, which is how ` @end|ng |rom ` writes them. ` m@iii@g oii ` and ` m@ili@g off ` change letters in other ways as well, and a list's address that they have changed (`iists.r-project.org`) is not recognised and is hidden.
- A web address written in a sentence. What follows ` at ` starts with `www.`, or is directly followed by `/` as in `the code at github.com/user/repo`. The slash counts only when the word before ` at ` has no full stop in it, written or spelled with ` dot `, and no address stands in what follows the slash up to the next blank, or starts there: `a.b at x.org/c.d at y.org` is two addresses, and `the code at github.com/user/repo at the moment` is a sentence.
- What follows one of fifteen ordinary words and ` at `: `look`, `looking`, `available`, `found`, `here`, `located`, `hosted`, `are`, `was`, `and`, `up`, `now`, `left`, `indexed` and `see`.

The last two hold for the lower case ` at ` only, also when it is broken over a line end, and for none of its other spellings.

Anything else that reads as an address is hidden, also where it is none: `tree at edge.length`, which is R code, and `we stare at the dot plot` are. A hidden word is a small loss, and a published address cannot be taken back.

### What is not changed, and what stays visible

- `id`, `thread_id` and `thread_depth` are made from the identifiers as the source has them, before those are hashed. They, the order of the messages and the shape of every thread are what a run without the flag gives. So are the dates, `month` and `source_url`.
- `from_email_hash` is not changed. It is an unsalted SHA-256 of the sender's address as the archive spells it, and it is what joins a person's messages across lists. Whoever has an address can compute its hash and look for it.
- A sender without a display name is shown under the mailbox name, as above.
- `jane@localhost` and `jane at 192.168.1.1` stay: a host without a full stop and a bare IP address are not read as domains.
- `@word.word` with no name before the sign stays, as said above.
- Words on two lines that do not look like an address stay, as said above.
- Other spellings are not read: ` at ` with a tab or with two blanks around it, an HTML entity for the sign, and any spelling not listed above.
- The flag is on `parse` only. `stats --aliases` and `aggregate --aliases` write the names of the alias file as the file has them. The `sources` subcommand writes Message-IDs as the source has them into the `rejected` entries of `sources/*.json`.

## Development

`cargo test` runs everything. No test touches the network: the fetching code sits behind a trait and is tested against saved pages in `tests/fixtures/`.

## Releasing

Pushing a tag that starts with `v` builds the Linux binary and publishes it as the latest release, which is what the archive repositories download on their next run. Tag the commit on `main` that should ship by its hash or as `origin/main`: this repository also has a tag called `main`, so the bare name is ambiguous. Do not start the workflow by hand, because a manual run names the release after the branch it was started on.
