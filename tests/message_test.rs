use rmail_parser::message::parse_message;

#[test]
fn test_parse_basic_message() {
    let raw = r#"From user1@example.com  Mon Feb  1 20:13:59 2026
From: Ben Bolker <bbo|ker @end|ng |rom gm@||@com>
Date: Sun, 1 Feb 2026 20:13:59 -0500
Subject: [R] weighted.residuals()
Message-ID: <msg001@example.com>
Content-Type: text/plain; charset=UTF-8

I noticed that weighted.residuals() doesn't handle
the case where weights are all zero."#;

    let msg = parse_message(raw).unwrap();
    assert_eq!(msg.from_name, "Ben Bolker");
    assert_eq!(msg.subject, "[R] weighted.residuals()");
    assert_eq!(msg.subject_clean, "weighted.residuals()");
    assert_eq!(msg.message_id, "<msg001@example.com>");
    assert!(msg.in_reply_to.is_none());
    assert!(msg.references.is_empty());
    assert!(msg.body_plain.contains("weighted.residuals()"));
    assert_eq!(msg.month, "2026-02");
}

#[test]
fn test_parse_reply_message() {
    let raw = r#"From user2@example.com  Mon Feb  2 12:28:38 2026
From: Martin Maechler <m@ech|er @end|ng |rom @t@t@m@th@ethz@ch>
Date: Mon, 2 Feb 2026 12:28:38 +0100
Subject: Re: [R] weighted.residuals()
Message-ID: <msg002@example.com>
In-Reply-To: <msg001@example.com>
References: <msg001@example.com>
Content-Type: text/plain; charset=UTF-8

Good catch."#;

    let msg = parse_message(raw).unwrap();
    assert_eq!(msg.from_name, "Martin Maechler");
    assert_eq!(msg.subject_clean, "weighted.residuals()");
    assert_eq!(msg.in_reply_to, Some("<msg001@example.com>".to_string()));
    assert_eq!(msg.references, vec!["<msg001@example.com>"]);
}

#[test]
fn test_deobfuscate_email() {
    use rmail_parser::message::deobfuscate_email;

    // Test basic Mailman obfuscation: "local @end|ng |rom domain"
    assert_eq!(
        deobfuscate_email("bbo|ker @end|ng |rom gm@||@com"),
        "bboker@gm.com"
    );
    assert_eq!(
        deobfuscate_email("m@ech|er @end|ng |rom @t@t@m@th@ethz@ch"),
        "mecher@t.t.m.th.ethz.ch"
    );

    // Test already-normal email (no obfuscation)
    assert_eq!(
        deobfuscate_email("user@example.com"),
        "user@example.com"
    );
}

#[test]
fn test_clean_subject() {
    use rmail_parser::message::clean_subject;

    assert_eq!(clean_subject("[R] weighted.residuals()"), "weighted.residuals()");
    assert_eq!(clean_subject("Re: [R] weighted.residuals()"), "weighted.residuals()");
    assert_eq!(clean_subject("[R] Re: weighted.residuals()"), "weighted.residuals()");
    assert_eq!(clean_subject("Fwd: [R] something"), "something");
    assert_eq!(clean_subject("[R-pkgs] new package"), "new package");
    assert_eq!(clean_subject("[Rd] doc question"), "doc question");
    assert_eq!(clean_subject("Re: Re: [R] topic"), "topic");
}

#[test]
fn test_extract_name() {
    use rmail_parser::message::extract_name;

    // "Name <email>" format
    assert_eq!(extract_name("Ben Bolker <bbo|ker @end|ng |rom gm@||@com>"), "Ben Bolker");

    // "email (Name)" format
    assert_eq!(extract_name("user@example.com (John Doe)"), "John Doe");

    // Plain email only
    assert_eq!(extract_name("user@example.com"), "user@example.com");
}

#[test]
fn test_message_id_hashing() {
    let raw = r#"From user1@example.com  Mon Feb  1 20:13:59 2026
From: Ben Bolker <bbo|ker @end|ng |rom gm@||@com>
Date: Sun, 1 Feb 2026 20:13:59 -0500
Subject: [R] weighted.residuals()
Message-ID: <msg001@example.com>
Content-Type: text/plain; charset=UTF-8

Body text."#;

    let msg = parse_message(raw).unwrap();
    // id should be "msg-" followed by 12 hex chars
    assert!(msg.id.starts_with("msg-"));
    assert_eq!(msg.id.len(), 4 + 12); // "msg-" + 12 hex chars
}

#[test]
fn test_body_snippet() {
    let long_body = "A".repeat(300);
    let raw = format!(
        "From user@example.com  Mon Feb  1 20:13:59 2026\n\
         From: Test User <test @end|ng |rom ex@mp|e@com>\n\
         Date: Sun, 1 Feb 2026 20:13:59 -0500\n\
         Subject: [R] test\n\
         Message-ID: <msg-snippet@example.com>\n\
         Content-Type: text/plain; charset=UTF-8\n\
         \n\
         {}",
        long_body
    );

    let msg = parse_message(&raw).unwrap();
    assert_eq!(msg.body_snippet.len(), 200);
    assert_eq!(msg.body_plain.len(), 300);
}

#[test]
fn test_thread_fields_initialized() {
    let raw = r#"From user1@example.com  Mon Feb  1 20:13:59 2026
From: Ben Bolker <bbo|ker @end|ng |rom gm@||@com>
Date: Sun, 1 Feb 2026 20:13:59 -0500
Subject: [R] test
Message-ID: <msg-thread@example.com>
Content-Type: text/plain; charset=UTF-8

Body."#;

    let msg = parse_message(raw).unwrap();
    assert_eq!(msg.thread_id, "");
    assert_eq!(msg.thread_depth, 0);
}

#[test]
fn test_body_without_mime_headers_keeps_its_letters() {
    // A pipermail text archive keeps no Content-Type, and the file has
    // already been read as text: the body is that text, accents and all.
    let raw = "From ana at example.org  Mon Mar  7 10:00:00 2016\n\
               From: ana at example.org (Ana)\n\
               Date: Mon, 7 Mar 2016 10:00:00 -0300\n\
               Subject: [R-br] =?iso-8859-1?q?fun=E7=E3o?=\n\
               Message-ID: <acc1@example.org>\n\
               \n\
               Função não é\n";

    let msg = parse_message(raw).unwrap();
    assert_eq!(msg.subject, "[R-br] função");
    assert_eq!(msg.body_plain, "Função não é");
}

#[test]
fn test_body_whose_headers_name_a_charset_keeps_its_letters() {
    // The headers name the charset the message was sent in, but the file
    // has been read as text already, so the body is not decoded again.
    let raw = "From bia at example.org  Mon Mar  7 11:00:00 2016\n\
               From: bia at example.org (Bia)\n\
               Date: Mon, 7 Mar 2016 11:00:00 -0300\n\
               Subject: teste\n\
               Message-ID: <acc2@example.org>\n\
               Content-Type: text/plain; charset=iso-8859-1\n\
               \n\
               Função não é\n";

    let msg = parse_message(raw).unwrap();
    assert_eq!(msg.body_plain, "Função não é");
}

#[test]
fn test_body_in_quoted_printable_is_decoded_in_its_charset() {
    let raw = "From cai at example.org  Mon Mar  7 12:00:00 2016\n\
               From: cai at example.org (Cai)\n\
               Date: Mon, 7 Mar 2016 12:00:00 -0300\n\
               Subject: teste\n\
               Message-ID: <acc3@example.org>\n\
               Content-Type: text/plain; charset=iso-8859-1\n\
               Content-Transfer-Encoding: quoted-printable\n\
               \n\
               Fun=E7=E3o n=E3o =E9\n";

    let msg = parse_message(raw).unwrap();
    assert_eq!(msg.body_plain, "Função não é");
}

#[test]
fn test_byte_order_mark_at_the_start_of_a_body_is_dropped() {
    let raw = "From dan at example.org  Mon Mar  7 13:00:00 2016\n\
               From: Dan <dan@example.org>\n\
               Date: Mon, 7 Mar 2016 13:00:00 -0300\n\
               Subject: teste\n\
               Message-ID: <acc4@example.org>\n\
               Content-Type: text/plain; charset=\"utf-8\"\n\
               Content-Transfer-Encoding: 8bit\n\
               \n\
               \u{feff}\n\
               Função\n";

    let msg = parse_message(raw).unwrap();
    assert_eq!(msg.body_plain, "Função");
}
