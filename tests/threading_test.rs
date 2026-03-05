use rmail_parser::message::parse_message;
use rmail_parser::threading::reconstruct_threads;

#[test]
fn test_reconstruct_threads_basic() {
    let raw_msgs = vec![
        r#"From u@e.com  Mon Feb  1 20:13:59 2026
From: Alice <alice@example.com>
Date: Sun, 1 Feb 2026 20:13:59 -0500
Subject: Topic A
Message-ID: <a1@example.com>
Content-Type: text/plain

First message."#,
        r#"From u@e.com  Mon Feb  2 12:28:38 2026
From: Bob <bob@example.com>
Date: Mon, 2 Feb 2026 12:28:38 +0100
Subject: Re: Topic A
Message-ID: <a2@example.com>
In-Reply-To: <a1@example.com>
References: <a1@example.com>
Content-Type: text/plain

Reply to first."#,
        r#"From u@e.com  Tue Feb  3 10:00:00 2026
From: Charlie <charlie@example.com>
Date: Tue, 3 Feb 2026 10:00:00 +0000
Subject: Topic B
Message-ID: <b1@example.com>
Content-Type: text/plain

Different thread."#,
    ];

    let mut messages: Vec<_> = raw_msgs.iter()
        .map(|r| parse_message(r).unwrap())
        .collect();

    let threads = reconstruct_threads(&mut messages);

    // Should produce 2 threads
    assert_eq!(threads.len(), 2);

    // Thread A should have 2 messages
    let thread_a = threads.iter().find(|t| t.subject == "Topic A").unwrap();
    assert_eq!(thread_a.message_count, 2);
    assert_eq!(thread_a.participants.len(), 2);

    // Thread B should have 1 message
    let thread_b = threads.iter().find(|t| t.subject == "Topic B").unwrap();
    assert_eq!(thread_b.message_count, 1);

    // Messages should have thread_id and depth set
    let reply = messages.iter().find(|m| m.message_id == "<a2@example.com>").unwrap();
    assert_eq!(reply.thread_depth, 1);
    assert_eq!(reply.thread_id, thread_a.id);
}
