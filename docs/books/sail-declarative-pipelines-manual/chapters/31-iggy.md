# Apache Iggy as a source

An Apache Iggy topic can feed an AutoCDC flow. Sail reads it as a bounded stream: each run takes the messages that exist when the run starts, and the next run continues from where the last one ended.

## Reading a topic

```python
@dp.temporary_view
def changes():
    return (
        spark.readStream.format("iggy")
        .option("url", "iggy://iggy:secret@127.0.0.1:8090")
        .option("stream", "cdc")
        .option("topic", "events")
        .option("timeout_ms", "5000")
        .schema("id BIGINT, name STRING, op STRING, seq BIGINT")
        .load()
    )

dp.create_streaming_table("target", format="delta")
dp.create_auto_cdc_flow(
    target="target",
    source="changes",
    keys=["id"],
    sequence_by="seq",
    apply_as_deletes=sf.expr("op = 'D'"),
    except_column_list=["op"],
)
```

A schema is required: Sail decodes each message into it.

| Option | Meaning |
| :-- | :-- |
| `url` | `iggy://user:password@host:port`. The user and password are used to log in; only the host and port name the source. |
| `stream`, `topic` | The Iggy stream and topic. Both must exist before the first run. |
| `payload_format` | `json` (the default), `raw`, or `envelope`. |
| `source_id` | With `envelope`, the column that holds the envelope's source id. |
| `batch_size` | Messages asked for in one poll. Default 1000. |
| `timeout_ms` | The longest one call to the broker may take. Default 30000. Without it a dead broker would be retried without limit. |

Option names are case-insensitive, as in Spark. An option that belongs to the Iggy reader's other modes, such as a consumer group, is refused rather than ignored: a bounded read keeps its own positions and never commits an offset to the broker.

## Positions

The first run starts at the first readable offset of each partition and ends at the offset the broker reports when the run starts. Each later run starts where the last ended. Messages that arrive while a run is in progress wait for the next run.

A full refresh starts again from the first readable offset. If the broker's retention has moved past the offset a run needs, the run stops before writing anything; the message names the gap.

## Topics that change

Iggy gives a topic an incarnation; deleting and recreating a topic gives it a new one. A run that finds the topic's incarnation changed stops and leaves the target as it was, because the recorded offsets no longer mean anything. A full refresh adopts the new topic and rebuilds the target from its beginning.

A topic that does not exist is refused before the first write, with a message that says the topic is absent.

## What is refused

- Reading an Iggy topic as a batch, with `spark.read`, is refused; the source serves pipeline AutoCDC flows.
- An append flow (`CREATE FLOW ... AS INSERT INTO`) can read Iggy as well, through the same source; see the streaming chapter.

## Running a broker for tests

Sail's tests run a broker they start themselves. They need the server binary of the Iggy version Sail is built against, named by an environment variable:

```sh
export SAIL_IGGY_SERVER_BINARY=/path/to/iggy-server
```

Build it from the Iggy repository at the commit Sail pins, with `cargo build --release -p server`, and point the variable at `target/release/iggy-server`. Without the variable the Iggy tests are skipped with a reason that says so. The tests start the broker on free ports with QUIC and WebSocket disabled, create the stream and topic over HTTP, and send messages to partition 0.
