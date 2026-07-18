# Stage Events command-line interface

The optional Stage Events CLI sends structured CloudEvents directly from a
shell. It provides one command for each supported `*CloudEvent` model and
validates the event data before making a request.

## Installation

The CLI dependencies are not included in the core installation. Enable them
with the `cli` extra:

```console
python -m pip install 'stage-events-client[cli]'
```

This installs the `send-stage-event` executable:

```console
send-stage-event --version
send-stage-event --help
```

If the package was installed without the extra, reinstall it with the command
above.

## Commands

The general command form is:

```console
send-stage-event COMMAND URL [OPTIONS]
```

`URL` is the complete HTTP or HTTPS endpoint, including its path and optional
query string. The base URL and endpoint path do not need to be configured
separately. URL fragments are not accepted.

| Command | Event model | Event type |
| --- | --- | --- |
| `calendar` | `CalendarCloudEvent` | `calendar-event` |
| `submitted` | `SubmittedCloudEvent` | `submitted` |
| `dismissed` | `DismissedCloudEvent` | `dismissed` |
| `prepared` | `PreparedCloudEvent` | `prepared` |
| `completed` | `CompletedCloudEvent` | `completed` |
| `failed` | `FailedCloudEvent` | `failed` |
| `piped` | `PipedCloudEvent` | `piped` |
| `staged` | `StagedCloudEvent` | `staged` |
| `ordered` | `OrderedCloudEvent` | `ordered` |

Use a command's help output to inspect its options:

```console
send-stage-event submitted --help
```

## Options

All event commands accept the same CloudEvent and request options.

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `--source TEXT` | Yes | — | CloudEvent source. It must contain three colon-separated components. |
| `--subject TEXT` | Yes | — | CloudEvent subject. It must contain three colon-separated components. |
| `--data JSON\|@FILE\|-` | Yes | — | Event-specific data as an inline JSON object, a file, or standard input. |
| `--partition-key TEXT` | No | Value of `--subject` | CloudEvent partition key. |
| `--x-kafka-topic TEXT` | No | Header omitted | Value of the `X-Kafka-Topic` request header. |
| `--token TEXT` | No | `STAGE_EVENTS_TOKEN` | Bearer token used for authentication. |
| `--timeout FLOAT` | No | `30.0` | Positive request timeout in seconds. |
| `--verify-ssl` | No | Enabled | Enable TLS certificate verification. |
| `--no-verify-ssl` | No | — | Disable TLS certificate verification. |

The event `type` is selected by the command and cannot be overridden. Event
data is validated against the corresponding Pydantic data model. Invalid JSON
or model validation errors are reported before any request is sent.

## Providing event data

### Inline JSON

Pass a JSON object directly to `--data`:

```console
send-stage-event submitted \
  https://events.example.com/hooks/cloud-events \
  --source workflows:example-process:submit \
  --subject workflows:2f660c57:example-workflow \
  --data '{"namespace":"workflows","time":"2026-07-18T12:00:00Z"}'
```

### JSON file

Prefix the file path with `@`:

```console
send-stage-event prepared \
  https://events.example.com/hooks/cloud-events \
  --source workflows:example-process:prepare \
  --subject workflows:2f660c57:example-workflow \
  --data @prepared-data.json
```

For example, `prepared-data.json` could contain:

```json
{
  "namespace": "workflows",
  "process_id": "example-process",
  "process_version": "1.2.0",
  "job_id": "2f660c57",
  "inputs": {
    "area": "s3://example-bucket/area.geojson"
  }
}
```

### Standard input

Use `-` to read the JSON object from standard input:

```console
cat submitted-data.json | send-stage-event submitted \
  https://events.example.com/hooks/cloud-events \
  --source workflows:example-process:submit \
  --subject workflows:2f660c57:example-workflow \
  --data -
```

In every input mode, `--data` must resolve to a JSON object rather than an array
or scalar value.

## Authentication

Supply a bearer token with `--token`:

```console
send-stage-event submitted \
  https://events.example.com/hooks/cloud-events \
  --source workflows:example-process:submit \
  --subject workflows:2f660c57:example-workflow \
  --data @submitted-data.json \
  --token your-bearer-token
```

For scripts and CI jobs, use the `STAGE_EVENTS_TOKEN` environment variable so
the token does not appear in shell history or process arguments:

```console
export STAGE_EVENTS_TOKEN=your-bearer-token

send-stage-event submitted \
  https://events.example.com/hooks/cloud-events \
  --source workflows:example-process:submit \
  --subject workflows:2f660c57:example-workflow \
  --data @submitted-data.json
```

When neither form is provided, the request is sent without an `Authorization`
header.

## Kafka topic header

The `X-Kafka-Topic` header is optional. When needed, pass it with
`--x-kafka-topic`:

```console
send-stage-event submitted \
  https://events.example.com/hooks/cloud-events \
  --source workflows:example-process:submit \
  --subject workflows:2f660c57:example-workflow \
  --data @submitted-data.json \
  --x-kafka-topic workflows.2f660c57.submitted
```

When the option is absent, the CLI does not add the header. Topic names accepted
by the Stage Events API follow this pattern:

```text
{namespace}.{workflow-uid}.{event-suffix}
```

The suffix is one of `prepared`, `submitted`, `completed`, `failed`, `piped`,
`staged`, `dismissed`, or `ordered`. Calendar topics use a duration followed by
`.calendar`, for example `workflows.2f660c57.10m.calendar`.

## TLS verification and timeouts

TLS certificate verification is enabled by default. Only use
`--no-verify-ssl` in a controlled development environment:

```console
send-stage-event submitted \
  https://localhost:12000/cloud-events \
  --source workflows:example-process:submit \
  --subject workflows:2f660c57:example-workflow \
  --data @submitted-data.json \
  --timeout 10 \
  --no-verify-ssl
```

## Output and exit status

On success, the CLI writes the parsed response body to standard output when it
is not empty and exits with status `0`.

Validation failures, connection errors, unexpected HTTP statuses, and
documented API errors are written to standard error and produce a non-zero exit
status. Structured API problem responses are rendered as formatted JSON.
