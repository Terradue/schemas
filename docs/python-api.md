# Python API

The `stage-events-client` package provides Pydantic event models and synchronous
and asynchronous HTTPX-based clients for sending structured CloudEvents to the
Stage Events API.

## Installation

Install the core Python library with:

```console
python -m pip install stage-events-client
```

Python 3.10 or newer is required. The optional `[cli]` extra is only needed for
the [`send-stage-event`](cli.md) command and is not required when using the
Python API.

## Create a client

Use `AuthenticatedClient` when the endpoint requires a bearer token:

```python
from stage_events_client import AuthenticatedClient

client = AuthenticatedClient(
    base_url="https://events.example.com",
    token="your-bearer-token",
)
```

The default authentication header is:

```text
Authorization: Bearer your-bearer-token
```

Use `Client` for an endpoint that does not require authentication:

```python
from stage_events_client import Client

client = Client(base_url="https://events.example.com")
```

The endpoint functions use `/cloud-events` as their default request path, so
`base_url` normally contains only the scheme, host, and any common API prefix.

## Construct an event

Every supported event has a CloudEvent model and a corresponding data model in
`stage_events_client.models`. The following example constructs a submitted
event by instantiating both models and setting their fields directly:

```python
from datetime import datetime, timezone

from stage_events_client.models import SubmittedCloudEvent, SubmittedData

subject = "workflows:2f660c57:example-workflow"

event_data = SubmittedData(
    namespace="workflows",
    time=datetime(2026, 7, 18, 12, 0, tzinfo=timezone.utc),
)

event = SubmittedCloudEvent(
    source="workflows:example-process:submit",
    subject=subject,
    partitionkey=subject,
    data=event_data,
)
```

`type` does not need to be supplied because each event model defines its own
value. In this example it defaults to `submitted`.

Models validate event-specific fields before a request is sent. This includes
timezone-aware timestamps, semantic versions, status values, and GeoJSON
structures where applicable.

The supported event models are:

| Event type | CloudEvent model | Data model |
| --- | --- | --- |
| `calendar-event` | `CalendarCloudEvent` | `CalendarData` |
| `submitted` | `SubmittedCloudEvent` | `SubmittedData` |
| `dismissed` | `DismissedCloudEvent` | `DismissedData` |
| `prepared` | `PreparedCloudEvent` | `PreparedData` |
| `completed` | `CompletedCloudEvent` | `CompletedData` |
| `failed` | `FailedCloudEvent` | `FailedData` |
| `piped` | `PipedCloudEvent` | `PipedData` |
| `staged` | `StagedCloudEvent` | `StagedData` |
| `ordered` | `OrderedCloudEvent` | `OrderedData` |

## Send an event synchronously

Import the endpoint module and call `sync`:

```python
from stage_events_client.api.default import send_cloud_event

with client:
    result = send_cloud_event.sync(
        client=client,
        body=event,
        x_kafka_topic="workflows.2f660c57.submitted",
    )

print(result)
```

The context manager closes the underlying `httpx.Client` after the request.
Create a client once and keep its context open when sending multiple events:

```python
with client:
    for event in events:
        send_cloud_event.sync(client=client, body=event)
```

### Optional Kafka topic

`x_kafka_topic` is optional. When it is omitted or set to `None`, the client
does not add an `X-Kafka-Topic` header:

```python
result = send_cloud_event.sync(
    client=client,
    body=event,
)
```

When supplied, topic names accepted by the API follow the form
`{namespace}.{workflow-uid}.{event-suffix}`. Calendar topics use a duration and
`.calendar`, such as `workflows.2f660c57.10m.calendar`.

## Inspect the complete response

`sync` returns only the parsed response. Use `sync_detailed` to inspect the
status, headers, raw content, and parsed value:

```python
response = send_cloud_event.sync_detailed(
    client=client,
    body=event,
    x_kafka_topic="workflows.2f660c57.submitted",
)

print(response.status_code)
print(response.headers)
print(response.content)
print(response.parsed)
```

The returned `stage_events_client.types.Response` has these attributes:

| Attribute | Description |
| --- | --- |
| `status_code` | Response status as an `http.HTTPStatus`. |
| `headers` | HTTP response headers. |
| `content` | Raw response bytes. |
| `parsed` | Parsed success value, problem model, or `None`. |

A successful `200` response is parsed as a string. A documented `400` response
is parsed into the corresponding problem-details model.

### Custom endpoint path

The detailed functions accept a custom relative path when the service does not
expose the default `/cloud-events` endpoint:

```python
response = send_cloud_event.sync_detailed(
    client=client,
    body=event,
    path="/hooks/cloud-events",
)
```

## Async usage

Use `asyncio` to return the parsed value or `asyncio_detailed` to return the
complete response:

```python
import asyncio

from stage_events_client import AuthenticatedClient
from stage_events_client.api.default import send_cloud_event


async def main() -> None:
    client = AuthenticatedClient(
        base_url="https://events.example.com",
        token="your-bearer-token",
    )

    async with client:
        result = await send_cloud_event.asyncio(
            client=client,
            body=event,
            x_kafka_topic="workflows.2f660c57.submitted",
        )

    print(result)


asyncio.run(main())
```

Do not use the same client instance in synchronous and asynchronous context
managers at the same time.

## Response and error handling

Set `raise_on_unexpected_status=True` to raise `UnexpectedStatus` when the API
returns a status that is not documented by the OpenAPI definition:

```python
from stage_events_client import AuthenticatedClient
from stage_events_client.errors import UnexpectedStatus

client = AuthenticatedClient(
    base_url="https://events.example.com",
    token="your-bearer-token",
    raise_on_unexpected_status=True,
)

try:
    result = send_cloud_event.sync(client=client, body=event)
except UnexpectedStatus as exc:
    print(exc.status_code)
    print(exc.content)
```

When this option is `False`, an unexpected status is parsed as `None`.
Documented API errors are returned as problem models rather than raised as
`UnexpectedStatus`:

For example, a missing header response can be handled with its concrete problem
type:

```python
from eoap_problems_registry import MissingRequestHeader

result = send_cloud_event.sync(client=client, body=event)

if isinstance(result, MissingRequestHeader):
    print(result.model_dump_json(indent=2))
```

HTTPX exceptions such as `httpx.TimeoutException` and
`httpx.RequestError` can still be raised for transport failures.

## Client configuration

Both client classes support shared HTTP configuration:

```python
import httpx

from stage_events_client import AuthenticatedClient

client = AuthenticatedClient(
    base_url="https://events.example.com",
    token="your-bearer-token",
    headers={"X-Correlation-ID": "request-id"},
    cookies={"session": "value"},
    timeout=httpx.Timeout(30.0),
    verify_ssl="/path/to/ca-bundle.pem",
    follow_redirects=True,
    httpx_args={"proxy": "http://proxy.example.com:8080"},
)
```

| Argument | Description |
| --- | --- |
| `base_url` | Base URL used for relative endpoint paths. |
| `headers` | Headers included with every request. |
| `cookies` | Cookies included with every request. |
| `timeout` | HTTPX timeout configuration. |
| `verify_ssl` | Boolean, CA bundle path, or `ssl.SSLContext`. |
| `follow_redirects` | Whether HTTP redirects are followed. |
| `httpx_args` | Additional `httpx.Client` and `httpx.AsyncClient` arguments. |
| `raise_on_unexpected_status` | Raise instead of returning `None` for undocumented statuses. |

TLS certificate verification is enabled by default. Setting `verify_ssl=False`
disables server verification and should only be used in a controlled
development environment.

### Derive an updated client

The `with_*` methods return an updated client:

```python
client = client.with_headers({"X-Correlation-ID": "new-request-id"})
client = client.with_cookies({"session": "new-value"})
client = client.with_timeout(httpx.Timeout(10.0))
```

### Supply an HTTPX client

Use `set_httpx_client` or `set_async_httpx_client` to provide an existing HTTPX
client:

```python
http_client = httpx.Client(
    base_url="https://events.example.com",
    timeout=30.0,
)
client.set_httpx_client(http_client)
```

Supplying an HTTPX client overrides the generated client's base URL, headers,
cookies, timeout, and TLS settings. Configure those values directly on the
HTTPX instance.
