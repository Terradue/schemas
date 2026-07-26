# Copyright 2026 Terradue
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Click command-line interface for sending Stage CloudEvents."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, TypeAlias
from urllib.parse import urlsplit, urlunsplit

import click
import httpx
from pydantic import BaseModel, ValidationError

from .__about__ import __version__
from .api.default import send_cloud_event
from .client import AuthenticatedClient, Client
from .errors import UnexpectedStatus
from .models import (
    CalendarCloudEvent,
    CompletedCloudEvent,
    DismissedCloudEvent,
    FailedCloudEvent,
    OrderedCloudEvent,
    PipedCloudEvent,
    PreparedCloudEvent,
    StagedCloudEvent,
    SubmittedCloudEvent,
)

CloudEventModel: TypeAlias = (
    type[CalendarCloudEvent]
    | type[CompletedCloudEvent]
    | type[DismissedCloudEvent]
    | type[FailedCloudEvent]
    | type[OrderedCloudEvent]
    | type[PipedCloudEvent]
    | type[PreparedCloudEvent]
    | type[StagedCloudEvent]
    | type[SubmittedCloudEvent]
)

EVENT_MODELS: dict[str, CloudEventModel] = {
    "calendar": CalendarCloudEvent,
    "submitted": SubmittedCloudEvent,
    "dismissed": DismissedCloudEvent,
    "prepared": PreparedCloudEvent,
    "completed": CompletedCloudEvent,
    "failed": FailedCloudEvent,
    "piped": PipedCloudEvent,
    "staged": StagedCloudEvent,
    "ordered": OrderedCloudEvent,
}


def _load_data(value: str) -> dict[str, Any]:
    """Load a JSON object from an inline value, a file, or standard input."""
    try:
        if value == "-":
            raw = sys.stdin.read()
        elif value.startswith("@"):
            raw = Path(value[1:]).read_text(encoding="utf-8")
        else:
            raw = value
        data = json.loads(raw)
    except OSError as exc:
        raise click.BadParameter(str(exc), param_hint="--data") from exc
    except json.JSONDecodeError as exc:
        raise click.BadParameter(
            f"invalid JSON at line {exc.lineno}, column {exc.colno}: {exc.msg}",
            param_hint="--data",
        ) from exc

    if not isinstance(data, dict):
        raise click.BadParameter("must contain a JSON object", param_hint="--data")
    return data


def _split_url(url: str) -> tuple[str, str]:
    """Split a complete endpoint URL into the values expected by the client."""
    parsed = urlsplit(url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise click.BadParameter(
            "must be an absolute HTTP or HTTPS URL",
            param_hint="URL",
        )
    if parsed.fragment:
        raise click.BadParameter("must not contain a fragment", param_hint="URL")

    base_url = urlunsplit((parsed.scheme, parsed.netloc, "", "", ""))
    path = urlunsplit(("", "", parsed.path or "/", parsed.query, ""))
    return base_url, path


def _render_error(parsed: object, status_code: int) -> str:
    if isinstance(parsed, BaseModel):
        return parsed.model_dump_json(indent=2)
    if parsed is not None:
        return str(parsed)
    return f"Stage Events API returned HTTP {status_code}"


def _send_event(
    event_model: CloudEventModel,
    *,
    url: str,
    source: str,
    subject: str,
    partition_key: str | None,
    data: str,
    x_kafka_topic: str | None,
    token: str | None,
    timeout: float,
    verify_ssl: bool,
) -> None:
    base_url, path = _split_url(url)
    payload = {
        "source": source,
        "subject": subject,
        "partitionkey": partition_key or subject,
        "data": _load_data(data),
    }

    try:
        event = event_model.model_validate(payload)
    except ValidationError as exc:
        raise click.ClickException(f"invalid event payload:\n{exc}") from exc

    client_options: dict[str, Any] = {
        "base_url": base_url,
        "timeout": httpx.Timeout(timeout),
        "verify_ssl": verify_ssl,
        "raise_on_unexpected_status": True,
    }
    client: Client | AuthenticatedClient
    if token:
        client = AuthenticatedClient(token=token, **client_options)
    else:
        client = Client(**client_options)

    try:
        with client:
            response = send_cloud_event.sync_detailed(
                client=client,
                body=event,
                x_kafka_topic=x_kafka_topic,
                path=path,
            )
    except (httpx.HTTPError, UnexpectedStatus) as exc:
        raise click.ClickException(str(exc)) from exc

    if 200 <= response.status_code < 300:
        if response.parsed:
            click.echo(response.parsed)
        return

    click.echo(_render_error(response.parsed, response.status_code), err=True)
    raise click.exceptions.Exit(1)


def _event_command(name: str, event_model: CloudEventModel) -> click.Command:
    @click.command(name=name, help=f"Send a {event_model.__name__}.")
    @click.argument("url")
    @click.option("--source", required=True, help="CloudEvent source.")
    @click.option("--subject", required=True, help="CloudEvent subject.")
    @click.option(
        "--partition-key",
        help="CloudEvent partition key; defaults to the subject.",
    )
    @click.option(
        "--data",
        required=True,
        metavar="JSON|@FILE|-",
        help="Event data as JSON, @path, or - for standard input.",
    )
    @click.option(
        "--x-kafka-topic",
        help="Optional X-Kafka-Topic request header.",
    )
    @click.option(
        "--token",
        envvar="STAGE_EVENTS_TOKEN",
        help="Bearer token; can also be set with STAGE_EVENTS_TOKEN.",
    )
    @click.option(
        "--timeout",
        type=click.FloatRange(min=0, min_open=True),
        default=30.0,
        show_default=True,
        help="Request timeout in seconds.",
    )
    @click.option(
        "--verify-ssl/--no-verify-ssl",
        default=True,
        show_default=True,
        help="Enable or disable TLS certificate verification.",
    )
    def command(**kwargs: Any) -> None:
        _send_event(event_model, **kwargs)

    return command


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
@click.version_option(version=__version__)
def main() -> None:
    """Send structured CloudEvents to a Stage Events endpoint."""


for _name, _model in EVENT_MODELS.items():
    main.add_command(_event_command(_name, _model))


if __name__ == "__main__":
    main()
