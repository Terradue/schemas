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

from http import HTTPStatus
from typing import Any

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models import (
    BadRequest,
    CalendarCloudEvent,
    CompletedCloudEvent,
    DismissedCloudEvent,
    FailedCloudEvent,
    InvalidBodyPropertyFormat,
    InvalidBodyPropertyValue,
    InvalidParameters,
    InvalidRequestHeaderFormat,
    InvalidRequestParameterFormat,
    InvalidRequestParameterValue,
    MissingBodyProperty,
    MissingRequestHeader,
    MissingRequestParameter,
    OrderedCloudEvent,
    PipedCloudEvent,
    PreparedCloudEvent,
    StagedCloudEvent,
    SubmittedCloudEvent,
)
from ...types import Response


def _get_kwargs(
    *,
    body: CalendarCloudEvent
    | CompletedCloudEvent
    | DismissedCloudEvent
    | FailedCloudEvent
    | OrderedCloudEvent
    | PipedCloudEvent
    | PreparedCloudEvent
    | StagedCloudEvent
    | SubmittedCloudEvent,
    x_kafka_topic: str,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}
    headers["X-Kafka-Topic"] = x_kafka_topic

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/cloud-events",
    }

    _kwargs["json"] = body.model_dump(mode="json")

    headers["Content-Type"] = "application/cloudevents+json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> (
    BadRequest
    | InvalidBodyPropertyFormat
    | InvalidBodyPropertyValue
    | InvalidParameters
    | InvalidRequestHeaderFormat
    | InvalidRequestParameterFormat
    | InvalidRequestParameterValue
    | MissingBodyProperty
    | MissingRequestHeader
    | MissingRequestParameter
    | str
    | None
):
    if response.status_code == 200:
        response_200 = response.text
        return response_200

    if response.status_code == 400:

        def _parse_response_400(
            data: object,
        ) -> (
            BadRequest
            | InvalidBodyPropertyFormat
            | InvalidBodyPropertyValue
            | InvalidParameters
            | InvalidRequestHeaderFormat
            | InvalidRequestParameterFormat
            | InvalidRequestParameterValue
            | MissingBodyProperty
            | MissingRequestHeader
            | MissingRequestParameter
        ):
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                response_400_type_0 = BadRequest.model_validate(data)

                return response_400_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                response_400_type_1 = InvalidBodyPropertyFormat.model_validate(data)

                return response_400_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                response_400_type_2 = InvalidBodyPropertyValue.model_validate(data)

                return response_400_type_2
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                response_400_type_3 = InvalidParameters.model_validate(data)

                return response_400_type_3
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                response_400_type_4 = InvalidRequestHeaderFormat.model_validate(data)

                return response_400_type_4
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                response_400_type_5 = InvalidRequestParameterFormat.model_validate(data)

                return response_400_type_5
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                response_400_type_6 = InvalidRequestParameterValue.model_validate(data)

                return response_400_type_6
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                response_400_type_7 = MissingBodyProperty.model_validate(data)

                return response_400_type_7
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                response_400_type_8 = MissingRequestHeader.model_validate(data)

                return response_400_type_8
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            if not isinstance(data, dict):
                raise TypeError()
            response_400_type_9 = MissingRequestParameter.model_validate(data)

            return response_400_type_9

        response_400 = _parse_response_400(response.json())

        return response_400

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[
    BadRequest
    | InvalidBodyPropertyFormat
    | InvalidBodyPropertyValue
    | InvalidParameters
    | InvalidRequestHeaderFormat
    | InvalidRequestParameterFormat
    | InvalidRequestParameterValue
    | MissingBodyProperty
    | MissingRequestHeader
    | MissingRequestParameter
    | str
]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: CalendarCloudEvent
    | CompletedCloudEvent
    | DismissedCloudEvent
    | FailedCloudEvent
    | OrderedCloudEvent
    | PipedCloudEvent
    | PreparedCloudEvent
    | StagedCloudEvent
    | SubmittedCloudEvent,
    x_kafka_topic: str,
) -> Response[
    BadRequest
    | InvalidBodyPropertyFormat
    | InvalidBodyPropertyValue
    | InvalidParameters
    | InvalidRequestHeaderFormat
    | InvalidRequestParameterFormat
    | InvalidRequestParameterValue
    | MissingBodyProperty
    | MissingRequestHeader
    | MissingRequestParameter
    | str
]:
    """Send CloudEvent.

     Sends a structured-mode CloudEvent.

    For more information, see [CloudEvents - Version
    1.0.2](https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/spec.md).

    Args:
        x_kafka_topic (str):
        body (CalendarCloudEvent | CompletedCloudEvent | DismissedCloudEvent | FailedCloudEvent |
            OrderedCloudEvent | PipedCloudEvent | PreparedCloudEvent | StagedCloudEvent |
            SubmittedCloudEvent):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[BadRequest | InvalidBodyPropertyFormat | InvalidBodyPropertyValue | InvalidParameters | InvalidRequestHeaderFormat | InvalidRequestParameterFormat | InvalidRequestParameterValue | MissingBodyProperty | MissingRequestHeader | MissingRequestParameter | str]
    """

    kwargs = _get_kwargs(
        body=body,
        x_kafka_topic=x_kafka_topic,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    *,
    client: AuthenticatedClient | Client,
    body: CalendarCloudEvent
    | CompletedCloudEvent
    | DismissedCloudEvent
    | FailedCloudEvent
    | OrderedCloudEvent
    | PipedCloudEvent
    | PreparedCloudEvent
    | StagedCloudEvent
    | SubmittedCloudEvent,
    x_kafka_topic: str,
) -> (
    BadRequest
    | InvalidBodyPropertyFormat
    | InvalidBodyPropertyValue
    | InvalidParameters
    | InvalidRequestHeaderFormat
    | InvalidRequestParameterFormat
    | InvalidRequestParameterValue
    | MissingBodyProperty
    | MissingRequestHeader
    | MissingRequestParameter
    | str
    | None
):
    """Send CloudEvent.

     Sends a structured-mode CloudEvent.

    For more information, see [CloudEvents - Version
    1.0.2](https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/spec.md).

    Args:
        x_kafka_topic (str):
        body (CalendarCloudEvent | CompletedCloudEvent | DismissedCloudEvent | FailedCloudEvent |
            OrderedCloudEvent | PipedCloudEvent | PreparedCloudEvent | StagedCloudEvent |
            SubmittedCloudEvent):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        BadRequest | InvalidBodyPropertyFormat | InvalidBodyPropertyValue | InvalidParameters | InvalidRequestHeaderFormat | InvalidRequestParameterFormat | InvalidRequestParameterValue | MissingBodyProperty | MissingRequestHeader | MissingRequestParameter | str
    """

    return sync_detailed(
        client=client,
        body=body,
        x_kafka_topic=x_kafka_topic,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: CalendarCloudEvent
    | CompletedCloudEvent
    | DismissedCloudEvent
    | FailedCloudEvent
    | OrderedCloudEvent
    | PipedCloudEvent
    | PreparedCloudEvent
    | StagedCloudEvent
    | SubmittedCloudEvent,
    x_kafka_topic: str,
) -> Response[
    BadRequest
    | InvalidBodyPropertyFormat
    | InvalidBodyPropertyValue
    | InvalidParameters
    | InvalidRequestHeaderFormat
    | InvalidRequestParameterFormat
    | InvalidRequestParameterValue
    | MissingBodyProperty
    | MissingRequestHeader
    | MissingRequestParameter
    | str
]:
    """Send CloudEvent.

     Sends a structured-mode CloudEvent.

    For more information, see [CloudEvents - Version
    1.0.2](https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/spec.md).

    Args:
        x_kafka_topic (str):
        body (CalendarCloudEvent | CompletedCloudEvent | DismissedCloudEvent | FailedCloudEvent |
            OrderedCloudEvent | PipedCloudEvent | PreparedCloudEvent | StagedCloudEvent |
            SubmittedCloudEvent):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[BadRequest | InvalidBodyPropertyFormat | InvalidBodyPropertyValue | InvalidParameters | InvalidRequestHeaderFormat | InvalidRequestParameterFormat | InvalidRequestParameterValue | MissingBodyProperty | MissingRequestHeader | MissingRequestParameter | str]
    """

    kwargs = _get_kwargs(
        body=body,
        x_kafka_topic=x_kafka_topic,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient | Client,
    body: CalendarCloudEvent
    | CompletedCloudEvent
    | DismissedCloudEvent
    | FailedCloudEvent
    | OrderedCloudEvent
    | PipedCloudEvent
    | PreparedCloudEvent
    | StagedCloudEvent
    | SubmittedCloudEvent,
    x_kafka_topic: str,
) -> (
    BadRequest
    | InvalidBodyPropertyFormat
    | InvalidBodyPropertyValue
    | InvalidParameters
    | InvalidRequestHeaderFormat
    | InvalidRequestParameterFormat
    | InvalidRequestParameterValue
    | MissingBodyProperty
    | MissingRequestHeader
    | MissingRequestParameter
    | str
    | None
):
    """Send CloudEvent.

     Sends a structured-mode CloudEvent.

    For more information, see [CloudEvents - Version
    1.0.2](https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/spec.md).

    Args:
        x_kafka_topic (str):
        body (CalendarCloudEvent | CompletedCloudEvent | DismissedCloudEvent | FailedCloudEvent |
            OrderedCloudEvent | PipedCloudEvent | PreparedCloudEvent | StagedCloudEvent |
            SubmittedCloudEvent):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        BadRequest | InvalidBodyPropertyFormat | InvalidBodyPropertyValue | InvalidParameters | InvalidRequestHeaderFormat | InvalidRequestParameterFormat | InvalidRequestParameterValue | MissingBodyProperty | MissingRequestHeader | MissingRequestParameter | str
    """

    return (
        await asyncio_detailed(
            client=client,
            body=body,
            x_kafka_topic=x_kafka_topic,
        )
    ).parsed
