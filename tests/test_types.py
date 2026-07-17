from http import HTTPStatus
from io import BytesIO
import unittest

from stage_events_client.types import File, Response, UNSET, Unset


class SharedTypesTests(unittest.TestCase):
    def test_unset_is_falsey(self) -> None:
        self.assertIsInstance(UNSET, Unset)
        self.assertFalse(UNSET)

    def test_file_converts_to_httpx_tuple(self) -> None:
        payload = BytesIO(b"content")
        upload = File(
            payload=payload, file_name="result.json", mime_type="application/json"
        )

        self.assertEqual(
            upload.to_tuple(), ("result.json", payload, "application/json")
        )

    def test_response_holds_raw_and_parsed_values(self) -> None:
        response = Response(
            status_code=HTTPStatus.OK,
            content=b"accepted",
            headers={"content-type": "text/plain"},
            parsed="accepted",
        )

        self.assertEqual(response.status_code, HTTPStatus.OK)
        self.assertEqual(response.parsed, "accepted")


if __name__ == "__main__":
    unittest.main()
