import json
import unittest
from unittest.mock import Mock, call, patch

import requests

from common.api import TimeCampAPI


def make_response(status_code, body, headers=None):
    response = requests.Response()
    response.status_code = status_code
    response._content = json.dumps(body).encode("utf-8")
    response.headers.update(headers or {})
    response.url = "https://app.timecamp.com/third_party/api/application"
    return response


class TimeCampAPIRequestTests(unittest.TestCase):
    def setUp(self):
        self.api = TimeCampAPI.__new__(TimeCampAPI)
        self.api.base_url = "https://app.timecamp.com/third_party/api"
        self.api.headers = {"Authorization": "Bearer test"}
        self.api.logger = Mock()

    @patch("common.api.time.sleep")
    @patch("common.api.requests.request")
    def test_retries_retryable_502_using_response_delay(self, request, sleep):
        unavailable = make_response(
            502,
            {
                "retryable": True,
                "retry_after": 60,
                "error_name": "origin_bad_gateway",
            },
        )
        success = make_response(200, {"123": {"app_name": "Terminal"}})
        request.side_effect = [unavailable, success]

        response = self.api._make_request("GET", "application")

        self.assertIs(response, success)
        self.assertEqual(request.call_count, 2)
        sleep.assert_called_once_with(60.0)

    @patch("common.api.time.sleep")
    @patch("common.api.requests.request")
    def test_stops_after_five_retryable_responses(self, request, sleep):
        request.side_effect = [
            make_response(502, {"retryable": True, "retry_after": 1})
            for _ in range(5)
        ]

        with self.assertRaises(requests.HTTPError):
            self.api._make_request("GET", "application")

        self.assertEqual(request.call_count, 5)
        self.assertEqual(sleep.call_args_list, [call(1.0)] * 4)

    @patch("common.api.time.sleep")
    @patch("common.api.requests.request")
    def test_does_not_retry_non_retryable_client_error(self, request, sleep):
        request.return_value = make_response(400, {"message": "Bad request"})

        with self.assertRaises(requests.HTTPError):
            self.api._make_request("GET", "application")

        request.assert_called_once()
        sleep.assert_not_called()

    def test_get_applications_uses_100_id_batches(self):
        application_ids = [str(application_id) for application_id in range(201)]
        self.api._make_request = Mock(
            side_effect=[
                Mock(json=Mock(return_value={})),
                Mock(json=Mock(return_value={})),
                Mock(json=Mock(return_value={})),
            ]
        )

        self.api.get_applications(application_ids)

        batch_sizes = [
            len(call.kwargs["params"]["application_ids"].split(","))
            for call in self.api._make_request.call_args_list
        ]
        self.assertEqual(batch_sizes, [100, 100, 1])


if __name__ == "__main__":
    unittest.main()
