import sys
import unittest
from unittest.mock import Mock, patch

from dlt_fetch_timecamp import parse_arguments, timecamp_source


class CustomFieldsTests(unittest.TestCase):
    def test_custom_fields_flag_defaults_to_off(self):
        for argv, expected in [([], False), (["--custom-fields"], True)]:
            with self.subTest(argv=argv), patch.object(sys, "argv", ["script"] + argv):
                self.assertEqual(parse_arguments().custom_fields, expected)

    def test_source_loads_custom_fields_of_selected_datasets(self):
        api = Mock()
        api.get_custom_field_values.side_effect = lambda resource_type: [
            {"resourceId": 1, "resourceType": resource_type, "value": "x"}
        ]

        source = timecamp_source(
            api=api,
            from_date="2026-09-22",
            to_date="2026-09-22",
            datasets=["entries", "tasks"],
            logger=Mock(),
            custom_fields=True,
        )

        self.assertEqual(
            list(source.resources.keys()),
            ["entries", "tasks", "entries_custom_fields", "tasks_custom_fields"],
        )
        self.assertEqual(
            list(source.resources["tasks_custom_fields"]),
            [{"resourceId": 1, "resourceType": "task", "value": "x"}],
        )
        self.assertEqual(
            list(source.resources["entries_custom_fields"]),
            [{"resourceId": 1, "resourceType": "entry", "value": "x"}],
        )

    def test_source_skips_custom_fields_without_flag(self):
        source = timecamp_source(
            api=Mock(),
            from_date="2026-09-22",
            to_date="2026-09-22",
            datasets=["entries", "tasks"],
            logger=Mock(),
        )

        self.assertEqual(list(source.resources.keys()), ["entries", "tasks"])


if __name__ == "__main__":
    unittest.main()
