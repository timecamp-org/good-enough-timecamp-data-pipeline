import sys
import unittest
from unittest.mock import Mock, patch

from dlt_fetch_timecamp import parse_arguments, run_pipeline


class DestinationTests(unittest.TestCase):
    def test_destination_format_defaults(self):
        cases = [
            ([], ("filesystem", "csv")),
            (["--destination", "bigquery"], ("bigquery", None)),
            (["--destination", "duckdb"], ("duckdb", None)),
        ]
        for argv, expected in cases:
            with self.subTest(argv=argv), patch.object(sys, "argv", ["script"] + argv):
                args = parse_arguments()
                self.assertEqual((args.destination, args.output_format), expected)

    def test_nonfilesystem_rejects_output(self):
        argv = ["script", "--destination", "duckdb", "--output", "./output"]
        with patch.object(sys, "argv", argv):
            with self.assertRaises(SystemExit) as exit_result:
                parse_arguments()
        self.assertEqual(exit_result.exception.code, 2)

    @patch("dlt_fetch_timecamp.timecamp_source")
    @patch("dlt_fetch_timecamp.dlt.pipeline")
    @patch("dlt_fetch_timecamp.os.makedirs")
    def test_named_destination_uses_dlt_default_format(
        self, makedirs, pipeline, source
    ):
        pipeline.return_value.run.return_value = "load info"

        result = run_pipeline(
            "2026-09-22",
            "2026-09-22",
            "./timecamp_data",
            None,
            ["entries"],
            Mock(),
            Mock(),
            destination="duckdb",
        )

        self.assertEqual(result, "load info")
        pipeline.assert_called_once_with(
            pipeline_name="timecamp_duckdb", destination="duckdb", dataset_name="timecamp"
        )
        pipeline.return_value.run.assert_called_once_with(
            source.return_value, loader_file_format=None
        )
        makedirs.assert_not_called()


if __name__ == "__main__":
    unittest.main()
