import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from locomo_official_eval import aggregate_scores, parse_batch_answers  # noqa: E402


class LoCoMoOfficialEvalTests(unittest.TestCase):
    def test_parse_batch_answers_accepts_json_and_strips_code_fence(self):
        raw = "```json\n{\"0\": \" May 7, 2023 \", \"1\": \"Alice\"}\n```"
        self.assertEqual(
            parse_batch_answers(raw, 2),
            {0: "May 7, 2023", 1: "Alice"},
        )

    def test_parse_batch_answers_falls_back_to_numbered_lines(self):
        self.assertEqual(
            parse_batch_answers("1. first answer\n2) second answer", 2),
            {0: "first answer", 1: "second answer"},
        )

    def test_aggregate_scores_reports_question_and_category_counts(self):
        result = aggregate_scores(
            [
                {"category": 1, "score": 0.5},
                {"category": 1, "score": 1.0},
                {"category": 5, "score": 0.0},
            ]
        )
        self.assertEqual(result["question_count"], 3)
        self.assertAlmostEqual(result["mean_f1"], 0.5)
        self.assertEqual(result["category_counts"], {"1": 2, "5": 1})


if __name__ == "__main__":
    unittest.main()
