import unittest

from txnmem_resampling import (
    _resample_whole_clusters,
    cluster_bootstrap_interval,
)


class _FixedRandom:
    def choices(self, population, *, k):
        self.population = list(population)
        self.k = k
        return [population[0], population[0], population[-1]]


class TxnMemResamplingTests(unittest.TestCase):
    def setUp(self):
        self.rows = [
            {"conversation": "a", "score": 0.0, "row": "a1"},
            {"conversation": "a", "score": 1.0, "row": "a2"},
            {"conversation": "b", "score": 0.5, "row": "b1"},
            {"conversation": "c", "score": 1.0, "row": "c1"},
            {"conversation": "c", "score": 0.0, "row": "c2"},
            {"conversation": "c", "score": 1.0, "row": "c3"},
        ]

    def test_resampling_preserves_every_row_in_each_selected_cluster(self):
        sampled = _resample_whole_clusters(
            self.rows,
            group_key="conversation",
            rng=_FixedRandom(),
        )

        self.assertEqual(
            [row["row"] for row in sampled],
            ["a1", "a2", "a1", "a2", "c1", "c2", "c3"],
        )

    def test_cluster_bootstrap_is_deterministic_and_reports_cluster_unit(self):
        first = cluster_bootstrap_interval(
            self.rows,
            "conversation",
            "score",
            repetitions=500,
            seed=17,
        )
        second = cluster_bootstrap_interval(
            self.rows,
            "conversation",
            "score",
            repetitions=500,
            seed=17,
        )

        self.assertEqual(first, second)
        self.assertEqual(first["group_count"], 3)
        self.assertEqual(first["row_count"], 6)
        self.assertEqual(first["sampling_unit"], "whole_group")
        self.assertAlmostEqual(first["estimate"], sum(row["score"] for row in self.rows) / 6)
        self.assertLessEqual(first["lower"], first["estimate"])
        self.assertGreaterEqual(first["upper"], first["estimate"])

    def test_cluster_bootstrap_rejects_missing_groups_and_boolean_scores(self):
        with self.assertRaises(ValueError):
            cluster_bootstrap_interval([], "conversation", "score")
        with self.assertRaises(ValueError):
            cluster_bootstrap_interval(
                [{"conversation": "a", "score": True}],
                "conversation",
                "score",
            )


if __name__ == "__main__":
    unittest.main()
