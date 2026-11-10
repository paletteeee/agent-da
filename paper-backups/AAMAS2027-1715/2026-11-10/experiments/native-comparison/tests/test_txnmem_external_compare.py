import copy
import sys
import unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
try:
    from txnmem_external_compare import build_case, judge, summarize_rows
except ImportError:
    build_case = judge = summarize_rows = None

class ExternalComparisonTests(unittest.TestCase):
    def setUp(self):
        self.assertIsNotNone(judge, "external comparison checker is not implemented")
    def case(self):
        return {"case_id":"one", "family":"cross_agent_provenance", "control":False,
                "expected_ids":["child"], "initial":[{"key":"root"},{"key":"unrelated"}],
                "write_sets":{"B":["child"]}, "producers":{"child":"B"},
                "edges":[["root","child"]], "operations":[], "strategy":"directed"}
    def observation(self):
        return {"status":"ok","final_ids":["child","unrelated"], "events":[],
                "transactions":{"B":"completed"}, "invalidated":["root"],
                "reads":[], "fault_triggered":True}
    def test_atomicity_has_two_writing_agents(self):
        c=build_case("interleaved_atomicity",0,"directed")
        self.assertTrue(c["write_sets"]["A"])
        self.assertTrue(c["write_sets"]["B"])
    def test_normal_wrong_content_does_not_complete(self):
        c=self.case();c["control"]=True;c["expected_values"]={"child":"correct"}
        o=self.observation();o["invalidated"]=[];o["final_ids"]=["root","child","unrelated"]
        o["final_snapshot"]={"root":{"value":"source"},"child":{"value":"wrong"},"unrelated":{"value":"unchanged"}}
        self.assertEqual(judge(c,o)["normal_completed"],0)
    def test_unknown_transaction_is_inconclusive(self):
        o=self.observation();o["transactions"]["B"]="unknown"
        self.assertEqual(judge(self.case(),o)["status"],"inconclusive")
    def test_deleted_source_does_not_hide_descendant_violation(self):
        row = judge(self.case(),self.observation())
        self.assertIn("stale_descendant",row["violations"])
        self.assertEqual(row["stale_descendant_count"],1)
    def test_missing_final_observation_is_inconclusive(self):
        obs=self.observation(); del obs["final_ids"]
        self.assertEqual(judge(self.case(),obs)["status"],"inconclusive")
    def test_unsupported_never_counts_as_correctness_pass(self):
        row=judge(self.case(),{"status":"unsupported_mapping","reason":"no delete"})
        self.assertFalse(row["correctness_included"])
        self.assertIsNone(row["any_violation"])
    def test_all_rejected_is_not_normal_task_success(self):
        case=self.case(); case["control"]=True
        obs=self.observation(); obs.update(final_ids=["root","unrelated"],invalidated=[],transactions={"B":"aborted"})
        row=judge(case,obs)
        self.assertEqual(row["normal_completed"],0)
        self.assertEqual(row["false_abort_count"],1)
    def test_deleted_descendants_are_valid_repair(self):
        obs=self.observation(); obs["final_ids"]=["unrelated"]
        self.assertNotIn("stale_descendant",judge(self.case(),obs)["violations"])
    def test_unrelated_removal_is_reported(self):
        obs=self.observation(); obs["final_ids"]=[]
        self.assertIn("unrelated_loss",judge(self.case(),obs)["violations"])
    def test_dirty_read_uses_actual_return_before_commit(self):
        obs=self.observation(); obs.update(invalidated=[],reads=[{"txn":"A","visible":["child"],"completed":[]}])
        self.assertIn("dirty_read",judge(self.case(),obs)["violations"])
    def test_random_preserves_each_actor_program(self):
        for family in ["multi_agent_visibility","interleaved_atomicity","concurrent_supersession","cross_agent_provenance"]:
            x=build_case(family,3,"random")
            for actor in ["A","B"]:
                positions=[o["actor_step"] for o in x["operations"] if o["actor"]==actor]
                self.assertEqual(positions,sorted(positions))
            self.assertEqual(x,build_case(family,3,"random"))
    def test_control_has_no_fault_and_requires_effects(self):
        x=build_case("interleaved_atomicity",2,"directed",True)
        self.assertFalse(x["fault"])
        self.assertGreaterEqual(len(x["expected_ids"]),2)
    def test_summary_keeps_unsupported_and_censored_first_hit(self):
        rows=[{"system":"X","strategy":"random","family":"f","case_id":"a","status":"unsupported_mapping","correctness_included":False,"any_violation":None,"violations":[],"trial_index":1},
              {"system":"X","strategy":"random","family":"f","case_id":"b","status":"ok","correctness_included":True,"any_violation":0,"violations":[],"trial_index":2}]
        g=summarize_rows(rows)["groups"]["X/random/f"]
        self.assertEqual(g["attempted"],2); self.assertEqual(g["evaluated"],1)
        self.assertIsNone(g["first_detection_trial"])
        self.assertTrue(g["right_censored"])

if __name__=="__main__": unittest.main()
