import sys
import unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"src"))
try:
    from txnmem_external_campaign import make_jobs, experiment_summaries
except ImportError:
    make_jobs=experiment_summaries=None
class CampaignTests(unittest.TestCase):
    def setUp(self):self.assertIsNotNone(make_jobs,"campaign is not implemented")
    def test_reuses_identical_observations_between_questions(self):
        jobs=make_jobs(1)
        self.assertEqual(len(jobs),66)
        both=[j for j in jobs if j["system"]=="Mem0" and j["case"]["strategy"]=="directed"]
        self.assertEqual(len(both),6)
        self.assertTrue(all(set(j["roles"])=={"discovery","comparison_perturbed"} for j in both))
    def test_all_four_systems_use_identical_comparison_inputs(self):
        jobs=make_jobs(2)
        for role in ["comparison_perturbed","comparison_normal"]:
            sets=[{j["case"]["input_sha256"] for j in jobs if j["system"]==s and role in j["roles"]} for s in ["TxnMem","Mem0","G-Memory","A-MEM"]]
            self.assertTrue(all(x==sets[0] for x in sets))
    def test_summary_does_not_mix_normal_and_perturbed(self):
        base={"system":"X","strategy":"directed","family":"f","case_id":"a","status":"ok","correctness_included":True,"violations":["dirty_read"],"any_violation":1,"trial_index":1,"roles":["comparison_perturbed"]}
        normal={**base,"case_id":"b","any_violation":0,"violations":[],"roles":["comparison_normal"],"control":True,"normal_completed":1,"normal_task_count":2,"false_abort_count":0}
        s=experiment_summaries([base,normal])
        self.assertEqual(s["comparison_perturbed"]["groups"]["X/directed/f"]["violation_rate"],1)
        self.assertEqual(s["comparison_normal"]["groups"]["X/directed/f"]["normal_completion_rate"],1)
if __name__=="__main__":unittest.main()
