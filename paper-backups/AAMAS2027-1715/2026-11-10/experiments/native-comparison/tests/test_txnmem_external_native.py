import sys
import unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"src"))
from txnmem_external_compare import build_case, judge
try:
    from txnmem_external_native import execute_case
except ImportError:
    execute_case=None

class NativeTests(unittest.TestCase):
    def setUp(self):
        self.assertIsNotNone(execute_case,"native execution is not implemented")
    def test_txnmem_normal_completes_real_backend_effects(self):
        case=build_case("multi_agent_visibility",0,"directed",True)
        obs=execute_case("TxnMem",case)
        self.assertEqual(obs["status"],"ok",obs)
        self.assertEqual(judge(case,obs)["normal_completed"],1)
        self.assertEqual(obs["implementation"],"MultiAgentStorageCoordinator/SQLiteInstrumentedMemoryBackend")
    def test_txnmem_interrupt_does_not_publish_buffer(self):
        case=build_case("interleaved_atomicity",0,"directed")
        obs=execute_case("TxnMem",case)
        self.assertTrue(obs["fault_triggered"])
        self.assertFalse(set(case["write_sets"]["A"])&set(obs["final_ids"]))
    def test_txnmem_conflict_aborts_one_writer(self):
        case=build_case("concurrent_supersession",0,"directed")
        obs=execute_case("TxnMem",case)
        self.assertEqual(list(obs["transactions"].values()).count("aborted"),1)
        self.assertEqual(judge(case,obs)["any_violation"],0)
    def test_gmemory_invalidation_excluded_before_side_effects(self):
        obs=execute_case("G-Memory",build_case("cross_agent_provenance",0,"directed"))
        self.assertEqual(obs["status"],"unsupported_mapping")
        self.assertFalse(obs.get("events"))

if __name__=="__main__":unittest.main()
