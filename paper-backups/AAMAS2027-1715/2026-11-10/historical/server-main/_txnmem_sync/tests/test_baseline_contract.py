import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from txnmem_experiment import check_invariants, generate_instance, run_instance  # noqa: E402


class BaselineContractTests(unittest.TestCase):
    def test_pilot_contract_stays_available(self):
        instance = generate_instance("atomic_multi_write", seed=0, config={"txn_size": 2})
        result = run_instance(instance, "TxnMem")
        self.assertEqual(instance["instance_id"], "atomic_multi_write_seed_0")
        self.assertEqual(result["transaction_state"], "aborted")
        self.assertEqual(check_invariants(instance, result), [])


if __name__ == "__main__":
    unittest.main()
