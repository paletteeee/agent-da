import unittest
from unittest.mock import patch
from tempfile import TemporaryDirectory
from pathlib import Path
from txnmem_external_compare import build_case,judge
from txnmem_external_campaign import run_system
class ReviewRegressions(unittest.TestCase):
 def test_executed_shape_ignores_content_seed(self):
  a=build_case("concurrent_supersession",0,"directed")
  b=build_case("concurrent_supersession",1,"directed")
  self.assertEqual(a["executed_structure_sha256"],b["executed_structure_sha256"])
 def test_invalidated_source_still_visible_is_violation(self):
  c=build_case("cross_agent_provenance",0,"directed")
  o=dict(status="ok",final_ids=["root","unrelated"],transactions={"A":"completed","B":"aborted"},reads=[],events=[],invalidated=["root"],fault_triggered=True)
  self.assertIn("invalidated_source_visible",judge(c,o)["violations"])
 def test_ordinary_discovery_is_replayed(self):
  c=build_case("multi_agent_visibility",0,"ordinary")
  o=dict(status="ok",final_ids=["unrelated"],final_snapshot={"unrelated":{"value":"Independent fact remains valid"}},transactions={"A":"completed","B":"completed"},reads=[],events=[],invalidated=[])
  with TemporaryDirectory() as d,patch("txnmem_external_campaign.Worker") as W:
   W.return_value.run.return_value=o
   rows,replays=run_system("Mem0",[dict(case=c,roles=["discovery"],trial_index=1,strategy_trial_index=1)],Path(d),2)
  self.assertEqual(len(replays),2)
  self.assertTrue(all(r["reproduced"] for r in replays))
