import unittest,tempfile,pathlib,json
from PIL import Image
import protocol
class ProvenanceTests(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory();self.root=pathlib.Path(self.tmp.name)
  self.pair={'image_id':'1','emotion':'amusement','target_index':0,'source':'/fixture/1.png','output':'1_amusement.jpg'}
  Image.new('RGB',(512,512),'red').save(self.root/self.pair['output'])
  self.record={**self.pair,'sha256':protocol.sha256(self.root/self.pair['output'])}
  self.log=self.root/'generation.jsonl';self.log.write_text(json.dumps(self.record)+'\n')
 def tearDown(self):self.tmp.cleanup()
 def audit(self):return protocol.audit_provenance({'pairs':[self.pair]},self.root)
 def test_valid_and_replaced_jpeg(self):
  self.assertEqual(self.audit()['verified_records'],1)
  Image.new('RGB',(512,512),'blue').save(self.root/self.pair['output'])
  with self.assertRaisesRegex(ValueError,'hash'):self.audit()
 def test_duplicate_and_wrong_pair(self):
  self.log.write_text((json.dumps(self.record)+'\n')*2)
  with self.assertRaisesRegex(ValueError,'duplicate'):self.audit()
  self.record['target_index']=7;self.log.write_text(json.dumps(self.record)+'\n')
  with self.assertRaisesRegex(ValueError,'pair'):self.audit()
 def test_missing_record_and_partial_resume(self):
  self.log.write_text('')
  with self.assertRaisesRegex(ValueError,'record'):self.audit()
  with self.assertRaisesRegex(ValueError,'record'):protocol.audit_provenance({'pairs':[self.pair]},self.root,complete=False)
  (self.root/self.pair['output']).unlink()
  self.assertEqual(protocol.audit_provenance({'pairs':[self.pair]},self.root,complete=False)['verified_records'],0)
if __name__=='__main__':unittest.main()
