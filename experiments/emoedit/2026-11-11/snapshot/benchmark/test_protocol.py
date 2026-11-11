import unittest,tempfile,pathlib,json
from PIL import Image
try:
 from protocol import EMOTIONS, EXCLUDED_IDS, build_manifest, ensure_protocol, audit_outputs
except ImportError:
 EMOTIONS=EXCLUDED_IDS=build_manifest=ensure_protocol=audit_outputs=None

class ProtocolTests(unittest.TestCase):
 def setUp(self):
  self.assertIsNotNone(build_manifest,'benchmark protocol implementation is required')
  self.tmp=tempfile.TemporaryDirectory();self.root=pathlib.Path(self.tmp.name)
 def tearDown(self):
  if hasattr(self,'tmp'):self.tmp.cleanup()
 def populate(self):
  for i in range(1,416):Image.new('RGB',(8,8),(i%256,i//256,40)).save(self.root/f'{i}.png')
 def test_official_exclusions_and_all_emotion_pairs(self):
  self.populate();m=build_manifest(self.root)
  self.assertEqual(len(m['inputs']),405);self.assertEqual(len(m['pairs']),3240)
  self.assertEqual(set(EXCLUDED_IDS),{'21','18','111','137','218','228','237','266','277','333'})
  self.assertTrue(set(EXCLUDED_IDS).isdisjoint({x['image_id'] for x in m['inputs']}))
  self.assertEqual(EMOTIONS,['amusement','awe','contentment','excitement','anger','disgust','fear','sadness'])
  self.assertEqual(len({x['output'] for x in m['pairs']}),3240)
 def test_duplicate_ids_rejected(self):
  self.populate();Image.new('RGB',(8,8)).save(self.root/'1.jpg')
  with self.assertRaisesRegex(ValueError,'duplicate'):build_manifest(self.root)
 def test_missing_input_rejected(self):
  self.populate();(self.root/'1.png').unlink()
  with self.assertRaisesRegex(ValueError,'405'):build_manifest(self.root)
 def test_changed_protocol_rejected(self):
  p=self.root/'protocol.json';ensure_protocol(p,{'seed':48000,'manifest':'a'});ensure_protocol(p,{'seed':48000,'manifest':'a'})
  with self.assertRaisesRegex(ValueError,'protocol'):ensure_protocol(p,{'seed':42,'manifest':'a'})
 def test_missing_and_corrupt_outputs_rejected(self):
  m={'pairs':[{'output':'1_amusement.jpg'},{'output':'1_sadness.jpg'}]}
  with self.assertRaisesRegex(ValueError,'missing'):audit_outputs(m,self.root)
  for p in m['pairs']:Image.new('RGB',(512,512)).save(self.root/p['output'])
  self.assertEqual(audit_outputs(m,self.root)['count'],2)
  (self.root/'1_sadness.jpg').write_bytes(b'broken')
  with self.assertRaisesRegex(ValueError,'corrupt'):audit_outputs(m,self.root)
 def test_non_square_output_rejected_for_fixed_benchmark(self):
  m={'pairs':[{'output':'1_amusement.jpg'}]}
  Image.new('RGB',(512,640)).save(self.root/'1_amusement.jpg')
  with self.assertRaisesRegex(ValueError,'corrupt'):audit_outputs(m,self.root)
if __name__=='__main__':unittest.main()
