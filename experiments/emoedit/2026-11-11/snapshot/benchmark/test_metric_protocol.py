import unittest,numpy as np
try:
 from evaluate import pixel_metrics,emotion_metrics
except ImportError:
 pixel_metrics=emotion_metrics=None
class MetricTests(unittest.TestCase):
 def setUp(self):self.assertIsNotNone(pixel_metrics,'metric implementation is required')
 def test_identity_pixel_scores(self):
  a=np.tile(np.arange(16,dtype=np.uint8)[None,:,None],(16,1,3));m=pixel_metrics(a,a)
  self.assertEqual(m['SSIM'],1.0);self.assertTrue(np.isinf(m['PSNR']))
 def test_psnr_uses_upstream_image_range(self):
  a=np.tile(np.arange(16,dtype=np.uint8)[None,:,None],(16,1,3));b=a+10;m=pixel_metrics(a,b)
  self.assertAlmostEqual(m['PSNR'],20*np.log10(15/10),places=10)
  self.assertAlmostEqual(m['PSNR_fixed255'],20*np.log10(255/10),places=10)
 def test_emotion_accuracy_and_increment_no_double_softmax(self):
  before=np.array([.1,.1,.1,.1,.2,.1,.1,.2]);after=np.array([.05,.05,.05,.05,.5,.1,.1,.1])
  m=emotion_metrics(before,after,4)
  self.assertEqual(m['Emo-A'],1);self.assertAlmostEqual(m['Emo-S'],.3);self.assertEqual(m['prediction'],4)
  self.assertEqual(emotion_metrics(before,after,7)['Emo-A'],0)
if __name__=='__main__':unittest.main()
