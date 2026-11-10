"""Fetch only pinned upstream files; validate every byte against the campaign manifest."""
import hashlib,json,urllib.request
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
REPOS={"GMemory":("bingreeky/GMemory","7b581c51d993bd600df14691d101d7e601040cc6"),
       "A-mem":("agiresearch/A-mem","ceffb860f0712bbae97b184d440df62bc910ca8d")}
manifest=json.loads((ROOT/"configs/external_source_manifest.json").read_text())
for name,expected in manifest.items():
 p=ROOT/name
 if p.exists():
  if hashlib.sha256(p.read_bytes()).hexdigest()!=expected:raise RuntimeError("Existing file differs: "+name)
  continue
 rel=Path(name);package=rel.parts[1];repo,rev=REPOS[package];upstream="/".join(rel.parts[2:])
 if upstream=="SOURCE_REVISION":
  data=(rev+"\n").encode()
  if hashlib.sha256(data).hexdigest()!=expected:
   raise RuntimeError("Use the bundled SOURCE_REVISION metadata for "+package)
 else:
  url=f"https://raw.githubusercontent.com/{repo}/{rev}/{upstream}"
  with urllib.request.urlopen(url,timeout=60) as r:data=r.read()
 if hashlib.sha256(data).hexdigest()!=expected:raise RuntimeError("Source hash mismatch: "+name)
 p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(data)
 print(name)
