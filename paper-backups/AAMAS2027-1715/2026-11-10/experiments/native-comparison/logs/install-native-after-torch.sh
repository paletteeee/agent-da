#!/bin/bash
cd /data/txnmem/external-comparison-20260928
while kill -0 2236976 2>/dev/null; do sleep 5; done
.venv/bin/python -c 'import torch; print(torch.__version__)' || exit 1
.venv/bin/python -m pip install 'torch==2.6.0' 'chromadb>=1.0.9,<1.2' 'langchain-chroma==0.2.6' 'langchain-community<0.4' 'langchain-core<1' networkx finch-clust pytest rank-bm25 litellm nltk 'transformers<5' 'sentence-transformers<4'
