# EmoEdit 全量 benchmark 备份（2026-11-11）

本备份包含实际运行的代码、完整测试数据、3,240 张生成图片、逐图指标和所用的公开 Q-Former 权重。这次完成的是公开模型的推理评测，未重新训练。

- 原始测试目录含 415 张图片，按官方说明排除 10 张，评测 405 张。
- 8 种目标情感，每种 405 个样本，共 3,240 个样本。
- 30 步；每个样本 CUDA seed=48000；guidance=7.5；image guidance=1.5。
- GPU：RTX 5090。详细环境见 snapshot 内的 environment.txt。
- 全部输出通过配对、尺寸、生成记录和 SHA256 校验；指标汇总已重新核验。

## 评测结果

| 指标 | 本次实测 | 论文参考值 |
|---|---:|---:|
| PSNR ↑ | 16.1652 | 16.6200 |
| SSIM ↑ | 0.5624 | 0.5710 |
| LPIPS ↓ | 0.3013 | 0.2890 |
| CLIP-I ↑ | 0.8239 | 0.8280 |
| Emo-A ↑ | 50.8333% | 50.09% |
| Emo-S ↑ | 0.3451 | 0.3350 |

公开 checkpoint 是否就是论文表格使用的训练权重尚未确认。LPIPS 主结果遵循作者代码的 [0,1] 输入；标准 [-1,1] 版本单独记录为 LPIPS_standard。

[完整报告](snapshot/benchmark/official30_seed48000_qformer4000/REPORT.md) · [逐图指标](snapshot/benchmark/official30_seed48000_qformer4000/metrics/per_pair.jsonl) · [最终审计](snapshot/benchmark/official30_seed48000_qformer4000/final_audit.json)

## 备份内容

- `snapshot/`：实际执行代码和结果记录，保留服务器内的相对目录结构，未改写评测代码。
- `snapshot/EmoEdit/`：实际使用的上游源码子集和本地推理入口。
- `snapshot/benchmark/upstream/`：官方验证函数、指标实现和情感分类器权重。
- `snapshot/artifacts/`：前期 demo 的图片及记录，与全量 benchmark 分开保存。
- `EXPORT_MANIFEST.json`：99 个代码与结果文件，以及四个大附件的大小和 SHA256。
- `MODEL_FILES.json`：原服务器基础模型的实际文件名、大小和 SHA256。

完整数据和较大权重保存在[本次 Release](https://github.com/paletteeee/agent-da/releases/tag/emoedit-benchmark-20261111)：

| 附件 | 内容 |
|---|---|
| [emoedit-code-results-20261111.tar.gz](https://github.com/paletteeee/agent-da/releases/download/emoedit-benchmark-20261111/emoedit-code-results-20261111.tar.gz) | 代码与结果记录的独立恢复包 |
| [emoedit-generated-images-3240.tar.gz](https://github.com/paletteeee/agent-da/releases/download/emoedit-benchmark-20261111/emoedit-generated-images-3240.tar.gz) | 全部 3,240 张 benchmark 输出 JPEG |
| [EmoEdit-inference.tar](https://github.com/paletteeee/agent-da/releases/download/emoedit-benchmark-20261111/EmoEdit-inference.tar) | 官方原始测试集压缩包，含 415 张图片 |
| [Q-Former_4000.pth](https://github.com/paletteeee/agent-da/releases/download/emoedit-benchmark-20261111/Q-Former_4000.pth) | 本次使用的公开 Emotion Adapter 权重 |

原始数据和全部生成图片均作为附件备份，不只是保存来源链接。通用 InstructPix2Pix、CLIP、AlexNet 权重不重复上传；其来源、版本和文件哈希均已保存。Python 虚拟环境、缓存、未使用的训练集以及其他实验不在此次备份范围内。

## 恢复方法

下载上述四个附件和 Release 中的 `SHA256SUMS` 到同一目录。Linux 使用 `sha256sum -c SHA256SUMS` 校验；macOS 使用 `shasum -a 256 -c SHA256SUMS`。

以下命令针对新建或空的目标目录，恢复到原运行路径：

```bash
mkdir -p /data/emoedit-baseline
tar -xzf emoedit-code-results-20261111.tar.gz -C /data/emoedit-baseline
mkdir -p /data/emoedit-baseline/benchmark/data /data/emoedit-baseline/models
cp EmoEdit-inference.tar /data/emoedit-baseline/benchmark/data/
tar -xf EmoEdit-inference.tar -C /data/emoedit-baseline/benchmark/data
tar -xzf emoedit-generated-images-3240.tar.gz -C /data/emoedit-baseline
cp Q-Former_4000.pth /data/emoedit-baseline/models/
```

恢复后即可读取报告、逐图指标和图片，无需安装模型环境。

如需重新运行，按 `snapshot/benchmark/official30_seed48000_qformer4000/environment.txt` 准备兼容的 CUDA/PyTorch 环境，并恢复基础模型：

- `timbrooks/instruct-pix2pix`：本次使用 fp16 safetensors 子集。
- `openai/clip-vit-large-patch14`：本次使用 `model.safetensors`。
- `openai/clip-vit-base-patch32`：本次使用 `pytorch_model.bin`。
- AlexNet：`https://download.pytorch.org/models/alexnet-owt-7be5be79.pth`，放到 `models/torch/hub/checkpoints/`。

Hugging Face 模型的精确 revision 是 `snapshot/models/*_metadata.json` 中的 `sha`。需要精确恢复时应显式传入该 revision，而非下载最新版本。实际使用的文件清单和校验值见 `MODEL_FILES.json`；原始下载脚本作为运行记录保留。

原环境准备完成后，在 `/data/emoedit-baseline` 执行：

```bash
.venv/bin/python -m unittest discover -s benchmark -p 'test_*.py'
bash benchmark/run_full.sh
```

脚本会校验并跳过已经生成的图片；协议不一致时会拒绝混合结果。换环境或参数的实验应使用新运行目录，保留本次备份。

## 来源与归属

上游项目：[JingyuanYY/EmoEdit](https://github.com/JingyuanYY/EmoEdit)，提交 `88f622cdb5c7afdc8954eef899064296fe4a4273`。官方数据和 Q-Former 权重来源见 `snapshot/EmoEdit/Readme.md` 的 Dropbox 链接。

上游代码、原始数据和模型的权利归原作者及相应权利人，本备份不授予额外许可。
