# TxnMem 论文工作与实验总报告：交付 QA 记录

日期：2026-08-17

## 交付对象

- 正式文件：`TxnMem_论文工作与实验总报告.docx`
- 内容源：`docs/txnmem_paper_work_and_experiment_report_zh.md`
- 确定性构建器：`scripts/build_txnmem_paper_work_report_docx.py`
- SHA-256：`4a3e713219d5bba30da14e14fe49f56e2d65ebe3d9da13cb5b3fb735fef6cf3e`
- 版式规模：18 页、5 幅图、6 张表

## 可复现构建

使用两个全新的临时栅格缓存分别执行完整构建，两份 DOCX 的 SHA-256 完全相同；它们也与用于逐页视觉检查的构建完全相同。图表由构建器从 SVG 定义重新栅格化，而不是复用仓库外的手工截图。

在 macOS 的捆绑 LibreOffice 环境渲染时，必须显式设置该运行时自带的 `FONTCONFIG_FILE`。这样才能稳定解析中文字体并避免空白字形。该要求已由仓库测试覆盖。

## 自动化验证

- 报告焦点测试：7/7 通过。
- 仓库全量测试：369 项通过，4 项按环境条件跳过。
- `git diff --check`：通过。
- 标题审计：12 个 Heading 1、36 个 Heading 2；正文编号列表未伪装成标题。
- 页面审计：1 个 Letter 纵向 section，四边距均为 1 英寸，首页使用独立页眉页脚设置。
- 图片审计：5 幅图均为 inline image，尺寸均在正文宽度内。
- 表格审计：6 张表的 `tblW`、`tblGrid` 与单元格宽度完全一致，总宽均为 9360 DXA，左缩进 120 DXA。
- 可访问性审计：high/medium/low 均为 0；5 幅图均有替代文本，表头均有语义标记。
- 隐私清理审计：无需删除 rsid、作者核心属性、自定义属性或外部关系；构建器已主动固定匿名元数据并清除修订会话标识。
- 样式检查：报告的正文、标题、列表、图注和 artifact 段落使用命名样式。检查器还报告标题页和表格单元格存在构建器有意设置的直接格式；这些项目不构成结构、TOC 或可访问性缺陷。

## 视觉 QA

使用最终 SHA 对应的渲染结果，以原始分辨率逐页检查第 1–18 页。确认：

- 中文与英文字符均正常显示；
- 无文本、图、表裁切或重叠；
- 图题、表头、页眉和页码位置一致；
- 两组编号列表均从 1 开始；
- joint realism 表的 MMD² 与 p 值完整可读；
- 附录中的长 artifact 路径在单元格内正常换行；
- 真实后端段落的中英文混排未出现异常拉伸；
- 无意外空白页。

## 内容与证据边界

报告系统梳理了论文问题、五项创新、实现路径、四层数据来源、实验 A–K、各实验目的与统计单位、创新—证据闭环、claim ledger、限制和投稿状态。所有主结果均指向 active artifact，并保留以下边界：

- 原生模型/公开 runtime 当前是逐事件 memory contract，不等同于跨多工具调用的事务管理器；
- 公开 benchmark 的 reward、success、assertion 和 QA F1 不等同于 memory accuracy；
- AppWorld projection 是 method/URL-only trace-grounded projection，不是原生 memory ground truth；
- 五个单机 Toxiproxy 场景与三次 client-to-model-server 跨主机运行不能外推为一般分布式事务、生产延迟或多主机 Agent worker 集群；
- joint realism 的显著失配是负结果，支持继续校准 synthetic generator，不支持“分布等价”主张。
