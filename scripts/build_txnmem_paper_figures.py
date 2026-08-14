#!/usr/bin/env python3
"""Build deterministic, evidence-backed SVG figures for the TxnMem manuscript."""

from __future__ import annotations

import argparse
import hashlib
import html
import json
import sys
from pathlib import Path
from typing import Any, Callable, Iterable

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from txnmem_paper_projection import controlled_result_rows


REQUIRED_FIGURE_IDS = (
    "motivation_timeline",
    "architecture",
    "commit_protocol",
    "provenance_repair",
    "controlled_results",
    "evidence_layers",
)

BLUE = "#1F4E79"
GRAY = "#68737D"
LIGHT_GRAY = "#E8ECEF"
DARK = "#263238"
RED = "#A63D40"
FONT = "Arial Unicode MS, Hiragino Sans GB, Noto Sans CJK SC, sans-serif"


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _load_json(root: Path, relative_path: str) -> dict[str, Any]:
    return json.loads((root / relative_path).read_text(encoding="utf-8"))


def _escape(value: object) -> str:
    return html.escape(str(value), quote=True)


def _lines(parts: list[str], x: float, y: float, lines: Iterable[str], *, size: int = 18,
           fill: str = DARK, anchor: str = "start", weight: str = "400", leading: int | None = None) -> None:
    step = leading or int(size * 1.28)
    for index, line in enumerate(lines):
        parts.append(
            f'<text x="{x:.1f}" y="{y + index * step:.1f}" text-anchor="{anchor}" '
            f'font-size="{size}" font-weight="{weight}" fill="{fill}">{_escape(line)}</text>'
        )


def _svg(width: int, height: int, title: str, description: str, body: list[str]) -> str:
    style = (
        "text { font-family: " + FONT + "; } "
        ".struct { stroke: " + GRAY + "; stroke-width: 1.4; fill: none; } "
        ".flow { stroke: " + BLUE + "; stroke-width: 2; fill: none; } "
        ".muted { stroke: " + GRAY + "; stroke-width: 1.2; fill: none; } "
        ".future { stroke: " + RED + "; stroke-width: 1.5; stroke-dasharray: 5 4; fill: none; }"
    )
    return "\n".join(
        [
            '<?xml version="1.0" encoding="UTF-8"?>',
            f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}" role="img" aria-labelledby="title desc">',
            f"<title id=\"title\">{_escape(title)}</title>",
            f"<desc id=\"desc\">{_escape(description)}</desc>",
            f"<style>{style}</style>",
            *body,
            "</svg>",
            "",
        ]
    )


def _box(parts: list[str], x: float, y: float, width: float, height: float, *,
         stroke: str = GRAY, fill: str = "none", radius: int = 8, stroke_width: float = 1.4,
         dash: str | None = None) -> None:
    dash_attr = f' stroke-dasharray="{dash}"' if dash else ""
    parts.append(
        f'<rect x="{x:.1f}" y="{y:.1f}" width="{width:.1f}" height="{height:.1f}" rx="{radius}" '
        f'fill="{fill}" stroke="{stroke}" stroke-width="{stroke_width}"{dash_attr}/>'
    )


def _arrow(parts: list[str], x1: float, y1: float, x2: float, y2: float, *, color: str = BLUE,
           dash: str | None = None) -> None:
    dash_attr = f' stroke-dasharray="{dash}"' if dash else ""
    parts.append(f'<line x1="{x1:.1f}" y1="{y1:.1f}" x2="{x2:.1f}" y2="{y2:.1f}" stroke="{color}" stroke-width="2"{dash_attr}/>')
    if abs(x2 - x1) >= abs(y2 - y1):
        direction = 1 if x2 >= x1 else -1
        points = f"{x2:.1f},{y2:.1f} {x2 - direction * 10:.1f},{y2 - 5:.1f} {x2 - direction * 10:.1f},{y2 + 5:.1f}"
    else:
        direction = 1 if y2 >= y1 else -1
        points = f"{x2:.1f},{y2:.1f} {x2 - 5:.1f},{y2 - direction * 10:.1f} {x2 + 5:.1f},{y2 - direction * 10:.1f}"
    parts.append(f'<polygon points="{points}" fill="{color}"/>')


def _note(parts: list[str], x: float, y: float, text: str, *, color: str = GRAY, anchor: str = "start") -> None:
    _lines(parts, x, y, [text], size=15, fill=color, anchor=anchor)


def _motivation_timeline(_: Path) -> tuple[str, str, str, list[str], tuple[int, int]]:
    width, height = 1100, 520
    parts: list[str] = [
        f'<text x="50" y="46" font-size="25" font-weight="700" fill="{BLUE}">地址—订单协作：三个状态错误发生点</text>',
        f'<line x1="86" y1="166" x2="1022" y2="166" stroke="{GRAY}" stroke-width="1.5"/>',
    ]
    stages = [(100, "读地址", "客服 Agent"), (310, "派生建议", "物流 Agent"), (520, "写订单", "订单 Agent"), (730, "后继读取", "协作链")]
    for x, action, actor in stages:
        parts.append(f'<circle cx="{x}" cy="166" r="7" fill="{BLUE}"/>')
        _lines(parts, x, 128, [action], size=18, fill=DARK, anchor="middle", weight="600")
        _note(parts, x, 194, actor, anchor="middle")
    _arrow(parts, 114, 166, 296, 166, color=GRAY)
    _arrow(parts, 324, 166, 506, 166, color=GRAY)
    _arrow(parts, 534, 166, 716, 166, color=GRAY)

    risks = [
        (250, 255, "风险 1：写入中崩溃", ["地址已写、订单未写", "半更新对后继可见"], "crash"),
        (560, 380, "风险 2：提交时撤权", ["begin 时允许", "commit 时旧授权仍写入"], "revoke"),
        (830, 255, "风险 3：来源失效", ["地址源被更正", "派生建议仍被读取"], "source"),
    ]
    for x, y, heading, details, marker in risks:
        _box(parts, x - 145, y - 28, 290, 104, stroke=RED, fill="none", radius=7)
        parts.append(f'<circle cx="{x - 119}" cy="{y - 4}" r="11" fill="{RED}"/>')
        _lines(parts, x - 119, y + 2, ["!"], size=15, fill="white", anchor="middle", weight="700")
        _lines(parts, x - 100, y, [heading], size=17, fill=RED, weight="700")
        _lines(parts, x - 100, y + 23, details, size=15, fill=DARK, leading=20)
        target_y = 171 if marker != "revoke" else 171
        _arrow(parts, x, y - 30 if y < 300 else y - 32, x, target_y, color=RED, dash="4 3")
    _note(parts, 50, 485, "问题不是检索排序：共享 memory 必须同时覆盖原子公开、提交时策略与来源闭包。", color=DARK)
    caption = "地址—订单协作时间线：崩溃、提交时撤权和来源失效分别造成半更新、旧授权提交与派生污染。"
    alt = "从读取地址到写订单的时间线，标出三个红色风险点：写入中崩溃、提交时撤权、地址源失效。"
    sources = [
        "docs/paper/txnmem_ccfa_draft_zh.md",
        "results/final_controlled/results/schedule_baseline.json",
        "results/final_controlled/results/minimal_mutant_witnesses.json",
    ]
    return _svg(width, height, "TxnMem 动机时间线", alt, parts), caption, alt, sources, (width, height)


def _architecture(_: Path) -> tuple[str, str, str, list[str], tuple[int, int]]:
    width, height = 1160, 620
    parts: list[str] = [
        f'<text x="50" y="46" font-size="25" font-weight="700" fill="{BLUE}">TxnMem 的确定性语义 core 与逐事件适配边界</text>',
    ]
    _box(parts, 46, 76, 285, 480, stroke=GRAY, fill="none", radius=10)
    _lines(parts, 68, 108, ["原生模型 / 公共 runtime / backend"], size=18, fill=DARK, weight="700")
    adapter_boxes = [(135, "Qwen tool loop"), (250, "τ-bench / AppWorld / LoCoMo"), (365, "Qdrant / Neo4j / services")]
    for y, label in adapter_boxes:
        _box(parts, 72, y, 232, 72, stroke=GRAY, fill=LIGHT_GRAY, radius=6)
        _lines(parts, 188, y + 31, [label], size=16, fill=DARK, anchor="middle", weight="600")
        _lines(parts, 188, y + 53, ["per-event adapter"], size=15, fill=GRAY, anchor="middle")
    _box(parts, 68, 455, 240, 76, stroke=RED, fill="none", radius=6)
    _lines(parts, 188, 482, ["逐事件接线"], size=17, fill=RED, anchor="middle", weight="700")
    _lines(parts, 188, 505, ["非事务适配；不提供事务"], size=15, fill=DARK, anchor="middle")
    _lines(parts, 188, 526, ["不缓冲跨工具调用写集"], size=15, fill=GRAY, anchor="middle")

    _box(parts, 370, 76, 494, 480, stroke=BLUE, fill="none", radius=10, stroke_width=2)
    _lines(parts, 394, 108, ["确定性 TxnMem core（事务语义）"], size=20, fill=BLUE, weight="700")
    components = [
        (405, 146, 188, 86, "Agent API", ["begin · read · write"]),
        (638, 146, 188, 86, "Transaction Manager", ["buffer · atomic decision"]),
        (405, 292, 188, 86, "Policy Engine", ["latest-policy revalidation"]),
        (638, 292, 188, 86, "Memory Store", ["committed objects only"]),
        (521, 430, 188, 86, "Provenance Repair", ["descendant invalidation-only"]),
    ]
    for x, y, w, h, title, subtitle in components:
        _box(parts, x, y, w, h, stroke=BLUE, fill="none", radius=6)
        _lines(parts, x + w / 2, y + 34, [title], size=16, fill=DARK, anchor="middle", weight="700")
        _lines(parts, x + w / 2, y + 58, subtitle, size=15, fill=GRAY, anchor="middle")
    _arrow(parts, 593, 189, 638, 189)
    _arrow(parts, 638, 216, 593, 292)
    _arrow(parts, 593, 335, 638, 335)
    _arrow(parts, 650, 430, 732, 378)
    _note(parts, 707, 418, "invalidation", color=GRAY, anchor="middle")
    _arrow(parts, 304, 220, 405, 189, color=GRAY)
    _note(parts, 322, 207, "event contract", anchor="middle")

    _box(parts, 904, 76, 210, 480, stroke=GRAY, fill="none", radius=10)
    _lines(parts, 925, 108, ["独立 reference simulator"], size=18, fill=DARK, weight="700")
    _box(parts, 928, 150, 162, 86, stroke=GRAY, fill=LIGHT_GRAY, radius=6)
    _lines(parts, 1009, 184, ["serial semantics"], size=16, fill=DARK, anchor="middle", weight="600")
    _lines(parts, 1009, 207, ["合法线性化集合"], size=15, fill=GRAY, anchor="middle")
    _box(parts, 928, 290, 162, 86, stroke=GRAY, fill="none", radius=6)
    _lines(parts, 1009, 324, ["differential oracle"], size=16, fill=DARK, anchor="middle", weight="600")
    _lines(parts, 1009, 347, ["比较可观察历史"], size=15, fill=GRAY, anchor="middle")
    _arrow(parts, 864, 335, 928, 335, color=GRAY)
    _arrow(parts, 1009, 236, 1009, 286, color=GRAY)
    _note(parts, 50, 592, "事务性承诺只来自 TxnMem core；适配器、模型与服务保持为可替换的逐事件边界。", color=DARK)
    caption = "TxnMem 架构：确定性 core/reference simulator 承担事务语义；Qwen、公共 runtime 与后端仅通过逐事件适配接入。"
    alt = "左侧是 Qwen、公共 runtime 和后端的非事务逐事件适配；中间是 TxnMem 的事务管理、策略、存储和来源修复 core；右侧是独立 reference simulator。"
    sources = ["docs/paper/txnmem_ccfa_draft_zh.md", "configs/txnmem_ccfa_paper.json"]
    return _svg(width, height, "TxnMem 架构边界", alt, parts), caption, alt, sources, (width, height)


def _commit_protocol(_: Path) -> tuple[str, str, str, list[str], tuple[int, int]]:
    width, height = 1120, 500
    parts: list[str] = [
        f'<text x="50" y="46" font-size="25" font-weight="700" fill="{BLUE}">提交协议：最新策略下重验证，再原子决议</text>',
        f'<line x1="80" y1="244" x2="1030" y2="244" stroke="{GRAY}" stroke-width="1.4"/>',
    ]
    steps = [
        (84, "begin", ["记录 policy version"]),
        (290, "buffer", ["write / derive / propagate", "仅事务缓冲区可见"]),
        (550, "最新策略重验证", ["read / write / propagation set", "使用 commit 时策略"]),
        (852, "原子 persist", ["公开整个 write set"]),
    ]
    widths = [150, 205, 244, 172]
    for (x, title, detail), box_width in zip(steps, widths):
        _box(parts, x, 168, box_width, 146, stroke=BLUE, fill="none", radius=7)
        _lines(parts, x + box_width / 2, 205, [title], size=18, fill=BLUE, anchor="middle", weight="700")
        _lines(parts, x + box_width / 2, 234, detail, size=15, fill=DARK, anchor="middle", leading=20)
    _arrow(parts, 234, 244, 286, 244)
    _arrow(parts, 499, 244, 546, 244)
    _arrow(parts, 798, 244, 848, 244)
    _box(parts, 573, 354, 194, 68, stroke=RED, fill="none", radius=7)
    _lines(parts, 670, 382, ["策略拒绝 / 冲突 → abort"], size=16, fill=RED, anchor="middle", weight="700")
    _lines(parts, 670, 405, ["丢弃缓冲写集"], size=15, fill=DARK, anchor="middle")
    _arrow(parts, 670, 315, 670, 350, color=RED)
    _lines(parts, 935, 388, ["崩溃恢复可见结果："], size=17, fill=DARK, anchor="middle", weight="700")
    _lines(parts, 935, 416, ["完整提交 / 未提交"], size=18, fill=BLUE, anchor="middle", weight="700")
    _note(parts, 50, 470, "恢复判定只接受一个原子决议；写集的真子集不属于合法观察结果。", color=DARK)
    caption = "TxnMem commit 协议：begin 后写入仅缓冲，commit 点以最新策略重验证，并原子持久化或 abort。"
    alt = "从 begin 到 buffer、最新策略重验证和原子持久化的协议图，策略拒绝分支指向 abort，崩溃恢复只有完整提交或未提交两种结果。"
    sources = ["docs/paper/txnmem_ccfa_draft_zh.md", "results/final_controlled/results/schedule_baseline.json"]
    return _svg(width, height, "TxnMem 提交协议", alt, parts), caption, alt, sources, (width, height)


def _provenance_repair(_: Path) -> tuple[str, str, str, list[str], tuple[int, int]]:
    width, height = 1120, 540
    parts: list[str] = [
        f'<text x="50" y="46" font-size="25" font-weight="700" fill="{BLUE}">Provenance repair：当前为后代失效闭包</text>',
        f'<text x="50" y="77" font-size="15" fill="{GRAY}">地址源被撤销或取代后，沿已记录的反向依赖使后代退出默认可见集合。</text>',
    ]
    nodes = [
        (105, 180, "地址 v1", "source", RED),
        (380, 180, "发货建议", "derived", BLUE),
        (655, 116, "订单更新", "derived", BLUE),
        (655, 244, "客服副本", "propagated", BLUE),
        (918, 180, "下游计划", "descendant", BLUE),
    ]
    for x, y, label, kind, color in nodes:
        _box(parts, x, y, 150, 70, stroke=color, fill="none", radius=7, stroke_width=2 if color == RED else 1.4)
        _lines(parts, x + 75, y + 30, [label], size=18, fill=DARK, anchor="middle", weight="700")
        _lines(parts, x + 75, y + 53, [kind], size=15, fill=GRAY, anchor="middle")
    _arrow(parts, 255, 215, 376, 215)
    _arrow(parts, 530, 207, 651, 151)
    _arrow(parts, 530, 223, 651, 279)
    _arrow(parts, 805, 151, 914, 207)
    _arrow(parts, 805, 279, 914, 223)
    _lines(parts, 180, 142, ["source invalidated"], size=15, fill=RED, anchor="middle", weight="700")
    _arrow(parts, 180, 156, 180, 176, color=RED)

    _box(parts, 355, 342, 540, 78, stroke=BLUE, fill="none", radius=7)
    _lines(parts, 625, 373, ["当前：仅后代失效"], size=19, fill=BLUE, anchor="middle", weight="700")
    _lines(parts, 625, 399, ["发货建议、订单更新、客服副本与下游计划 → invalid / 不再默认可见"], size=15, fill=DARK, anchor="middle")
    _arrow(parts, 180, 252, 450, 338, color=BLUE, dash="5 3")
    _arrow(parts, 730, 316, 730, 338, color=BLUE, dash="5 3")

    _box(parts, 355, 448, 540, 58, stroke=RED, fill="none", radius=7, dash="6 4")
    _lines(parts, 625, 473, ["未来扩展（不属于当前路径）：stale / 重算 / 新来源 repair"], size=16, fill=RED, anchor="middle", weight="700")
    _lines(parts, 625, 494, ["需要额外状态、授权条件与证据"], size=15, fill=DARK, anchor="middle")
    caption = "当前 provenance repair 仅沿已记录依赖执行后代失效闭包；stale、重算和新来源修复均为未来扩展。"
    alt = "地址源失效后，发货建议、订单更新、客服副本和下游计划组成的依赖图被后代失效闭包处理；下方虚线框将 stale、重算和新来源 repair 标为未来扩展。"
    sources = ["docs/paper/txnmem_ccfa_draft_zh.md", "results/final_controlled/results/minimal_mutant_witnesses.json"]
    return _svg(width, height, "TxnMem 来源修复边界", alt, parts), caption, alt, sources, (width, height)


def _controlled_results(root: Path) -> tuple[str, str, str, list[str], tuple[int, int]]:
    artifact = "results/paper_evidence/controlled_suite.json"
    rows = controlled_result_rows(root)
    instance_count = rows[0]["instance_count"]
    width, height = 1100, 570
    parts: list[str] = [
        f'<text x="50" y="46" font-size="25" font-weight="700" fill="{BLUE}">受控套件：违规数与独立 oracle 一致性</text>',
        f'<text x="50" y="75" font-size="15" fill="{GRAY}">{instance_count} instances × {len(rows)} variants；横条为目标违规数（分母 {instance_count}）。</text>',
        f'<line x1="250" y1="112" x2="950" y2="112" stroke="{GRAY}" stroke-width="1.2"/>',
    ]
    for tick in range(0, instance_count + 1, 100):
        x = 250 + 700 * tick / instance_count
        parts.append(f'<line x1="{x:.1f}" y1="112" x2="{x:.1f}" y2="470" stroke="{LIGHT_GRAY}" stroke-width="1"/>')
        _note(parts, x, 100, str(tick), anchor="middle")
    _lines(parts, 50, 126, ["variant"], size=15, fill=GRAY, weight="700")
    _lines(parts, 978, 126, ["oracle match"], size=15, fill=GRAY, anchor="middle", weight="700")
    for index, row in enumerate(rows):
        y = 160 + index * 64
        name = row["variant"]
        violations = row["violation_count"]
        matches = row["oracle_match_count"]
        color = BLUE if name == "TxnMem" else (RED if name == "Naive" else GRAY)
        _lines(parts, 50, y + 22, [name], size=17, fill=DARK, weight="700" if name == "TxnMem" else "400")
        parts.append(f'<rect x="250" y="{y:.1f}" width="700" height="30" fill="none" stroke="{GRAY}" stroke-width="1"/>')
        if violations:
            parts.append(f'<rect x="250" y="{y:.1f}" width="{700 * violations / instance_count:.1f}" height="30" fill="{color}"/>')
        else:
            parts.append(f'<rect x="250" y="{y:.1f}" width="4" height="30" fill="{BLUE}"/>')
        _lines(parts, 250 + max(7, 700 * violations / instance_count) + 10, y + 22, [f"{violations}"], size=16, fill=color, weight="700")
        _lines(parts, 1005, y + 22, [f"{matches}/{instance_count}"], size=17, fill=BLUE if matches == instance_count else DARK, anchor="middle", weight="700" if matches == instance_count else "400")
    _lines(parts, 50, 515, ["解释：这是确定性 controlled simulator 对独立 reference semantics 的结果，不是公开任务 accuracy。"], size=15, fill=DARK)
    caption = "受控套件主结果：五个实现变体的目标违规数和独立 oracle match 数；完整 TxnMem 为 0/400 与 400/400。"
    alt = "五条横向违规条显示 Naive 350、TxnMem-NoTxn 200、TxnMem-NoPolicyCommit 50、TxnMem-NoRepair 100、TxnMem 0；右侧 oracle match 分别为 50、200、350、300、400（分母均为 400）。"
    return _svg(width, height, "受控套件主结果", alt, parts), caption, alt, [artifact], (width, height)


def _evidence_layers(root: Path) -> tuple[str, str, str, list[str], tuple[int, int]]:
    config_path = "configs/txnmem_ccfa_paper.json"
    claims_path = "configs/paper_claims.json"
    schedule_path = "results/final_controlled/results/schedule_baseline.json"
    witnesses_path = "results/final_controlled/results/minimal_mutant_witnesses.json"
    toxiproxy_path = "results/submission_evidence/toxiproxy_faults_30/aggregate.json"
    cross_host_path = "results/cross_host_model_load_formal_v8_aggregate/results/model_load_repetition_summary.json"
    native_path = "results/remaining_tasks/native_repetitions5/repetition_report.json"
    config = _load_json(root, config_path)
    claims = _load_json(root, claims_path)
    schedule = _load_json(root, schedule_path)
    witnesses = _load_json(root, witnesses_path)
    toxiproxy = _load_json(root, toxiproxy_path)
    cross_host = _load_json(root, cross_host_path)
    native = _load_json(root, native_path)
    active_claims = {claim["claim_id"]: claim["claim_boundary"] for claim in claims["claims"] if claim["status"] == "active"}
    width, height = 1160, 650
    parts: list[str] = [
        f'<text x="50" y="46" font-size="25" font-weight="700" fill="{BLUE}">分层证据：每层回答不同问题，不能互换</text>',
        f'<text x="50" y="75" font-size="15" fill="{GRAY}">图层按证据对象组织，而非系统“完成状态”；所有结论受各自 claim boundary 约束。</text>',
    ]
    layers = [
        (112, "受控正确性", f"{schedule['causal_case_count']} instances；{witnesses['witness_count']} 个最小 witness；独立 oracle", "只证明受控语义，不是 public-task accuracy", BLUE),
        (206, "原生模型", f"Qwen tool loop：{native['repetitions']}×10 tasks；逐事件 contract / oracle", "机制接线，不是 end-user quality", GRAY),
        (300, "公共 runtime", "τ-bench / AppWorld / LoCoMo：workflow 与适配边界", "workflow reward / F1 不是 memory accuracy", GRAY),
        (394, "真实服务", f"Toxiproxy：{toxiproxy['scenario_count']}×{toxiproxy['repetitions_per_scenario']}；仅故障/响应路径", "未独立核验双存储状态；非原子性/可用性/延迟证据", GRAY),
        (488, "跨主机", f"{cross_host['repetition_count']} 次 client→model-server attested repetitions", "不是多 host Agent workers、连续 tunnel 或 production latency", RED),
    ]
    for y, label, evidence, boundary, color in layers:
        _box(parts, 70, y, 1020, 72, stroke=color, fill="none", radius=8, stroke_width=2 if color == BLUE else 1.4)
        _box(parts, 88, y + 15, 150, 42, stroke=color, fill=LIGHT_GRAY if color != RED else "none", radius=5)
        _lines(parts, 163, y + 42, [label], size=18, fill=DARK, anchor="middle", weight="700")
        _lines(parts, 265, y + 32, [evidence], size=16, fill=DARK, weight="600")
        _lines(parts, 265, y + 55, ["边界：" + boundary], size=15, fill=RED if color == RED else GRAY)
    _lines(parts, 70, 599, ["设计配置声明的正文图：" + "、".join(config["body_figure_ids"])], size=15, fill=GRAY)
    _lines(parts, 70, 623, ["证据链强调：controlled correctness → 接线/服务/拓扑的外部相关性；后者不改写前者的语义结论。"], size=15, fill=DARK)
    caption = "TxnMem 的分层证据链：从受控正确性到模型、公共 runtime、真实服务和跨主机证据；Toxiproxy 层仅覆盖故障/响应路径，状态未独立核验。"
    alt = "五层证据图依次为受控正确性、原生模型、公共 runtime、真实服务和跨主机；Toxiproxy 层明确标注只观察代理故障与响应路径，未独立核验双存储状态。"
    sources = [config_path, claims_path, schedule_path, witnesses_path, toxiproxy_path, cross_host_path, native_path]
    # Access the claim ledger as part of construction, rather than presenting an unbound status view.
    if "controlled_correctness_400x5" not in active_claims:
        raise ValueError("controlled correctness claim is not active")
    return _svg(width, height, "TxnMem 分层证据", alt, parts), caption, alt, sources, (width, height)


BUILDERS: dict[str, Callable[[Path], tuple[str, str, str, list[str], tuple[int, int]]]] = {
    "motivation_timeline": _motivation_timeline,
    "architecture": _architecture,
    "commit_protocol": _commit_protocol,
    "provenance_repair": _provenance_repair,
    "controlled_results": _controlled_results,
    "evidence_layers": _evidence_layers,
}


def build_all(root: Path, out_dir: Path) -> dict[str, Any]:
    """Write all manuscript SVGs and a reproducible source/output-hash manifest."""
    root = root.resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    figures: dict[str, Any] = {}
    for figure_id in REQUIRED_FIGURE_IDS:
        svg, caption, alt_text, source_paths, dimensions = BUILDERS[figure_id](root)
        filename = f"{figure_id}.svg"
        output_path = out_dir / filename
        output_path.write_text(svg, encoding="utf-8")
        figures[figure_id] = {
            "file": filename,
            "sources": [
                {"path": source_path, "sha256": _sha256(root / source_path)}
                for source_path in source_paths
            ],
            "dimensions": {"width": dimensions[0], "height": dimensions[1]},
            "caption": caption,
            "alt_text": alt_text,
            "output_sha256": _sha256(output_path),
        }
    manifest = {"schema_version": 1, "figures": figures}
    (out_dir / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("."))
    parser.add_argument("--out-dir", type=Path, default=Path("paper_assets/figures"))
    args = parser.parse_args()
    manifest = build_all(args.root, args.out_dir)
    print(f"generated {len(manifest['figures'])} SVG figures in {args.out_dir}")


if __name__ == "__main__":
    main()
