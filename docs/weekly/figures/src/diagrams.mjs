// Vẽ các sơ đồ Hình 1, 2, 3, 5, 6, 7 của báo cáo tuần bằng Graphviz (viz-js), ra SVG.
// Nội dung theo bản đặc tả trong docs/system (Hình 1.1, 4.3, 2.6, 3.2, 4.5, 4.6) và observability/alloy/config.alloy.
// Chạy: npm i @viz-js/viz@3.11.0 && node diagrams.mjs <thư mục ra>
import { instance } from "@viz-js/viz";
import { writeFileSync } from "node:fs";

const out = process.argv[2] ?? ".";
// Màu theo tầng: [viền/chữ đậm, nền nhạt]
const C = {
  src: ["#455a64", "#eceff1"], crawl: ["#00838f", "#e0f7fa"], lake: ["#1565c0", "#e3f2fd"],
  kafka: ["#c62828", "#ffebee"], speed: ["#6a1b9a", "#f3e5f5"], batch: ["#2e7d32", "#e8f5e9"],
  serve: ["#e65100", "#fff3e0"], orch: ["#37474f", "#eceff1"], obs: ["#5d4037", "#efebe9"],
  gate: ["#c62828", "#ffebee"], plain: ["#757575", "#ffffff"],
};
const SHAPE = { store: "cylinder", topic: "hexagon", proc: "box", ext: "box", dec: "diamond" };

const HEAD = `
  graph [fontname="Helvetica", fontsize=11, bgcolor="white", pad=0.2, nodesep=0.3, ranksep=0.45, newrank=true];
  node  [fontname="Helvetica", fontsize=10, penwidth=1.3, margin="0.12,0.06"];
  edge  [fontname="Helvetica", fontsize=9, color="#546e7a", fontcolor="#37474f", penwidth=1.1, arrowsize=0.7];`;

const lab = (s) => s.includes("<") ? `<${s}>` : `"${s}"`;
function n(id, label, kind, layer, extra = "") {
  const [line, fill] = C[layer];
  const shape = SHAPE[kind];
  const style = kind === "proc" ? "rounded,filled" : "filled";
  return `${id} [label=${lab(label)}, shape=${shape}, style="${style}", color="${line}", fillcolor="${fill}"${extra}];`;
}
function cluster(id, title, layer, body, extra = "") {
  const [line] = C[layer];
  return `subgraph cluster_${id} { label=<<B>${title}</B>>; fontcolor="${line}"; color="${line}"; style="rounded,dashed"; penwidth=1.0; margin=10; ${extra}
    ${body.join("\n    ")}
  }`;
}
const e = (a, b, attrs = "") => `${a} -> ${b}${attrs ? ` [${attrs}]` : ""};`;
const BOLD = 'penwidth=2.2, color="#263238"';
const DASH = 'style=dashed';
const t = (s) => `<FONT FACE="Courier" POINT-SIZE="9">${s}</FONT>`;

const figs = {};

// ---------------------------------------------------------------- Hình 1: kiến trúc tổng thể
figs["fig-1-kien-truc-tong-the"] = `digraph G { rankdir=TB; ${HEAD}
  graph [nodesep=0.35, ranksep=0.4];
  ${cluster("src", "Nguồn ngoài", "src", [
    n("tiki", `Tiki`, "ext", "src"),
    n("laz", `Lazada`, "ext", "src")])}
  ${cluster("crawl", "Thu thập", "crawl", [
    n("frontier", `PostgreSQL<BR/>${t("audit.crawl_frontier")}`, "store", "crawl"),
    n("worker", "crawl-worker<BR/>Fetcher → Extractor", "proc", "crawl")])}
  ${cluster("bronze", "Bronze (MinIO/S3) – nguồn sự thật", "lake", [
    n("raw", `raw`, "store", "lake"),
    n("records", `records`, "store", "lake", ', penwidth=2.2'),
    n("caudit", `crawl_audit`, "store", "lake")])}
  ${cluster("speed", "Speed layer (Spark Structured Streaming)", "speed", [
    n("k1", t("records.v1"), "topic", "kafka"),
    n("norm", "normalizer", "proc", "speed"),
    n("k2", t("observations.v1"), "topic", "kafka"),
    n("state", "stateful query<BR/>7 loại thay đổi", "proc", "speed"),
    n("k3", t("changes.v1"), "topic", "kafka")])}
  ${cluster("batch", "Batch layer (Airflow, mỗi giờ)", "batch", [
    n("silver", "Silver (Delta)", "store", "lake"),
    n("dw", `Gold ${t("dw")}`, "store", "lake"),
    n("runs", "Gold runs<BR/>12 mart theo run", "store", "lake"),
    n("gate", "quality gate<BR/>22 luật", "dec", "gate"),
    n("ptr", t("current.json"), "store", "lake")])}
  ${cluster("serve", "Phục vụ", "serve", [
    n("pg", `PostgreSQL ${t("cache.*")}`, "store", "serve"),
    n("es", "Elasticsearch", "store", "serve"),
    n("redis", `Redis ${t("rt:*")}`, "store", "serve"),
    n("superset", "Superset", "ext", "serve"),
    n("kibana", "Kibana", "ext", "serve")])}
  ${n("airflow", "Airflow 3 – 4 DAG", "ext", "orch")}
  ${n("graf", "Alloy · Prometheus · Loki · Grafana", "ext", "obs")}
  ${e("tiki", "worker")} ${e("laz", "worker")}
  ${e("frontier", "worker", `${DASH}, label="lease"`)}
  ${e("worker", "raw", BOLD)} ${e("worker", "records", BOLD)} ${e("worker", "caudit")}
  ${e("records", "k1", BOLD)} ${e("k1", "norm")} ${e("norm", "k2")} ${e("k2", "state")} ${e("state", "k3")}
  ${e("state", "es")} ${e("state", "redis")}
  ${e("records", "silver", BOLD)} ${e("silver", "dw", BOLD)} ${e("caudit", "dw")}
  ${e("dw", "runs", BOLD)} ${e("runs", "gate", BOLD)}
  ${e("gate", "pg", `${BOLD}, label="PASS"`)} ${e("gate", "ptr", 'label="promote"')}
  ${e("pg", "superset")} ${e("es", "kibana")}
  ${e("airflow", "silver", `${DASH}, label="lập lịch mỗi giờ"`)}
  ${e("airflow", "frontier", `${DASH}, label="seed SWEEP", constraint=false`)}
  ${e("graf", "pg", `${DASH}, label="đọc audit", constraint=false`)}
  { rank=same; frontier; worker; airflow; }
  { rank=same; superset; kibana; graf; }
}`;

// ---------------------------------------------------------------- Hình 2: một lượt crawl
figs["fig-2-luong-crawl"] = `digraph G { rankdir=TB; ${HEAD}
  graph [nodesep=0.35, ranksep=0.32];
  lease  [label=<Lấy task: ${t("lease_due(10)")}>, shape=box, style="rounded,filled", color="${C.crawl[0]}", fillcolor="${C.crawl[1]}"];
  frontier [label=<PostgreSQL<BR/>${t("audit.crawl_frontier")}>, shape=cylinder, style=filled, color="${C.crawl[0]}", fillcolor="${C.crawl[1]}"];
  ${n("circuit", "Circuit của<BR/>nguồn đang mở?", "dec", "plain")}
  ${n("robots", "đọc robots.txt<BR/>giãn cách request", "proc", "crawl")}
  ${n("fetch", "GET trang<BR/>(Lazada: Playwright + bắt XHR)", "proc", "crawl")}
  ${n("site", "Tiki / Lazada", "ext", "src")}
  ${n("raw", `Bronze/raw`, "store", "lake")}
  ${n("status", "HTTP status OK?", "dec", "plain")}
  ${n("extract", "extract → bỏ PII → build_records", "proc", "crawl")}
  ${n("records", `Bronze/records`, "store", "lake")}
  ${n("kafka", `Kafka ${t("records.v1")}`, "topic", "kafka")}
  ${n("ok", "Kết quả?", "dec", "plain")}
  ${n("succ", "SUCCEEDED<BR/>+ hẹn lần crawl sau", "proc", "batch")}
  ${n("retry", "RETRY_WAIT<BR/>thử lại sau", "proc", "serve")}
  ${n("fail", "FAILED", "proc", "gate")}
  ${n("audit", `${t("audit.crawl_*")} + lake crawl_audit`, "store", "crawl")}
  ${n("wait", "trả task về hàng đợi", "proc", "plain")}
  frontier -> lease;
  ${e("lease", "circuit")}
  ${e("circuit", "wait", 'label="mở"')}
  ${e("circuit", "robots", 'label="đóng"')}
  ${e("robots", "fetch")} ${e("fetch", "site", 'dir=both, label="request / response"')}
  ${e("fetch", "raw", BOLD)}
  ${e("raw", "status", 'label="lưu raw TRƯỚC khi kiểm status"')}
  ${e("status", "extract", 'label="2xx"')}
  ${e("status", "ok", 'label="429 / 5xx / chặn"')}
  ${e("extract", "records", BOLD)} ${e("records", "kafka", BOLD)} ${e("kafka", "ok")}
  ${e("ok", "succ", 'label="thành công"')} ${e("ok", "retry", 'label="lỗi tạm thời"')} ${e("ok", "fail", 'label="lỗi vĩnh viễn"')}
  ${e("succ", "audit")} ${e("retry", "audit")} ${e("fail", "audit")}
  { rank=same; fetch; site; }
  { rank=same; circuit; wait; }
}`;

// ---------------------------------------------------------------- Hình 3: dữ liệu qua các tầng
const layer = (id, title, fmt, grain, ly) =>
  `${id} [shape=box, style="rounded,filled", color="${C[ly][0]}", fillcolor="${C[ly][1]}", label=<<TABLE BORDER="0" CELLPADDING="2" CELLSPACING="0">
    <TR><TD><B>${title}</B></TD></TR>
    <TR><TD><FONT POINT-SIZE='8' COLOR="#37474f">${fmt}</FONT></TD></TR>
    <TR><TD><FONT POINT-SIZE='8' COLOR="#607d8b"><I>${grain}</I></FONT></TD></TR></TABLE>>];`;
figs["fig-3-du-lieu-qua-cac-tang"] = `digraph G { rankdir=TB; ${HEAD}
  graph [nodesep=0.7, ranksep=0.6];
  ${layer("raw", "Bronze · raw", "bin.gz + metadata.json", "1 phản hồi / trang", "lake")}
  ${layer("rec", "Bronze · records", "JSONL.gz, payload gốc", "≤ 40 listing_row + 1 context / trang", "lake")}
  ${layer("silver", "Silver · offer_observations_v2", "Delta, 53 cột<BR/>+ *_listing_wide, page_context, quarantine", "1 observation hợp lệ", "batch")}
  ${layer("star", "Gold · star dw", "Delta, MERGE<BR/>7 dim + 1 bridge + 6 fact", "1 sighting (fact chính)", "batch")}
  ${layer("mart", "Gold · 12 mart / run", "Delta, thư mục run_id=…", "vd. 1 offer (offer_current)", "batch")}
  ${layer("cache", "PostgreSQL · cache.*", "12 bảng, TRUNCATE + INSERT<BR/>trong 1 transaction", "bản của run đã promote", "serve")}
  ${e("raw", "rec", `${BOLD}, label="tách dòng\nbọc lineage"`)} ${e("rec", "silver", `${BOLD}, label="chuẩn hoá\ndedup, quarantine"`)}
  ${e("silver", "star", `${BOLD}, label="  SCD2, xxhash64"`)}
  ${e("mart", "star", `${BOLD}, dir=back, label="lọc ≤ as_of\nbỏ quảng cáo"`)}
  ${e("cache", "mart", `${BOLD}, dir=back, label="quality PASS\npublish"`)}
  { rank=same; raw; rec; silver; }
  { rank=same; cache; mart; star; }
}`;

// ---------------------------------------------------------------- Hình 5: speed layer
figs["fig-5-speed"] = `digraph G { rankdir=TB; ${HEAD}
  graph [nodesep=0.3, ranksep=0.35];
  ${n("k1", t("records.v1"), "topic", "kafka")}
  ${cluster("q1", "Query 1: normalizer (không state)", "speed", [
    n("dec", t("decode_records"), "proc", "speed"),
    n("norm", `${t("to_offer_observation")}<BR/>+ ${t("split_quarantine")}`, "proc", "speed"),
    n("drop", "bỏ (quảng cáo)", "ext", "plain")])}
  ${n("dlq1", t("records.v1.dlq"), "topic", "kafka")}
  ${n("k2", t("observations.v1"), "topic", "kafka", ", penwidth=2.2")}
  ${cluster("q2", "Query 2: stateful theo offer_id", "speed", [
    n("contract", "kiểm hợp đồng<BR/>event_id, key, giá ≥ 0", "dec", "plain"),
    n("inv", "INVALID", "proc", "speed"),
    n("grp", `${t("groupBy(offer_id)")}<BR/>${t("applyInPandasWithState")}`, "proc", "speed"),
    n("cmp", "so với<BR/>OfferState", "dec", "plain"),
    n("dup", "DUPLICATE (bỏ)", "proc", "plain"),
    n("late", "LATE (bỏ)", "proc", "plain"),
    n("chg", "STATE + 0..n CHANGE", "proc", "speed"),
    n("bw", "BatchWriter<BR/>Kafka → ES → Redis → audit", "proc", "speed")])}
  ${n("dlq2", t("observations.v1.dlq"), "topic", "kafka")}
  ${n("k3", t("changes.v1"), "topic", "kafka")}
  ${n("es", "Elasticsearch · Redis", "store", "serve")}
  ${e("k1", "dec", BOLD)} ${e("dec", "norm", BOLD)}
  ${e("norm", "drop", 'label="quảng cáo"')} ${e("norm", "dlq1", 'label="lỗi"')} ${e("norm", "k2", `${BOLD}, label="hợp lệ"`)}
  ${e("k2", "contract", BOLD)} ${e("contract", "inv", 'label="sai"')} ${e("inv", "dlq2")}
  ${e("contract", "grp", `${BOLD}, label="đúng"`)} ${e("grp", "cmp", BOLD)}
  ${e("cmp", "dup", 'label="cùng observation_id"')} ${e("cmp", "late", 'label="cũ hơn state"')} ${e("cmp", "chg", `${BOLD}, label="mới"`)}
  ${e("chg", "bw", BOLD)} ${e("bw", "k3", BOLD)} ${e("bw", "es", BOLD)}
}`;

// ---------------------------------------------------------------- Hình 6: DAG + state machine
const st = (id, label, ly, extra = "") => `${id} [label="${label}", shape=box, style="rounded,filled", color="${C[ly][0]}", fillcolor="${C[ly][1]}", fontsize=9${extra}];`;
figs["fig-6-dag-batch"] = `digraph G { rankdir=TB; ${HEAD}
  graph [nodesep=0.3, ranksep=0.3, compound=true];
  ${cluster("dag", "DAG marketplace_batch", "orch", [
    n("sensor", "window_settled", "proc", "orch"),
    n("s1", "silver", "proc", "batch"), n("s2", "dimensions", "proc", "batch"), n("s3", "facts", "proc", "batch"),
    n("s4", "marts", "proc", "batch"),
    n("s5", "quality", "proc", "gate", ", penwidth=2.2"),
    n("s6", "publish", "proc", "batch"), n("s7", "promote", "proc", "batch"),
    e("sensor", "s1"), e("s1", "s2"), e("s2", "s3"), e("s3", "s4"), e("s4", "s5"), e("s5", "s6"), e("s6", "s7")])}
  ${cluster("sm", "Trạng thái run", "orch", [
    'start [shape=circle, label="", width=0.15, style=filled, fillcolor="#263238", color="#263238"];',
    'end [shape=doublecircle, label="", width=0.12, style=filled, fillcolor="#263238", color="#263238"];',
    st("CREATED", "CREATED", "plain"), st("SILVER_BUILT", "SILVER_BUILT", "plain"),
    st("DIMENSIONS_BUILT", "DIMENSIONS_BUILT", "plain"), st("FACTS_BUILT", "FACTS_BUILT", "plain"),
    st("GOLD_WRITTEN", "GOLD_WRITTEN", "plain"), st("QUALITY_PASSED", "QUALITY_PASSED", "plain"),
    st("PUBLISHED", "PUBLISHED", "plain"), st("PROMOTED", "PROMOTED", "batch", ", penwidth=2.2"),
    st("QUALITY_FAILED", "QUALITY_FAILED", "gate"), st("FAILED", "FAILED", "gate"), st("HELD", "HELD", "serve"),
    'start -> CREATED;',
    'CREATED -> SILVER_BUILT [label="silver"];', 'SILVER_BUILT -> DIMENSIONS_BUILT [label="dimensions"];',
    'DIMENSIONS_BUILT -> FACTS_BUILT [label="facts"];', 'FACTS_BUILT -> GOLD_WRITTEN [label="marts"];',
    'GOLD_WRITTEN -> QUALITY_PASSED [label="PASS"];', 'QUALITY_PASSED -> PUBLISHED [label="publish"];',
    'PUBLISHED -> PROMOTED [label="promote (CAS)"];', 'PROMOTED -> end;',
    'GOLD_WRITTEN -> QUALITY_FAILED [label="FAIL\\nRESTORE star", color="#c62828", fontcolor="#c62828"];',
    'QUALITY_FAILED -> GOLD_WRITTEN [label="--resume", style=dashed];',
    'PUBLISHED -> HELD [label="pointer conflict", color="#e65100", fontcolor="#e65100"];',
    'QUALITY_PASSED -> HELD [label="backfill bị từ chối", color="#e65100", fontcolor="#e65100"];',
    'HELD -> PROMOTED [style=dashed, label="chạy lại"];',
    '{ CREATED SILVER_BUILT DIMENSIONS_BUILT FACTS_BUILT } -> FAILED [color="#c62828", label="", arrowsize=0.5, penwidth=0.8];',
    'FAILED -> CREATED [style=dashed, label="--resume", constraint=false];',
    ''])}
  sensor -> start [style=invis]; { rank=same; sensor; start; }
}`;

// ---------------------------------------------------------------- Hình 7: giám sát
figs["fig-7-giam-sat"] = `digraph G { rankdir=LR; ${HEAD}
  graph [nodesep=0.18, ranksep=0.45];
  ${cluster("srcm", "Nguồn metrics", "orch", [
    n("cad", "cAdvisor", "ext", "plain"),
    n("host", "node (unix)", "ext", "plain"),
    n("exp", "exporter Kafka · Redis ·<BR/>Elasticsearch", "ext", "plain"),
    n("statsd", "StatsD của Airflow", "ext", "plain"),
    n("probe", "blackbox HTTP probe", "ext", "plain"),
    n("sql", `SQL trên ${t("audit.*")}&nbsp;mỗi phút`, "store", "crawl")])}
  ${cluster("srcl", "Nguồn log", "orch", [
    n("dlog", "log container Docker", "ext", "plain"),
    n("alog", "log task Airflow (file)", "ext", "plain")])}
  ${n("alloy", "Grafana Alloy", "proc", "obs", ", penwidth=2.2")}
  ${n("prom", "Prometheus", "store", "obs")}
  ${n("loki", "Loki", "store", "obs")}
  ${n("graf", "Grafana", "ext", "obs", ", penwidth=2.2")}
  ${n("alert", "19 luật cảnh báo", "dec", "gate")}
  ${["cad", "host", "exp", "statsd", "probe", "sql"].map((s) => e(s, "alloy")).join(" ")}
  ${e("dlog", "alloy")} ${e("alog", "alloy")}
  ${e("alloy", "prom", `${BOLD}, label="remote_write"`)} ${e("alloy", "loki", `${BOLD}, label="push"`)}
  ${e("prom", "graf")} ${e("loki", "graf")} ${e("graf", "alert")}
  { rank=same; graf; alert; }
}`;

const viz = await instance();
for (const [name, dot] of Object.entries(figs)) {
  writeFileSync(`${out}/${name}.svg`, viz.renderString(dot, { format: "svg", engine: "dot" }));
  console.log(name);
}
