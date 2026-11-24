// Vẽ Hình 4 (star schema dw) bằng Graphviz (viz-js), ra SVG.
// Chạy: npm i @viz-js/viz@3.11.0 && node fig-4-star-schema.mjs > fig-4-star-schema.svg
// Nội dung bảng/khoá lấy từ docs/model/star-schema.dbml (rút gọn: khoá + vài cột chính).
import { instance } from "@viz-js/viz";

const FACT = "#1f4e79", DIM = "#2e7d32", BRIDGE = "#6d4c41";

// [tên, màu, [[cột, kiểu, đánh dấu]]]  đánh dấu: PK | FK | PK,FK | ""
const tables = [
  ["fact_offer_observation", FACT, [
    ["observation_id", "varchar", "PK"], ["date_key", "int", "FK"], ["listing_key", "bigint", "FK"],
    ["seller_key", "bigint", "FK"], ["brand_key", "bigint", "FK"], ["category_key", "bigint", "FK"],
    ["listing_flags_key", "bigint", "FK"], ["marketplace_key", "bigint", "FK"],
    ["current_price", "decimal", ""], ["list_price", "decimal", ""], ["discount_percent", "decimal", ""],
    ["sold_count", "bigint", ""], ["rating_value", "decimal", ""], ["raw_uri", "text", ""]]],
  ["fact_price_snapshot_daily", FACT, [
    ["date_key", "int", "PK,FK"], ["offer_key", "bigint", "PK"], ["listing_key", "bigint", "FK"],
    ["open_price", "decimal", ""], ["close_price", "decimal", ""], ["min_price", "decimal", ""],
    ["max_price", "decimal", ""], ["observation_count", "int", ""]]],
  ["fact_price_change", FACT, [
    ["observation_id", "varchar", "PK"], ["date_key", "int", "FK"], ["listing_key", "bigint", "FK"],
    ["old_price", "decimal", ""], ["new_price", "decimal", ""], ["pct_change", "decimal", ""],
    ["direction", "varchar", ""], ["is_large_drop", "boolean", ""]]],
  ["fact_counter_daily", FACT, [
    ["date_key", "int", "PK,FK"], ["offer_key", "bigint", "PK"], ["counter_name", "varchar", "PK"],
    ["listing_key", "bigint", "FK"], ["first_value", "bigint", ""], ["last_value", "bigint", ""],
    ["valid_delta_sum", "bigint", ""]]],
  ["fact_category_page_daily", FACT, [
    ["date_key", "int", "PK,FK"], ["category_key", "bigint", "PK,FK"], ["observed_offer_count", "int", ""],
    ["median_price", "decimal", ""], ["discounted_offer_ratio", "decimal", ""], ["in_stock_ratio", "decimal", ""]]],
  ["fact_crawl_attempt", FACT, [
    ["attempt_id", "bigint", "PK"], ["date_key", "int", "FK"], ["marketplace_key", "bigint", "FK"],
    ["category_key", "bigint", "FK"], ["status", "varchar", ""], ["http_status", "int", ""],
    ["latency_ms", "bigint", ""], ["parsed_count", "bigint", ""]]],

  ["dim_date", DIM, [
    ["date_key", "int", "PK"], ["full_date", "date", ""], ["day_of_week", "smallint", ""],
    ["is_weekend", "boolean", ""], ["is_double_day_sale", "boolean", ""]]],
  ["dim_product_listing", DIM, [
    ["listing_key", "bigint", "PK"], ["offer_key", "bigint", ""], ["product_title", "text", ""],
    ["brand_key", "bigint", "FK"], ["category_key", "bigint", "FK"], ["seller_key", "bigint", "FK"],
    ["valid_from", "timestamp", ""], ["valid_to", "timestamp", ""], ["is_current", "boolean", ""]]],
  ["dim_seller", DIM, [
    ["seller_key", "bigint", "PK"], ["seller_name", "varchar", ""], ["is_official_store", "boolean", ""]]],
  ["dim_brand", DIM, [
    ["brand_key", "bigint", "PK"], ["brand_id", "varchar", ""], ["brand_name", "varchar", ""]]],
  ["dim_category", DIM, [
    ["category_key", "bigint", "PK"], ["parent_key", "bigint", "FK"], ["category_name", "varchar", ""],
    ["level", "smallint", ""], ["full_path", "varchar", ""]]],
  ["bridge_category_ancestry", BRIDGE, [
    ["ancestor_key", "bigint", "PK,FK"], ["descendant_key", "bigint", "PK,FK"], ["depth", "smallint", ""]]],
  ["dim_listing_flags", DIM, [
    ["listing_flags_key", "bigint", "PK"], ["availability", "varchar", ""], ["has_discount", "boolean", ""],
    ["is_flash_deal", "boolean", ""]]],
  ["dim_marketplace", DIM, [
    ["marketplace_key", "bigint", "PK"], ["marketplace_code", "varchar", ""], ["default_currency", "char(3)", ""]]],
];

// [từ bảng.cột, tới bảng.cột]
const refs = [
  ["fact_offer_observation.date_key", "dim_date.date_key"],
  ["fact_offer_observation.listing_key", "dim_product_listing.listing_key"],
  ["fact_offer_observation.seller_key", "dim_seller.seller_key"],
  ["fact_offer_observation.brand_key", "dim_brand.brand_key"],
  ["fact_offer_observation.category_key", "dim_category.category_key"],
  ["fact_offer_observation.listing_flags_key", "dim_listing_flags.listing_flags_key"],
  ["fact_offer_observation.marketplace_key", "dim_marketplace.marketplace_key"],
  ["fact_price_snapshot_daily.date_key", "dim_date.date_key"],
  ["fact_price_snapshot_daily.listing_key", "dim_product_listing.listing_key"],
  ["fact_price_change.date_key", "dim_date.date_key"],
  ["fact_price_change.listing_key", "dim_product_listing.listing_key"],
  ["fact_counter_daily.date_key", "dim_date.date_key"],
  ["fact_counter_daily.listing_key", "dim_product_listing.listing_key"],
  ["fact_category_page_daily.date_key", "dim_date.date_key"],
  ["fact_category_page_daily.category_key", "dim_category.category_key"],
  ["fact_crawl_attempt.date_key", "dim_date.date_key"],
  ["fact_crawl_attempt.marketplace_key", "dim_marketplace.marketplace_key"],
  ["fact_crawl_attempt.category_key", "dim_category.category_key"],
  ["dim_product_listing.seller_key", "dim_seller.seller_key"],
  ["dim_product_listing.brand_key", "dim_brand.brand_key"],
  ["dim_product_listing.category_key", "dim_category.category_key"],
  ["bridge_category_ancestry.ancestor_key", "dim_category.category_key"],
  ["bridge_category_ancestry.descendant_key", "dim_category.category_key"],
];

const esc = (s) => s.replace(/&/g, "&amp;").replace(/</g, "&lt;");
function node([name, color, cols]) {
  const rows = cols.map(([c, t, m], i) => {
    const bg = i % 2 ? "#ffffff" : "#f3f6f9";
    const bold = m.includes("PK");
    const label = bold ? `<B>${esc(c)}</B>` : esc(c);
    const mark = m ? `<FONT COLOR="${m.includes("PK") ? "#b8860b" : "#7a8a99"}" POINT-SIZE="7"><B>${m}</B></FONT>` : "";
    return `<TR><TD ALIGN="LEFT" BGCOLOR="${bg}" PORT="${c}_l">${mark}&nbsp;${label}</TD>` +
           `<TD ALIGN="RIGHT" BGCOLOR="${bg}" PORT="${c}_r"><FONT COLOR="#666666"><I>${t}</I></FONT></TD></TR>`;
  }).join("");
  return `"${name}" [label=<<TABLE BORDER="1" CELLBORDER="0" CELLSPACING="0" CELLPADDING="4" COLOR="${color}">` +
    `<TR><TD COLSPAN="2" BGCOLOR="${color}" CELLPADDING="5"><FONT COLOR="white" POINT-SIZE="11"><B>${name}</B></FONT></TD></TR>${rows}</TABLE>>];`;
}

// Thứ tự cột từ trái sang phải: bridge | seller, brand, category | listing | 6 fact | date, flags, marketplace
const rankOf = (t) => t === "legend" ? 0 : t.startsWith("fact_") ? 3
  : t === "bridge_category_ancestry" ? 0
  : ["dim_seller", "dim_brand", "dim_category"].includes(t) ? 1
  : t === "dim_product_listing" ? 2 : 4;

function edge([from, to]) {
  const [ta, ca] = from.split("."), [tb, cb] = to.split(".");   // ta.ca (nhiều) -> tb.cb (một)
  if (rankOf(ta) < rankOf(tb))  // bảng "nhiều" nằm bên trái
    return `"${ta}":"${ca}_r" -> "${tb}":"${cb}_l" [dir=back, arrowtail=crow, arrowhead=none];`;
  return `"${tb}":"${cb}_r" -> "${ta}":"${ca}_l" [dir=forward, arrowhead=crow, arrowtail=none];`;
}


const legend = `legend [label=<<TABLE BORDER="1" COLOR="#b0bec5" CELLBORDER="0" CELLSPACING="2" CELLPADDING="3">
  <TR><TD COLSPAN="2" ALIGN="LEFT"><B>Chú giải</B></TD></TR>
  <TR><TD BGCOLOR="${FACT}" WIDTH="18"> </TD><TD ALIGN="LEFT">Fact (6 bảng)</TD></TR>
  <TR><TD BGCOLOR="${DIM}" WIDTH="18"> </TD><TD ALIGN="LEFT">Dimension (7 bảng)</TD></TR>
  <TR><TD BGCOLOR="${BRIDGE}" WIDTH="18"> </TD><TD ALIGN="LEFT">Bridge (cây danh mục)</TD></TR>
  <TR><TD ALIGN="LEFT"><FONT COLOR="#b8860b"><B>PK</B></FONT></TD><TD ALIGN="LEFT">khoá chính</TD></TR>
  <TR><TD ALIGN="LEFT"><FONT COLOR="#7a8a99"><B>FK</B></FONT></TD><TD ALIGN="LEFT">khoá ngoại; chân gà ở phía "nhiều"</TD></TR>
</TABLE>>];`;

const dot = `digraph dw {
  graph [rankdir=LR, nodesep=0.22, ranksep=0.9, splines=true, fontname="Helvetica", bgcolor="white", pad=0.15];
  node  [shape=plain, fontname="Helvetica", fontsize=10];
  edge  [color="#78909c", penwidth=1.1, arrowsize=0.9];
  ${tables.map(node).join("\n  ")}
  ${legend}
  ${[0, 1, 2, 3, 4].map((r) => `{ rank=same; ${[...tables.map((t) => t[0]), "legend"].filter((t) => rankOf(t) === r).map((t) => `"${t}"`).join("; ")} }`).join("\n  ")}
  "bridge_category_ancestry" -> "legend" [style=invis];
  ${refs.map(edge).join("\n  ")}
}`;

const viz = await instance();
process.stdout.write(viz.renderString(dot, { format: "svg", engine: "dot" }));
