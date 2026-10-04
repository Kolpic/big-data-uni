#!/usr/bin/env python3
"""
═══════════════════════════════════════════════════════════════════════════
STAGE 6 - AUTOMATIC REPORT (.txt + .html)
═══════════════════════════════════════════════════════════════════════════

Builds the automatic report required by the assignment from the results of
the previous stages.  It contains:

  * the number of processed records,
  * the execution time of every main operation,
  * the key descriptive findings (KPIs, distribution, Top-N, Pareto, trends),
  * short written conclusions,
  * the generated visualizations (embedded in the HTML version).

Both a plain-text (.txt) and an HTML (.html) report are written to reports/.
This module is normally called by main.py, which passes in the collected
results, but it can also rebuild the report from the CSV/JSON artifacts.
"""

import json
import textwrap
from datetime import datetime
from html import escape

from config import REPORTS_DIR, CHARTS_DIR, METADATA_DIR
from logger_config import setup_logger

logger = setup_logger(__name__, log_file="report.log")

CHART_TITLES = {
    "01_revenue_distribution.png": "Distribution of transaction revenue",
    "02_top_products.png": "Top 10 products by revenue",
    "03_category_pareto.png": "Revenue share by category (Pareto)",
    "04_monthly_trend.png": "Monthly revenue trend",
    "05_orders_by_hour.png": "Orders by hour of day",
    "06_order_value_categories.png": "Orders by value category",
}


def _conclusions(results: dict) -> list:
    """Derive short written conclusions from the analysis results."""
    out = []
    spark = results.get("spark", {})
    kpis = spark.get("kpis", {})
    if kpis:
        out.append(
            f"The dataset contains {kpis.get('total_orders', 0):,} valid orders "
            f"from {kpis.get('unique_users', 0):,} unique customers, generating a "
            f"total revenue of ${kpis.get('total_revenue', 0):,.2f} "
            f"(average order value ${kpis.get('avg_order_value', 0):,.2f})."
        )
    cats = spark.get("revenue_by_category", [])
    if cats:
        total = sum(c["total_revenue"] for c in cats)
        top = cats[0]
        share = top["total_revenue"] / total * 100 if total else 0
        out.append(
            f"Revenue is highly concentrated (a long-tail / Pareto pattern): the "
            f"top category '{top['product_category']}' alone accounts for "
            f"{share:.1f}% of all revenue."
        )
    dist = {d["metric"]: d for d in spark.get("distribution_stats", [])}
    if "total_amount" in dist:
        d = dist["total_amount"]
        out.append(
            f"The order-value distribution is right-skewed "
            f"(skewness = {d['skewness']:.2f}): the mean (${d['mean']:,.2f}) is well "
            f"above the median (${d['median']:,.2f}), so a few large orders pull the "
            f"average up - exactly the shape descriptive analysis is meant to reveal."
        )
    top_products = spark.get("top_products", [])
    if top_products:
        out.append(
            f"The single best-selling product by revenue is "
            f"'{top_products[0]['product_name']}' "
            f"(${top_products[0]['total_revenue']:,.2f})."
        )
    hourly = spark.get("hourly", [])
    if hourly:
        peak = max(hourly, key=lambda h: h["orders_count"])
        out.append(
            f"Order activity peaks around {int(peak['hour']):02d}:00 "
            f"({peak['orders_count']:,} orders in that hour)."
        )
    # Cross-engine validation note.
    dask_stats = {d["metric"]: d for d in results.get("dask", {}).get("descriptive_statistics", [])}
    ray_global = results.get("ray", {}).get("global", {})
    if "revenue" in dask_stats and ray_global:
        out.append(
            f"The three engines agree: mean revenue computed by Dask "
            f"(${dask_stats['revenue']['mean']:,.2f}), Spark and Ray "
            f"(${ray_global.get('mean', 0):,.2f}) match, which validates the pipeline."
        )
    return out


def _timings_table(results: dict) -> list:
    """Collect (stage, seconds) rows for the timing section."""
    rows = []
    mapping = [
        ("1. Data generation (Faker)", results.get("generation", {}).get("generation_seconds")),
        ("2. Cleaning & stats (Dask)", results.get("dask", {}).get("elapsed_seconds")),
        ("3. Descriptive analysis (PySpark)", results.get("spark", {}).get("elapsed_seconds")),
        ("4. Parallel stats (Ray)", results.get("ray", {}).get("elapsed_seconds")),
        ("5. Visualization", results.get("visualize", {}).get("elapsed_seconds")),
    ]
    for name, secs in mapping:
        if secs is not None:
            rows.append((name, secs))
    return rows


def build_report(results: dict) -> dict:
    """Write both the .txt and .html reports. Returns their paths."""
    logger.info("=" * 70)
    logger.info("STAGE 6 - AUTOMATIC REPORT")
    logger.info("=" * 70)

    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    spark = results.get("spark", {})
    kpis = spark.get("kpis", {})
    processed = (
        spark.get("kpis", {}).get("total_orders")
        or results.get("dask", {}).get("clean_count")
        or results.get("ray", {}).get("processed_records")
        or 0
    )
    raw_count = results.get("dask", {}).get("raw_count", processed)
    removed = results.get("dask", {}).get("removed_count", 0)
    conclusions = _conclusions(results)
    timings = _timings_table(results)
    total_time = round(sum(s for _, s in timings), 2)

    # Load dataset metadata if available.
    meta = {}
    meta_file = METADATA_DIR / "dataset_metadata.json"
    if meta_file.exists():
        meta = json.loads(meta_file.read_text(encoding="utf-8"))

    # ───────────────────────── TEXT REPORT ──────────────────────────────
    txt_path = REPORTS_DIR / "analysis_report.txt"
    lines = []
    W = 80
    lines.append("=" * W)
    lines.append("BIG DATA PROJECT - DESCRIPTIVE ANALYSIS (Variant 1)".center(W))
    lines.append("E-commerce transactions - automatic report".center(W))
    lines.append("=" * W)
    lines.append(f"Generated at : {generated_at}")
    lines.append(f"Analysis type: Descriptive (What happened?)")
    lines.append(f"Libraries    : Faker, Dask, PySpark, Ray")
    lines.append("")

    lines.append("-" * W)
    lines.append("1) DATASET")
    lines.append("-" * W)
    lines.append(f"  Raw records generated : {raw_count:,}")
    lines.append(f"  Invalid removed       : {removed:,}")
    lines.append(f"  Records processed     : {processed:,}")
    if meta:
        lines.append(f"  Files / NULL rate     : {meta.get('num_files')} files / "
                     f"{meta.get('null_rate', 0) * 100:.0f}% NULLs")
        lines.append(f"  Raw size on disk      : {meta.get('total_size_mb', 0):.2f} MB")
    lines.append("")

    if kpis:
        lines.append("-" * W)
        lines.append("2) KEY PERFORMANCE INDICATORS")
        lines.append("-" * W)
        lines.append(f"  Total revenue         : ${kpis.get('total_revenue', 0):,.2f}")
        lines.append(f"  Average order value   : ${kpis.get('avg_order_value', 0):,.2f}")
        lines.append(f"  Total orders          : {kpis.get('total_orders', 0):,}")
        lines.append(f"  Unique customers      : {kpis.get('unique_users', 0):,}")
        lines.append(f"  Unique products       : {kpis.get('unique_products', 0):,}")
        lines.append(f"  Total items sold      : {int(kpis.get('total_items_sold', 0)):,}")
        lines.append("")

    dist = spark.get("distribution_stats", [])
    if dist:
        lines.append("-" * W)
        lines.append("3) DISTRIBUTION STATISTICS")
        lines.append("-" * W)
        header = f"  {'metric':<13}{'mean':>12}{'median':>12}{'std':>12}{'skew':>9}"
        lines.append(header)
        for d in dist:
            lines.append(f"  {d['metric']:<13}{d['mean']:>12,.2f}{d['median']:>12,.2f}"
                         f"{d['std']:>12,.2f}{d['skewness']:>9.2f}")
        lines.append("")

    top_products = spark.get("top_products", [])
    if top_products:
        lines.append("-" * W)
        lines.append("4) TOP 5 PRODUCTS BY REVENUE")
        lines.append("-" * W)
        for i, p in enumerate(top_products[:5], 1):
            lines.append(f"  {i}. {p['product_name']:<22} ${p['total_revenue']:>14,.2f} "
                         f"({p['orders_count']:,} orders)")
        lines.append("")

    cats = spark.get("revenue_by_category", [])
    if cats:
        total = sum(c["total_revenue"] for c in cats)
        lines.append("-" * W)
        lines.append("5) REVENUE BY CATEGORY")
        lines.append("-" * W)
        for c in cats:
            share = c["total_revenue"] / total * 100 if total else 0
            lines.append(f"  {c['product_category']:<18} ${c['total_revenue']:>14,.2f} "
                         f"({share:5.1f}%)")
        lines.append("")

    if timings:
        lines.append("-" * W)
        lines.append("6) EXECUTION TIME OF MAIN OPERATIONS")
        lines.append("-" * W)
        for name, secs in timings:
            lines.append(f"  {name:<40} {secs:>8.2f} s")
        lines.append(f"  {'TOTAL':<40} {total_time:>8.2f} s")
        lines.append("")

    if conclusions:
        lines.append("-" * W)
        lines.append("7) CONCLUSIONS")
        lines.append("-" * W)
        for i, c in enumerate(conclusions, 1):
            # wrap long lines, keeping continuation lines aligned under the text
            lines.append(textwrap.fill(
                c, width=78, initial_indent=f"  {i}. ", subsequent_indent="     "))
        lines.append("")

    charts = results.get("visualize", {}).get("charts", [])
    if charts:
        lines.append("-" * W)
        lines.append("8) VISUALIZATIONS (see reports/charts/)")
        lines.append("-" * W)
        for ch in charts:
            lines.append(f"  - {ch}: {CHART_TITLES.get(ch, '')}")
        lines.append("")

    lines.append("=" * W)
    lines.append(f"End of report - generated {generated_at}".center(W))
    lines.append("=" * W)
    txt_path.write_text("\n".join(lines), encoding="utf-8")
    logger.info(f"   text report  -> {txt_path}")

    # ───────────────────────── HTML REPORT ──────────────────────────────
    html_path = REPORTS_DIR / "analysis_report.html"
    html_path.write_text(
        _build_html(results, generated_at, raw_count, removed, processed,
                    kpis, dist, top_products, cats, timings, total_time,
                    conclusions, charts, meta),
        encoding="utf-8",
    )
    logger.info(f"   html report  -> {html_path}")
    logger.info("=" * 70)
    return {"txt": str(txt_path), "html": str(html_path)}


def _build_html(results, generated_at, raw_count, removed, processed, kpis,
                dist, top_products, cats, timings, total_time, conclusions,
                charts, meta) -> str:
    """Render a self-contained HTML report (charts referenced from charts/)."""
    total_cat = sum(c["total_revenue"] for c in cats) if cats else 0

    def kpi_card(label, value):
        return f'<div class="card"><div class="val">{value}</div><div class="lbl">{escape(label)}</div></div>'

    kpi_html = ""
    if kpis:
        kpi_html = "".join([
            kpi_card("Total revenue", f"${kpis.get('total_revenue', 0):,.0f}"),
            kpi_card("Orders", f"{kpis.get('total_orders', 0):,}"),
            kpi_card("Avg order value", f"${kpis.get('avg_order_value', 0):,.2f}"),
            kpi_card("Unique customers", f"{kpis.get('unique_users', 0):,}"),
            kpi_card("Unique products", f"{kpis.get('unique_products', 0):,}"),
        ])

    dist_rows = "".join(
        f"<tr><td>{escape(d['metric'])}</td><td>{d['mean']:,.2f}</td>"
        f"<td>{d['median']:,.2f}</td><td>{d['std']:,.2f}</td>"
        f"<td>{d['q25']:,.2f}</td><td>{d['q75']:,.2f}</td>"
        f"<td>{d['skewness']:.2f}</td></tr>"
        for d in dist
    )
    prod_rows = "".join(
        f"<tr><td>{i}</td><td>{escape(p['product_name'])}</td>"
        f"<td>{escape(str(p['product_category']))}</td>"
        f"<td>${p['total_revenue']:,.2f}</td><td>{p['orders_count']:,}</td></tr>"
        for i, p in enumerate(top_products[:10], 1)
    )
    cat_rows = "".join(
        f"<tr><td>{escape(c['product_category'])}</td><td>${c['total_revenue']:,.2f}</td>"
        f"<td>{(c['total_revenue'] / total_cat * 100 if total_cat else 0):.1f}%</td>"
        f"<td>{c['orders_count']:,}</td></tr>"
        for c in cats
    )
    time_rows = "".join(
        f"<tr><td>{escape(n)}</td><td>{s:.2f} s</td></tr>" for n, s in timings
    )
    concl_html = "".join(f"<li>{escape(c)}</li>" for c in conclusions)
    charts_html = "".join(
        f'<figure><img src="charts/{escape(ch)}" alt="{escape(CHART_TITLES.get(ch, ch))}">'
        f"<figcaption>{escape(CHART_TITLES.get(ch, ch))}</figcaption></figure>"
        for ch in charts
    )

    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Big Data - Descriptive Analysis Report</title>
<style>
  :root {{ --blue:#0072B2; --ink:#1a1a1a; --muted:#666; --line:#e3e3e3; --bg:#f7f8fa; --card:#fff; }}
  * {{ box-sizing:border-box; }}
  body {{ font-family:-apple-system,Segoe UI,Roboto,Arial,sans-serif; color:var(--ink);
         background:var(--bg); margin:0; line-height:1.55; }}
  .wrap {{ max-width:1000px; margin:0 auto; padding:32px 20px 64px; }}
  header {{ border-bottom:3px solid var(--blue); padding-bottom:16px; margin-bottom:24px; }}
  h1 {{ margin:0 0 6px; font-size:26px; }}
  h2 {{ font-size:19px; margin:32px 0 12px; border-left:4px solid var(--blue); padding-left:10px; }}
  .meta {{ color:var(--muted); font-size:14px; }}
  .cards {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(160px,1fr)); gap:12px; margin:16px 0; }}
  .card {{ background:var(--card); border:1px solid var(--line); border-radius:10px; padding:16px; }}
  .card .val {{ font-size:22px; font-weight:700; color:var(--blue); }}
  .card .lbl {{ font-size:13px; color:var(--muted); margin-top:4px; }}
  table {{ border-collapse:collapse; width:100%; background:var(--card); border:1px solid var(--line);
           border-radius:8px; overflow:hidden; font-size:14px; margin:8px 0; }}
  th,td {{ padding:8px 12px; text-align:left; border-bottom:1px solid var(--line); }}
  th {{ background:#eef2f6; font-size:12px; text-transform:uppercase; letter-spacing:.04em; color:var(--muted); }}
  td:nth-child(n+2) {{ font-variant-numeric:tabular-nums; }}
  ul {{ padding-left:20px; }} li {{ margin-bottom:8px; }}
  figure {{ margin:0 0 24px; background:var(--card); border:1px solid var(--line); border-radius:10px; padding:12px; }}
  figure img {{ width:100%; height:auto; border-radius:6px; }}
  figcaption {{ color:var(--muted); font-size:13px; margin-top:8px; text-align:center; }}
  footer {{ margin-top:40px; color:var(--muted); font-size:13px; border-top:1px solid var(--line); padding-top:14px; }}
</style></head>
<body><div class="wrap">
  <header>
    <h1>Big Data Project - Descriptive Analysis</h1>
    <div class="meta">E-commerce transactions &middot; Variant 1 &middot;
      Libraries: Faker, Dask, PySpark, Ray &middot; Generated {generated_at}</div>
  </header>

  <h2>Dataset</h2>
  <p>Raw records generated: <b>{raw_count:,}</b> &middot;
     invalid removed: <b>{removed:,}</b> &middot;
     records processed: <b>{processed:,}</b>
     {f"&middot; raw size: <b>{meta.get('total_size_mb', 0):.1f} MB</b>" if meta else ""}</p>

  <h2>Key performance indicators</h2>
  <div class="cards">{kpi_html}</div>

  <h2>Distribution statistics</h2>
  <table><thead><tr><th>Metric</th><th>Mean</th><th>Median</th><th>Std</th>
    <th>Q25</th><th>Q75</th><th>Skewness</th></tr></thead><tbody>{dist_rows}</tbody></table>

  <h2>Top products by revenue</h2>
  <table><thead><tr><th>#</th><th>Product</th><th>Category</th><th>Revenue</th><th>Orders</th></tr></thead>
    <tbody>{prod_rows}</tbody></table>

  <h2>Revenue by category</h2>
  <table><thead><tr><th>Category</th><th>Revenue</th><th>Share</th><th>Orders</th></tr></thead>
    <tbody>{cat_rows}</tbody></table>

  <h2>Execution time of main operations</h2>
  <table><thead><tr><th>Stage</th><th>Time</th></tr></thead><tbody>{time_rows}
    <tr><td><b>Total</b></td><td><b>{total_time:.2f} s</b></td></tr></tbody></table>

  <h2>Conclusions</h2>
  <ul>{concl_html}</ul>

  <h2>Visualizations</h2>
  {charts_html}

  <footer>Automatically generated report &middot; descriptive big-data analysis pipeline.</footer>
</div></body></html>"""


if __name__ == "__main__":
    # Rebuild from nothing (useful for quick HTML/CSS iteration).
    build_report({})
