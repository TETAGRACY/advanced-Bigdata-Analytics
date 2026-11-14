"""
PART 4 - Static visualisations (PNG, 200 dpi) from the CSV outputs of the pipeline.

  fig1_daily_revenue.png        daily revenue + 7-day moving average, promo week shaded   (Spark SQL3)
  fig2_category_revenue.png     revenue vs gross profit, top-10 categories               (MongoDB A1)
  fig3_funnel_by_device.png     conversion funnel stages by device type                  (Spark SQL1)
  fig4_views_vs_sales.png       most-viewed vs best-selling products                     (Spark SQL4)
  fig5_cohort_heatmap.png       cohort retention heat-map                                (Spark cohort)
  fig6_clv_segments.png         share of customers vs share of 3-year CLV per segment    (Integration)
  fig7_frequency_segments.png   customers vs revenue by purchase-frequency segment       (MongoDB A2)

Colour roles follow one validated palette: categorical slots in fixed order
(blue, orange, aqua), a single-hue blue ramp for magnitude, muted ink for chrome.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from common import FIG_DIR, OUTPUT_DIR  # noqa: E402

import matplotlib  # noqa: E402
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import matplotlib.ticker as mt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from matplotlib.colors import LinearSegmentedColormap  # noqa: E402

# ---- palette (reference instance, light mode) ----
SURFACE, INK, INK2, MUTED = "#fcfcfb", "#0b0b0b", "#52514e", "#898781"
GRID, AXIS = "#e1e0d9", "#c3c2b7"
S1, S2, S3 = "#2a78d6", "#eb6834", "#1baf7a"           # categorical slots 1-3, fixed order
BLUE_RAMP = ["#f4f8fd", "#cde2fb", "#9ec5f4", "#6da7ec", "#3987e5", "#256abf", "#184f95", "#0d366b"]

plt.rcParams.update({
    "figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
    "font.family": "DejaVu Sans", "font.size": 9.5, "text.color": INK,
    "axes.edgecolor": AXIS, "axes.labelcolor": INK2, "axes.titlesize": 12, "axes.titleweight": "bold",
    "axes.titlelocation": "left", "axes.titlepad": 14, "axes.spines.top": False, "axes.spines.right": False,
    "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.6, "axes.axisbelow": True,
    "xtick.color": MUTED, "ytick.color": MUTED, "xtick.labelcolor": INK2, "ytick.labelcolor": INK2,
    "legend.frameon": False, "legend.fontsize": 9,
})


def load(name):
    return pd.read_csv(os.path.join(OUTPUT_DIR, name))


def subtitle(ax, text):
    ax.text(0, 1.015, text, transform=ax.transAxes, fontsize=9, color=INK2, va="bottom")


def save(fig, name):
    os.makedirs(FIG_DIR, exist_ok=True)
    fig.tight_layout()
    fig.savefig(os.path.join(FIG_DIR, name), dpi=200)
    plt.close(fig)
    print("  saved", name)


money = mt.FuncFormatter(lambda v, _: f"${v / 1000:,.0f}k" if abs(v) >= 1000 else f"${v:,.0f}")


# ------------------------------------------------------------------------------------------
def fig1_daily_revenue():
    df = load("spark_SQL3_daily_revenue_moving_avg.csv")
    df["txn_date"] = pd.to_datetime(df.txn_date)
    # keep the 90-day window only (16 Jan - 15 Apr); 16 Apr holds a few orders from sessions
    # that ended after midnight and would show as a false cliff
    df = df[(df.txn_date >= "2025-01-16") & (df.txn_date <= "2025-04-15")]
    fig, ax = plt.subplots(figsize=(9, 3.8))
    promo = df[df.promo_week]
    ax.axvspan(promo.txn_date.min() - pd.Timedelta(hours=12), promo.txn_date.max() + pd.Timedelta(hours=12),
               color=GRID, alpha=0.7, lw=0, zorder=0)
    ax.text(promo.txn_date.min(), df.revenue.max() * 1.02, "Promo week", color=INK2, fontsize=8.5)
    ax.plot(df.txn_date, df.revenue, color=S1, lw=1, alpha=0.45, label="Daily revenue")
    ax.plot(df.txn_date, df.revenue_7d_avg, color=S1, lw=2.2, label="7-day moving average")
    ax.yaxis.set_major_formatter(money)
    ax.set_ylim(0, df.revenue.max() * 1.12)
    ax.grid(axis="x", visible=False)
    ax.legend(loc="lower left", ncol=2)
    uplift = promo.revenue.mean() / df[~df.promo_week].revenue.mean() - 1
    ax.set_title("Daily revenue, Jan–Apr 2025")
    subtitle(ax, f"Promo week averaged {uplift:+.0%} revenue vs. other days")
    save(fig, "fig1_daily_revenue.png")


def fig2_category_revenue():
    df = load("mongo_A1_revenue_by_category_month.csv")
    g = (df.groupby("category_name")[["revenue", "gross_profit"]].sum()
         .sort_values("revenue", ascending=True).tail(10))
    fig, ax = plt.subplots(figsize=(9, 4.4))
    y = np.arange(len(g))
    h = 0.38
    ax.barh(y + h / 2 + 0.01, g.revenue, height=h, color=S1, label="Revenue")
    ax.barh(y - h / 2 - 0.01, g.gross_profit, height=h, color=S2, label="Gross profit")
    for yi, (r, p) in enumerate(zip(g.revenue, g.gross_profit)):
        ax.text(r, yi + h / 2, f"  ${r / 1000:,.0f}k", va="center", fontsize=8, color=INK2)
        ax.text(p, yi - h / 2, f"  {100 * p / r:.0f}% margin", va="center", fontsize=7.5, color=MUTED)
    ax.set_yticks(y, g.index)
    ax.xaxis.set_major_formatter(money)
    ax.grid(axis="y", visible=False)
    ax.set_xlim(0, g.revenue.max() * 1.15)
    ax.legend(loc="lower right")
    top_share = g.revenue.iloc[-3:].sum() / df.revenue.sum()
    ax.set_title("Revenue and gross profit — top 10 categories")
    subtitle(ax, f"Top 3 categories = {top_share:.0%} of all revenue (MongoDB pipeline A1)")
    save(fig, "fig2_category_revenue.png")


def fig3_funnel():
    df = load("spark_SQL1_funnel_by_device_referrer.csv")
    stages = ["sessions", "viewed", "cart", "converted"]
    rows = []
    for dev, g in df.groupby("device_type"):
        n = g.sessions.sum()
        rows.append([dev, 100.0,
                     (g.pct_viewed_product * g.sessions).sum() / n,
                     (g.pct_added_to_cart * g.sessions).sum() / n,
                     (g.pct_converted * g.sessions).sum() / n])
    f = pd.DataFrame(rows, columns=["device"] + stages).set_index("device").loc[["desktop", "tablet", "mobile"]]
    fig, ax = plt.subplots(figsize=(9, 3.9))
    x = np.arange(len(stages))
    w = 0.26
    for i, (dev, col) in enumerate(zip(f.index, [S1, S2, S3])):
        bars = ax.bar(x + (i - 1) * (w + 0.015), f.loc[dev], width=w, color=col, label=dev.capitalize())
        for b, v in zip(bars[2:], f.loc[dev].values[2:]):
            ax.text(b.get_x() + b.get_width() / 2, v + 1.5, f"{v:.1f}%", ha="center", fontsize=8, color=INK2)
    ax.set_xticks(x, ["All sessions", "Viewed a product", "Added to cart", "Purchased"])
    ax.yaxis.set_major_formatter(mt.PercentFormatter())
    ax.set_ylim(0, 110)
    ax.grid(axis="x", visible=False)
    ax.legend(loc="upper right", ncol=3)
    ax.set_title("Conversion funnel by device type")
    subtitle(ax, f"Desktop converts {f.loc['desktop', 'converted'] / f.loc['mobile', 'converted']:.1f}x "
                 f"better than mobile (share of sessions reaching each stage)")
    save(fig, "fig3_funnel_by_device.png")


def fig4_views_vs_sales():
    df = load("spark_SQL4_view_vs_purchase_per_product.csv")
    df["is_active"] = df.is_active.astype(str).str.lower() == "true"
    act, ina = df[df.is_active], df[~df.is_active]
    fig, ax = plt.subplots(figsize=(9, 4.3))
    ax.scatter(act.views, act.units_sold, s=11, color=S1, alpha=0.4, lw=0, label="Active product")
    ax.scatter(ina.views, ina.units_sold, s=16, color=S2, alpha=0.9, edgecolor=SURFACE, lw=0.6, zorder=3,
               label="Inactive / unbuyable product")
    for _, r in pd.concat([act.nlargest(3, "units_sold"), ina.nlargest(2, "views")]).iterrows():
        ax.annotate(r.product_id, (r.views, r.units_sold), xytext=(6, 3), textcoords="offset points",
                    fontsize=7.5, color=INK2)
    corr = act[["views", "units_sold"]].corr().iloc[0, 1]
    ax.set_xlabel("Product-detail page views")
    ax.set_ylabel("Units sold")
    ax.legend(loc="upper left")
    overlap = len(set(df.nlargest(20, "views").product_id) & set(df.nlargest(20, "units_sold").product_id))
    ax.set_title("Most-viewed vs. best-selling products")
    subtitle(ax, f"Active products: views predict sales (r = {corr:.2f}); {overlap} of the top-20 viewed are "
                 f"top-20 sellers. Inactive products sit on the floor.")
    save(fig, "fig4_views_vs_sales.png")


def fig5_cohort():
    df = pd.read_csv(os.path.join(OUTPUT_DIR, "spark_cohort_retention.csv"), index_col=0)
    df = df[[c for c in df.columns if c.startswith("20")]]          # month columns only
    df.columns = [c + ("*" if c in ("2025-01", "2025-04") else "") for c in df.columns]
    fig, ax = plt.subplots(figsize=(9, 4.6))
    cmap = LinearSegmentedColormap.from_list("blue", BLUE_RAMP)
    im = ax.imshow(df.values, cmap=cmap, aspect="auto")
    ax.set_xticks(range(len(df.columns)), df.columns)
    ax.set_yticks(range(len(df.index)), df.index)
    ax.grid(False)
    for s in ax.spines.values():
        s.set_visible(False)
    vmax = df.values.max()
    for i in range(df.shape[0]):
        for j in range(df.shape[1]):
            v = df.values[i, j]
            if v > 0:
                ax.text(j, i, f"{v:.0f}%", ha="center", va="center", fontsize=7.5,
                        color="#ffffff" if v > vmax * 0.55 else INK)
    ax.set_xlabel("Purchase month   (* half month: data window is 16 Jan - 15 Apr 2025)")
    ax.set_ylabel("Registration cohort")
    cb = fig.colorbar(im, ax=ax, fraction=0.03, pad=0.02)
    cb.outline.set_visible(False)
    cb.ax.tick_params(colors=MUTED, labelsize=8)
    cb.set_label("% of cohort purchasing", color=INK2, fontsize=8.5)
    ax.set_title("Cohort analysis — share of each registration cohort buying per month")
    subtitle(ax, "Older cohorts behave alike (~50% buy in March); new sign-ups buy hardest in their first month")
    save(fig, "fig5_cohort_heatmap.png")


def fig6_clv():
    df = load("integration_clv_segments.csv")
    df["user_share"] = 100 * df.users / df.users.sum()
    df = df.sort_values("avg_clv_3y", ascending=True)        # highest-value segment on top
    fig, ax = plt.subplots(figsize=(9, 3.6))
    y = np.arange(len(df))
    h = 0.38
    ax.barh(y + h / 2 + 0.01, df.user_share, height=h, color=S1, label="Share of customers")
    ax.barh(y - h / 2 - 0.01, df.clv_share_pct, height=h, color=S2, label="Share of 3-year CLV")
    for yi, (u, c, a) in enumerate(zip(df.user_share, df.clv_share_pct, df.avg_clv_3y)):
        ax.text(u, yi + h / 2, f"  {u:.1f}%" if u < 1 else f"  {u:.0f}%", va="center", fontsize=8, color=INK2)
        ax.text(c, yi - h / 2, f"  {c:.0f}%  (avg ${a:,.0f})", va="center", fontsize=8, color=INK2)
    ax.set_yticks(y, df.segment)
    ax.xaxis.set_major_formatter(mt.PercentFormatter())
    ax.set_xlim(0, max(df.user_share.max(), df.clv_share_pct.max()) * 1.3)
    ax.grid(axis="y", visible=False)
    ax.legend(loc="lower right")
    champ = df[df.segment == "Champions"].iloc[0]
    ax.set_title("Customer value segments (K-Means on RFM + HBase engagement)")
    subtitle(ax, f"Champions: {champ.user_share:.1f}% of customers hold {champ.clv_share_pct:.0f}% of projected value")
    save(fig, "fig6_clv_segments.png")


def fig7_frequency():
    df = load("mongo_A2_customer_segments.csv")
    df["user_pct"] = 100 * df.users / df.users.sum()
    labels = [s.split(" - ")[1] for s in df.segment]
    fig, ax = plt.subplots(figsize=(9, 3.6))
    x = np.arange(len(df))
    w = 0.38
    ax.bar(x - w / 2 - 0.01, df.user_pct, width=w, color=S1, label="% of customers")
    ax.bar(x + w / 2 + 0.01, df.revenue_share_pct, width=w, color=S2, label="% of revenue")
    for xi, (u, r) in enumerate(zip(df.user_pct, df.revenue_share_pct)):
        ax.text(xi - w / 2, u + 1, f"{u:.0f}%", ha="center", fontsize=8, color=INK2)
        ax.text(xi + w / 2, r + 1, f"{r:.0f}%", ha="center", fontsize=8, color=INK2)
    ax.set_xticks(x, labels)
    ax.yaxis.set_major_formatter(mt.PercentFormatter())
    ax.set_ylim(0, max(df.user_pct.max(), df.revenue_share_pct.max()) * 1.18)
    ax.grid(axis="x", visible=False)
    ax.legend(loc="upper left")
    loyal = df.iloc[-1]
    ax.set_title("Customers and revenue by purchase frequency")
    subtitle(ax, f"Loyal customers (7+ orders) are {loyal.user_pct:.0f}% of the base but "
                 f"{loyal.revenue_share_pct:.0f}% of revenue (MongoDB pipeline A2)")
    save(fig, "fig7_frequency_segments.png")


def main():
    for f in [fig1_daily_revenue, fig2_category_revenue, fig3_funnel, fig4_views_vs_sales,
              fig5_cohort, fig6_clv, fig7_frequency]:
        try:
            f()
        except FileNotFoundError as e:
            print(f"  skipped {f.__name__}: missing {e.filename}")


if __name__ == "__main__":
    main()
