import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

hours           = ""
card_share      = []
avg_tip_rate    = []
avg_total_card  = []
avg_total_cash  = []

x      = np.arange(len(hours))
labels = [f"{h}:00" for h in hours]

fig, axes = plt.subplots(1, 2, figsize=(13, 5))
fig.suptitle("Q4 — Payment Method & Tips by Hour (2024)", fontsize=14, fontweight="bold", y=1.02)

ax1 = axes[0]
bars = ax1.bar(x, [v * 100 for v in card_share], color="#2196F3", edgecolor="white", width=0.5)
ax1.set_xticks(x)
ax1.set_xticklabels(labels, fontsize=11)
ax1.set_ylabel("Card Share (%)", fontsize=11)
ax1.set_title("Card Share by Hour", fontsize=12, fontweight="bold")
ax1.set_ylim(70, 80)
ax1.yaxis.grid(True, linestyle="--", alpha=0.6)
ax1.set_axisbelow(True)
for bar, val in zip(bars, card_share):
    ax1.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.1,
             f"{val*100:.2f}%", ha="center", va="bottom", fontsize=10, fontweight="bold")

ax2 = axes[1]
bars2 = ax2.bar(x, [v * 100 for v in avg_tip_rate], color="#4CAF50", edgecolor="white", width=0.5)
ax2.set_xticks(x)
ax2.set_xticklabels(labels, fontsize=11)
ax2.set_ylabel("Avg Tip Rate (% of fare)", fontsize=11)
ax2.set_title("Average Card Tip Rate by Hour\n(recorded tips only — payment_type=1)", fontsize=11, fontweight="bold")
ax2.set_ylim(0, 50)
ax2.yaxis.grid(True, linestyle="--", alpha=0.6)
ax2.set_axisbelow(True)
for bar, val in zip(bars2, avg_tip_rate):
    ax2.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.3,
             f"{val*100:.2f}%", ha="center", va="bottom", fontsize=10, fontweight="bold")

note = (
    "Note: Cash tips (payment_type=2) are not recorded in the system.\n"
    "The avg_tip_rate applies only to electronic payments and does not reflect\n"
    "actual tipping behavior for cash payments."
)
fig.text(0.5, -0.06, note, ha="center", fontsize=9, color="#555555",
         bbox=dict(boxstyle="round,pad=0.4", facecolor="#FFF9C4", edgecolor="#CCCCCC"))

plt.tight_layout()
output_path=""
plt.savefig(output_path, dpi=150, bbox_inches="tight")

