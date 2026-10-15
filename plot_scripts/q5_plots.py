import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np

flow_data = []

airport_data = []

boroughs = ["Manhattan", "Queens", "Brooklyn", "Bronx", "EWR", "Staten Island"]

matrix = np.zeros((len(boroughs), len(boroughs)))
for pu, do, trips in flow_data:
    if pu in boroughs and do in boroughs:
        i = boroughs.index(pu)
        j = boroughs.index(do)
        matrix[i][j] = trips

fig, axes = plt.subplots(1, 2, figsize=(16, 6))
fig.suptitle("Q5 — Borough Flows & Airport Trips\n(Hours: , Days: , January 2024)",
             fontsize=13, fontweight="bold", y=1.02)

ax1 = axes[0]
log_matrix = np.log1p(matrix)
im = ax1.imshow(log_matrix, cmap="YlOrRd", aspect="auto")

ax1.set_xticks(range(len(boroughs)))
ax1.set_yticks(range(len(boroughs)))
ax1.set_xticklabels(boroughs, rotation=30, ha="right", fontsize=9)
ax1.set_yticklabels(boroughs, fontsize=9)
ax1.set_xlabel("Dropoff Borough", fontsize=10)
ax1.set_ylabel("Pickup Borough", fontsize=10)
ax1.set_title("Borough→Borough Flow Heatmap\n(color: log-scale trips)", fontsize=11, fontweight="bold")

for i in range(len(boroughs)):
    for j in range(len(boroughs)):
        val = int(matrix[i][j])
        if val > 0:
            text_color = "white" if log_matrix[i][j] > log_matrix.max() * 0.6 else "black"
            ax1.text(j, i, f"{val:,}", ha="center", va="center",
                     fontsize=7, color=text_color, fontweight="bold")

plt.colorbar(im, ax=ax1, label="log(trips + 1)", shrink=0.8)


ax2 = axes[1]

labels = [f"{pu[:12]}→\n{do[:12]}" for pu, do, _ in airport_data]
trips  = [t for _, _, t in airport_data]
colors = ["#E53935" if "JFK" in pu or "JFK" in do
          else "#1E88E5" if "LaGuardia" in pu or "LaGuardia" in do
          else "#43A047"
          for pu, do, _ in airport_data]

y_pos = range(len(labels))
bars = ax2.barh(y_pos, trips, color=colors, edgecolor="white", height=0.6)
ax2.set_yticks(y_pos)
ax2.set_yticklabels(labels, fontsize=8)
ax2.invert_yaxis()
ax2.set_xlabel("Number of Trips", fontsize=10)
ax2.set_title("Top-10 Airport Trips (Zone→Zone)", fontsize=11, fontweight="bold")
ax2.xaxis.grid(True, linestyle="--", alpha=0.5)
ax2.set_axisbelow(True)

for bar, val in zip(bars, trips):
    ax2.text(bar.get_width() + 10, bar.get_y() + bar.get_height() / 2,
             f"{val:,}", va="center", fontsize=8, fontweight="bold")

from matplotlib.patches import Patch
legend_elements = [
    Patch(facecolor="#E53935", label="JFK"),
    Patch(facecolor="#1E88E5", label="LaGuardia"),
]
ax2.legend(handles=legend_elements, loc="lower right", fontsize=8)

plt.tight_layout()
plt.savefig("q5_plots.png", dpi=150, bbox_inches="tight")

