import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

K = ""

raw_lines = []
with open("", "r", encoding="utf-8") as f:
    for line in f:
        clean_line = line.strip()
        if clean_line.startswith("|") and not clean_line.startswith("|--"):
            parts = [p.strip() for p in clean_line.split("|")[1:-1]]
            raw_lines.append(parts)

headers = raw_lines[0]
data_rows = raw_lines[1:]

df = pd.DataFrame(data_rows, columns=headers)

numeric_cols = ['trips', 'total_revenue', 'avg_revenue_per_trip', 'revenue_per_mile', 'revenue_per_minute']
for col in numeric_cols:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')

df = df.drop_duplicates(subset=['pickup_zone', 'distance_bucket']).dropna(subset=['total_revenue'])
df = df[df['total_revenue'] > 0]

df_sorted = df.sort_values(by='total_revenue', ascending=False)
df_clean = df_sorted.head(K).copy()

df_clean['zone_bucket'] = df_clean['pickup_zone'] + "\n(" + df_clean['distance_bucket'] + ")"

sns.set_theme(style="whitegrid")
plt.figure(figsize=(18, 10))

ax = sns.barplot(
    data=df_clean, 
    x='zone_bucket', 
    y='total_revenue', 
    hue='distance_bucket', 
    palette='viridis',
    dodge=False,
    edgecolor='black',
    linewidth=1
)

for p in ax.patches:
    height = p.get_height()
    if height > 0:
        ax.annotate(f'${height:,.0f}', 
                    (p.get_x() + p.get_width() / 2., height), 
                    ha='center', va='bottom', 
                    fontsize=8, fontweight='bold', color='black',
                    xytext=(0, 5), textcoords='offset points')

plt.title('Top Highest Revenue Pickup Zones and Distance Buckets (Q3)', fontsize=16, fontweight='bold', pad=25)
plt.xlabel('Pickup Zone & Distance Category', fontsize=13, fontweight='bold', labelpad=15)
plt.ylabel('Total Revenue ($)', fontsize=13, fontweight='bold', labelpad=15)
plt.xticks(rotation=45, ha='right', fontsize=9)
plt.legend(title='Distance Bucket', title_fontsize='11', fontsize='10', loc='upper right')

ax.get_yaxis().set_major_formatter(plt.FuncFormatter(lambda x, loc: f"${x:,.0f}"))

plt.tight_layout()

output_path = ""
plt.savefig(output_path, dpi=300)

