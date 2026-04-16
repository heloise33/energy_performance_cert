import matplotlib.pyplot as plt
import pandas as pd

df = pd.read_csv('/home/onyxia/work/dpe_2021_2024_partial.csv')

print(df.columns)

df["date_etablissement_dpe"] = pd.to_datetime(df["date_etablissement_dpe"])

dpe_counts = df.groupby(["etiquette_dpe"]).size().reset_index(name="count")

print(df["etiquette_dpe"].unique())

colors = ["#008000", "#50C878", "#ADFF2F", "#FFFF00", "#FFA500", "#FF4500", "#FF0000"]

# Group by month and etiquette_dpe
df["month"] = df["date_etablissement_dpe"].dt.to_period("M")
monthly_label = df.groupby(["month", "etiquette_dpe"]).size().unstack(fill_value=0)
monthly_label.index = monthly_label.index.to_timestamp()

perc = monthly_label.div(monthly_label.sum(axis=1), axis=0) * 100
perc = perc[["A", "B", "C", "D", "E", "F", "G"]]

for label in perc.columns:
    min_val = perc[label].min()
    max_val = perc[label].max()
    min_month = perc[label].idxmin()
    max_month = perc[label].idxmax()
    print(f"{label} → min: {min_val:.1f}% ({min_month.strftime('%Y-%m')})  |  max: {max_val:.1f}% ({max_month.strftime('%Y-%m')})")

fig, ax = plt.subplots(figsize=(8, 5))
ax.bar(dpe_counts["etiquette_dpe"], dpe_counts["count"], color=colors)

ax.set_title("DPE certificates by energy label")
ax.set_xlabel("Etiquette DPE")
ax.set_ylabel("Count")
plt.tight_layout()
plt.savefig("dpe_by_label.png", dpi=150)
plt.show()


fig, ax = plt.subplots(figsize=(16, 5))
perc.plot(kind="bar", stacked=True, ax=ax, color=colors, width=0.8)

ax.set_title("DPE certificates issued per month by energy label")
ax.set_xlabel("Month")
ax.set_ylabel("Count")
ax.legend(title="Etiquette DPE", bbox_to_anchor=(1.01, 1), loc="upper left")
plt.xticks(rotation=45, ha="right")
plt.tight_layout()
plt.savefig("dpe_monthly_by_label.png", dpi=150)
plt.show()

fig, ax = plt.subplots(figsize=(16, 5))
monthly_label.plot(kind="bar", stacked=True, ax=ax, color=colors, width=0.8)

ax.set_title("DPE certificates issued per month by energy label")
ax.set_xlabel("Month")
ax.set_ylabel("Count")
ax.legend(title="Etiquette DPE", bbox_to_anchor=(1.01, 1), loc="upper left")
plt.xticks(rotation=45, ha="right")
plt.tight_layout()
plt.savefig("dpe_monthly_by_label.png", dpi=150)
plt.show()

print(df["numero_dpe"].nunique())
print(df.shape[0])
print(df["numero_dpe"].duplicated().sum(), "duplicates")