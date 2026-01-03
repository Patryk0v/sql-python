import pandas as pd

# wczytanie danych
df = pd.read_csv("../data/sample_data.csv")

print("DANE:")
print(df)

# obliczenia
df["total_value"] = df["price"] * df["quantity"]

summary = df.groupby("category")["total_value"].sum()

print("\nPODSUMOWANIE:")
print(summary)

# zapis wyniku
summary.to_csv("../data/summary_by_category.csv")
print("\nZapisano summary_by_category.csv")

