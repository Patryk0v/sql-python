import pandas as pd

# wczytanie danych

df = pd.read_csv("../data/sample_data_dirty.csv")

print("\n=== START: wczytane dane ===")
print(f"Wiersze: {len(df)} | Kolumny: {df.shape[1]}")
print("Braki (przed czyszczeniem):")
print(df.isna().sum())

# obliczenia
#pierwsze wiersze
print(df) 
#.info()
df.info()
#liczbę braków w kazdej kolumnie
df.isna().sum()

#3.Napraw dane:
#category:brakujace uzupelnij wartoscia unknown
df.category =df.category.fillna('Unknown')    
#quantity: zamień na liczbę; błędne wartości (np. three) mają się stać NaN
df.quantity = pd.to_numeric(df.quantity, errors="coerce")
#price
df.price = pd.to_numeric(df.price, errors="coerce")

print("\n=== PO CZYSZCZENIU: typy i braki ===")
print("Typy kolumn:")
print(df.dtypes)
print("Braki (po czyszczeniu):")
print(df.isna().sum())


#4. dodaj kolumne total_value:
df["total_value"] = df["price"] * df["quantity"]

#5.zapis wyniku
df.to_csv("../data/cleaned_all.csv", index=False)
#usuwanie wierszy pustych z 2 kolumn
df_dropna = df.dropna(subset = ['price', 'quantity'])
print("\n=== DROPNA: wersja do obliczeń ===")
print(f"Wiersze przed: {len(df)} | po dropna: {len(df_dropna)}")


df_dropna.to_csv("../data/cleaned_dropna.csv", index=False)
print("\nZapisuję pliki: cleaned_all.csv i cleaned_dropna.csv ...")

#6.Wypisz

len(df.index)
len(df_dropna.index)


summary = df_dropna.groupby("category")["total_value"].sum()
print("\n=== PODSUMOWANIE total_value per category (dropna) ===")
print(summary)


