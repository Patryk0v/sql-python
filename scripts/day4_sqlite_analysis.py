# -*- coding: utf-8 -*-
"""
Day 4 – SQLite + SQL analysis from Python

Cel:
1) Wczytać cleaned_dropna.csv do DataFrame
2) Zapisać DataFrame do bazy SQLite (tabela: sales)
3) Wykonać 3 zapytania SQL i zobaczyć wyniki
4) Zapisać wynik końcowy do CSV (data/sql_summary.csv)
"""

import sqlite3
import pandas as pd


# === 0) ŚCIEŻKI (trzymamy w jednym miejscu, żeby nie było chaosu) ===
CSV_PATH = r"C:\Users\PC\Desktop\sql-python-main\data\cleaned_dropna.csv"
DB_PATH = r"C:\Users\PC\Desktop\sql-python-main\data\sales.db"
OUT_SUMMARY_PATH = r"C:\Users\PC\Desktop\sql-python-main\data\sql_summary.csv"


# === 1) Wczytanie danych z CSV do Pandas ===
df = pd.read_csv(CSV_PATH)

print("\n=== Wczytano cleaned_dropna.csv ===")
print(df.head())
print("\nTypy kolumn:")
print(df.dtypes)
print("\nLiczba wierszy:", len(df))


# === 2) Połączenie z bazą SQLite ===
# sqlite3.connect() tworzy plik bazy, jeśli go nie ma.
# 'conn' to połączenie do bazy - przez nie wysyłasz zapytania SQL.
conn = sqlite3.connect(DB_PATH)

# Dobra praktyka: użyj try/finally, żeby zawsze zamknąć połączenie,
# nawet jak coś się wysypie.
try:
    # === 3) Zapis danych do tabeli w bazie ===
    # to_sql() tworzy tabelę 'sales' i wstawia dane z df.
    # if_exists='replace' -> kasuje starą tabelę i tworzy od nowa
    # index=False -> nie zapisuje indeksu pandas jako dodatkowej kolumny
    df.to_sql("sales", conn, if_exists="replace", index=False)

    print("\n=== Zapisano dane do SQLite (tabela: sales) ===")

    # === 4) CURSOR – co to jest? ===
    # Cursor to "wskaźnik" / obiekt, który pozwala wykonywać polecenia SQL
    # i odbierać wyniki w trybie niskopoziomowym (execute/fetch).
    # W naszym stylu analitycznym częściej użyjemy pd.read_sql_query,
    # bo daje wyniki od razu jako DataFrame.
    cursor = conn.cursor()

    # === 5) QUERY 1: Podgląd danych (LIMIT) ===
    sql_preview = """
    SELECT *
    FROM sales
    LIMIT 5;
    """
    df_preview = pd.read_sql_query(sql_preview, conn)
    print("\n=== QUERY 1: Podgląd danych (LIMIT 5) ===")
    print(df_preview)

    # === 6) QUERY 2: Suma total_value per category (sort malejąco) ===
    sql_by_category = """
    SELECT
        category,
        SUM(total_value) AS total_value_sum
    FROM sales
    GROUP BY category
    ORDER BY total_value_sum DESC;
    """
    df_by_category = pd.read_sql_query(sql_by_category, conn)
    print("\n=== QUERY 2: Suma total_value per category (DESC) ===")
    print(df_by_category)

    # === 7) Funkcja: filtr po category i progu total_value ===
    # Nie składamy SQL f-stringiem z wartościami (zły nawyk).
    # Używamy parametrów (?) – bezpieczniej i czytelniej.
    def get_sales_summary(min_total_value: float = 30.0, category: str | None = None) -> pd.DataFrame:
        """
        Zwraca podsumowanie total_value per category z warunkiem HAVING.
        Jeśli podasz category='A', to zwróci tylko kategorię A.

        min_total_value: próg na sumę total_value (np. 30)
        category: jeśli None -> wszystkie kategorie, jeśli np. 'A' -> tylko A
        """
        base_sql = """
        SELECT
            category,
            SUM(total_value) AS total_value_sum
        FROM sales
        GROUP BY category
        HAVING SUM(total_value) >= ?
        """

        params = [min_total_value]

        # Jeśli chcesz tylko jedną kategorię, dopisz warunek.
        # Uwaga: filtr po category najlepiej zrobić w WHERE przed GROUP BY,
        # bo jest logicznie prostszy i szybszy.
        if category is not None:
            base_sql = """
            SELECT
                category,
                SUM(total_value) AS total_value_sum
            FROM sales
            WHERE category = ?
            GROUP BY category
            HAVING SUM(total_value) >= ?
            """
            params = [category, min_total_value]

        return pd.read_sql_query(base_sql, conn, params=params)

    # === 8) QUERY 3: warunek suma(total_value) > 30 (wszystkie kategorie) ===
    df_summary = get_sales_summary(min_total_value=30.0)
    print("\n=== QUERY 3: Kategorie z SUM(total_value) >= 30 ===")
    print(df_summary)

    # Przykład: tylko kategoria A (jeśli istnieje)
    df_summary_A = get_sales_summary(min_total_value=30.0, category="A")
    print("\n=== Dodatkowo: tylko category='A' i próg 30 ===")
    print(df_summary_A)

    # === 9) Zapis wyniku (zgodnie z wymaganiem dnia 4) ===
    df_summary.to_csv(OUT_SUMMARY_PATH, index=False)
    print(f"\nZapisano wynik do: {OUT_SUMMARY_PATH}")

finally:
    # === 10) Zamykanie połączenia ===
    conn.close()
    print("\nPołączenie z bazą zamknięte.")
