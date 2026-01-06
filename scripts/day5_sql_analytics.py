# -*- coding: utf-8 -*-


import sqlite3
import pandas as pd


# === 0) ŚCIEŻKI (trzymamy w jednym miejscu, żeby nie było chaosu) ===
DB_PATH = r"C:\Users\PC\Desktop\sql-python-main\data\sales.db"


# === 1) Połączenie z bazą SQLite ===
conn = sqlite3.connect(DB_PATH)


try: 
    # 2) Podgląd bazy sales
    sql_preview = """
    SELECT *
    FROM sales
    LIMIT 5;
    """
    df_preview = pd.read_sql_query(sql_preview, conn)
    print("\n=== QUERY 1: Podgląd danych (LIMIT 5) ===")
    print(df_preview)
    
    #3) Jaki procent całkowitej sprzedaży stanowi każda kategoria? 
    #kolumny: category, category_total, overall_total, percentage_of_total
    sql_zadanie1 = """
    SELECT category, SUM(total_value) as category_total, SUM(SUM(total_value)) OVER () AS overall_total, ROUND(100.0 * SUM(total_value) / (SUM(SUM(total_value)) OVER ()), 2) AS percentage_of_total
    from sales
    GROUP BY category
    order by category
    ;
    """
    df_zad1 = pd.read_sql_query(sql_zadanie1, conn)
    print("\n=== QUERY 1: Jaki procent całkowitej sprzedaży stanowi każda kategoria?  ===")
    print(df_zad1)
    
    
    #4) Która kategoria sprzedaje najwięcej, która najmniej?
    #kolumny: category, category_total, rank_by_sales
    sql_zadanie2 = """
    SELECT category, SUM(total_value) as category_total, RANK() OVER (order by sum(total_value) DESC) as rank_by_sales
    from sales
    GROUP BY category
    ORDER BY SUM(total_value) DESC
    ;
    """
    df_zad2 = pd.read_sql_query(sql_zadanie2, conn)
    print("\n=== QUERY 2: Która kategoria sprzedaje najwięcej, która najmniej?  ===")
    print(df_zad2)
    
    #5) Jak narasta sprzedaż, gdy sortujemy kategorie od największej do najmniejszej?
    #kolumny: category, category_total, running_total
    sql_zadanie3 = """
    SELECT category, SUM(total_value) as category_total, SUM(SUM(total_value)) OVER (ORDER BY SUM(total_value) DESC
              ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as running_total
    from sales
    group by category
    order by SUM(total_value) DESC
    ;
    """
    df_zad3 = pd.read_sql_query(sql_zadanie3, conn)
    print("\n=== QUERY 3: Jak narasta sprzedaż, gdy sortujemy kategorie od największej do najmniejszej?  ===")
    print(df_zad3)
    
    #6) Które kategorie są powyżej średniej sprzedaży?
    #kolumny: category, category_total, avg_category_total, above_avg
    sql_zadanie4 = """
    SELECT category, SUM(total_value) as category_total, AVG(SUM(total_value)) OVER () as avg_category_total,
    CASE WHEN SUM(total_value) >= AVG(SUM(total_value)) OVER () THEN 1 ELSE 0 END as above_avg
    from sales
    group by category
    order by SUM(total_value) DESC
    ;
    """
    df_zad4 = pd.read_sql_query(sql_zadanie4, conn)
    print("\n=== QUERY 4: Które kategorie są powyżej średniej sprzedaży?  ===")
    print(df_zad4)
finally:
    conn.close()
    print("\nPołączenie z bazą zamknięte.")
