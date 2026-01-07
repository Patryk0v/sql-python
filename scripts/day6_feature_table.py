import pandas as pd
import sqlite3

# === 0) ŚCIEŻKI (trzymamy w jednym miejscu, żeby nie było chaosu) ===
DB_PATH = r"C:\Users\PC\Desktop\sql-python-main\data\sales.db"
DF_PATH = r"C:\Users\PC\Desktop\sql-python-main\data"

# === 1) Połączenie z bazą SQLite ===
conn = sqlite3.connect(DB_PATH)



#DATAFRAME, który posiada category, order_count (ile wierszy w danej kategorii), total_revenue, avg_order_value,
#min_order_value, max_order_value, revenue_share_pct (udzial % w calkowitej sprzedazy)

try:
    
    day6_baza = """
    SELECT category as category, count(*) as order_count, SUM(total_value) AS total_revenue, avg(total_value) as avg_order_value, min(total_value) as min_order_value,
    max(total_value) as max_order_value, Round(sum(total_value) / (SUM(SUM(total_value)) OVER()),2)*100 as revenue_share_pct
    FROM sales
    group by category;
    """
    df_preview = pd.read_sql_query(day6_baza, conn)
    print(df_preview)
    
    
    df_preview.to_csv(DF_PATH+'\\feature_table_category.csv',index=False)

finally:
conn.close()
