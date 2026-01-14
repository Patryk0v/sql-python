import sqlite3
import pandas as pd


CSV_PATH = r'C:\Users\PC\Desktop\sql-python-main\data_raw\sales_raw.csv'
DB_PATH = r"C:\Users\PC\Desktop\sql-python-main\data\sales_bi.db"

df = pd.read_csv(CSV_PATH)

print("\n=== Wczytano sales_raw.csv ===")
print(df.head())
print("\nTypy kolumn:")
print(df.dtypes)
print("\nLiczba wierszy:", len(df))

conn = sqlite3.connect(DB_PATH)

try:
    df.to_sql("sales_raw", conn, if_exists="replace", index=False)
    
    print("\n=== Zapisano dane do SQLite (tabela: sales_raw) ===")
    
    #Tworzenie tabeli 1
    fact_sales = """
    DROP TABLE IF EXISTS fact_sales;
    
    CREATE TABLE fact_sales AS
    SELECT
        order_id,
        order_date,
        CAST(strftime('%Y', order_date) AS INTEGER) AS year,
        CAST(strftime('%m', order_date) AS INTEGER) AS month,
        category,
        region,
        customer_segment,
        price,
        quantity,
        total_value
    FROM sales_raw;
    """
    
    conn.executescript(fact_sales)
    conn.commit()
    
    fact_sales = pd.read_sql_query("SELECT * FROM fact_sales;", conn)
    print("\n=== QUERY 1: Tabela fact_sales ===")
    print(fact_sales)
    
    
    #Tworzenie tabeli 2 agregacje
    agg_sales_monthly = """
    DROP TABLE IF EXISTS agg_sales_monthly;

    CREATE TABLE agg_sales_monthly AS
    SELECT 
        CAST(strftime('%Y', order_date) AS INTEGER) AS year,
        CAST(strftime('%m', order_date) AS INTEGER) AS month,
        SUM(total_value) AS total_revenue,
        COUNT(*) AS orders_count,
        AVG(total_value) AS avg_order_value
    FROM fact_sales
    GROUP BY year, month;

    """
    
    conn.executescript(agg_sales_monthly)
    conn.commit()
    agg_sales_monthly = pd.read_sql_query("SELECT * FROM agg_sales_monthly;", conn)
    print("\n=== QUERY 2: Tabela agregacje miesieczne ===")
    print(agg_sales_monthly)
    
    
    agg_category_performance = """
    DROP TABLE IF EXISTS agg_category_performance;

    CREATE TABLE agg_category_performance AS
    SELECT 
        category,
        SUM(total_value) AS total_revenue,
        COUNT(*) AS orders_count,
        AVG(total_value) AS avg_order_value,
        ROUND(
            100.0 * SUM(total_value) / NULLIF(SUM(SUM(total_value)) OVER (), 0),
            2
        ) AS revenue_share_pct
    FROM fact_sales
    GROUP BY category;
    """
    
    conn.executescript(agg_category_performance)
    conn.commit()
    agg_category_performance = pd.read_sql_query("SELECT * FROM agg_category_performance;", conn)
    print("\n=== QUERY 2: Tabela performens kategorii ===")
    print(agg_category_performance)
    
    
    agg_region_segment = """
    DROP TABLE IF EXISTS agg_region_segment;

    CREATE TABLE agg_region_segment AS
    SELECT 
        region,
        customer_segment,
        SUM(total_value) AS total_revenue,
        COUNT(*) AS orders_count
    FROM fact_sales
    GROUP BY region, customer_segment;
    """
    
    conn.executescript(agg_region_segment)
    conn.commit()
    agg_region_segment = pd.read_sql_query("SELECT * FROM agg_region_segment;", conn)
    print("\n=== QUERY 2: Tabela regionowa ===")
    print(agg_region_segment)
    
finally:
    conn.close()
