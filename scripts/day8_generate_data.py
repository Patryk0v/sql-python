import numpy as np
import pandas as pd

from datetime import datetime


# ====== USTAWIENIA (TU BĘDZIESZ GRZEBAŁ) ======
N_ROWS = 20000

START_DATE = "2025-01-01"
END_DATE = "2025-12-31"

CATEGORIES = ["A", "B", "C", "D"]
REGIONS = ["North", "South", "West", "East"]
SEGMENTS = ["retail", "business"]

# Wagi: mają sumować się do 1.0
CATEGORY_WEIGHTS = [0.50, 0.25, 0.20, 0.05]   # A dominuje
REGION_WEIGHTS = [0.30, 0.25, 0.25, 0.20]
SEGMENT_WEIGHTS = [0.75, 0.25]

# Bazowe ceny per kategoria (średnie, potem zrobimy szum)
CATEGORY_BASE_PRICE = {"A": 30, "B": 18, "C": 12, "D": 8}

# Sezonowość: miesiące z większą liczbą zamówień
# (np. listopad/grudzień większa sprzedaż)
MONTH_WEIGHTS = {
    1: 0.05, 2: 0.06, 3: 0.07, 4: 0.07,
    5: 0.08, 6: 0.10, 7: 0.09, 8: 0.09,
    9: 0.08, 10: 0.07, 11: 0.11, 12: 0.13
}


def random_dates_by_month_weights(n: int, start: str, end: str, month_weights: dict) -> pd.Series:
    """
    Generuje daty z większym prawdopodobieństwem w wybranych miesiącach.
    Wersja prosta: losujemy miesiąc wg wag, potem losujemy dzień w tym miesiącu.
    """
    start_dt = datetime.fromisoformat(start)
    end_dt = datetime.fromisoformat(end)

    years = list(range(start_dt.year, end_dt.year + 1))
    months = list(month_weights.keys())
    probs = np.array([month_weights[m] for m in months], dtype=float)
    probs = probs / probs.sum()

    picked_months = np.random.choice(months, size=n, p=probs)

    dates = []
    for m in picked_months:
        # losujemy rok w zakresie
        y = np.random.choice(years)

        # bez zabawy w różne długości miesięcy: bierzemy 1..28 (wystarczy do symulacji)
        day = np.random.randint(1, 29)

        d = datetime(y, m, day)

        # przytnij do zakresu start-end
        if d < start_dt:
            d = start_dt
        if d > end_dt:
            d = end_dt

        dates.append(d.date().isoformat())

    return pd.Series(dates)


def generate_sales(n_rows: int) -> pd.DataFrame:
    # ----- klucze i daty -----
    order_id = np.arange(1, n_rows + 1)

    order_date = random_dates_by_month_weights(
        n=n_rows,
        start=START_DATE,
        end=END_DATE,
        month_weights=MONTH_WEIGHTS
    )

    # ----- kategorie / region / segment -----
    category = np.random.choice(CATEGORIES, size=n_rows, p=CATEGORY_WEIGHTS)
    region = np.random.choice(REGIONS, size=n_rows, p=REGION_WEIGHTS)
    customer_segment = np.random.choice(SEGMENTS, size=n_rows, p=SEGMENT_WEIGHTS)

    # ----- price i quantity (realistycznie, z szumem) -----
    # cena zależy od kategorii + trochę losowości
    base_price = pd.Series(category).map(CATEGORY_BASE_PRICE).astype(float)

    # szum: np. lognormal daje dodatnie wartości i sensowny rozrzut
    noise = np.random.lognormal(mean=0.0, sigma=0.25, size=n_rows)
    price = (base_price * noise)
    #1) business -> +10%
    business_mask = (customer_segment == "business")
    price = np.where(business_mask, price * 1.10, price)
    # 2) miesiąc 11 lub 12 -> +15%
    month = pd.to_datetime(order_date).dt.month
    season_mask = month.isin([11, 12])
    price = np.where(season_mask, price * 1.15, price)
    
    price = np.round(price, 2)

    # quantity: retail zwykle 1-3, business częściej większe zamówienia
    quantity = np.where(
        customer_segment == "business",
        np.random.poisson(lam=3.0, size=n_rows) + 1,   # 1..?
        np.random.poisson(lam=1.2, size=n_rows) + 1
    )

    # ----- df -----
    df = pd.DataFrame({
        "order_id": order_id,
        "order_date": order_date,
        "category": category,
        "region": region,
        "customer_segment": customer_segment,
        "price": price,
        "quantity": quantity
    })

    # total_value jako pole pomocnicze (przyda się jutro do walidacji)
    df["total_value"] = (df["price"] * df["quantity"]).round(2)

    return df


if __name__ == "__main__":
    df = generate_sales(N_ROWS)

    print("=== PREVIEW ===")
    print(df.head())
    print("\n=== INFO ===")
    print(df.info())
    print("\n=== BASIC CHECKS ===")
    print("Rows:", len(df))
    print("Total revenue:", df["total_value"].sum())

    # zapis
    out_path = r"C:\Users\PC\Desktop\sql-python-main\data_raw\sales_raw.csv"
    df.to_csv(out_path, index=False)
    print(f"\nSaved to: {out_path}")

