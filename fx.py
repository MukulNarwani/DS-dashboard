import requests
from datetime import date
from db import Database

INTERVAL_TO_ANNUAL = {
    "hourly":  52 * 40,    # 2080 hrs
    "daily":   52 * 5,     # 260 days
    "weekly":  52,
    "monthly": 12,
    "yearly":  1,
}

def fetch_and_store_fx_rates(db: Database, base: str = "USD") -> dict[str, float]:
    """Fetch today's rates from frankfurter.app and store in fx_rates table."""
    today = date.today().isoformat()
    resp = requests.get(f"https://api.frankfurter.app/latest?from={base}", timeout=10)
    data = resp.json()
    rates_to_usd = {currency: 1 / rate for currency, rate in data["rates"].items()}
    rates_to_usd["USD"] = 1.0

    with db._get_connection() as conn:
        for currency, rate in rates_to_usd.items():
            conn.execute(
                """INSERT INTO fx_rates (date, currency_from, rate_to_usd)
                   VALUES (?, ?, ?)
                   ON CONFLICT(date, currency_from) DO UPDATE SET rate_to_usd = excluded.rate_to_usd""",
                (today, currency, rate)
            )
    return rates_to_usd

def get_fx_rate(db: Database, currency: str) -> float:
    """Get most recent stored rate for a currency → USD."""
    with db._get_connection() as conn:
        row = conn.execute(
            """SELECT rate_to_usd FROM fx_rates
               WHERE currency_from = ? ORDER BY date DESC LIMIT 1""",
            (currency.upper(),)
        ).fetchone()
    return row["rate_to_usd"] if row else None

def normalise_to_usd_annual(
    amount: float, currency: str, interval: str, db: Database
) -> Optional[float]:
    if amount is None:
        return None
    rate = get_fx_rate(db, currency)
    multiplier = INTERVAL_TO_ANNUAL.get((interval or "yearly").lower(), 1)
    if rate is None:
        return None
    return round(amount * multiplier * rate, 2)
