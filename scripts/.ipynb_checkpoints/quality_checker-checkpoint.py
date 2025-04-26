import pandas as pd
import re
from datetime import datetime

# Load datasets (semicolon-delimited)
cust = pd.read_csv("customers.csv", sep=";")
prod = pd.read_csv("products.csv", sep=";")
trans = pd.read_csv("transactions.csv", sep=";")

# Convert date column
trans["transaction_date"] = pd.to_datetime(trans["transaction_date"], errors="coerce")

# -------------------------------
# Initialize flag collector
flagged_rows = []
issues_log = []

# 1. MISSING VALUES IN ALL TABLES
def log_missing(df, name):
    missing = df.isnull().sum()
    total = missing.sum()
    if total > 0:
        print(f"[!] {name} table has {total} missing values")
        print(missing[missing > 0])
        issues_log.append(f"{name}: Missing values")
    else:
        print(f"[✓] No missing values in {name}")

log_missing(cust, "Customers")
log_missing(prod, "Products")
log_missing(trans, "Transactions")

# 2. DUPLICATES
for df, name in [(cust, "Customers"), (prod, "Products"), (trans, "Transactions")]:
    dups = df.duplicated()
    if dups.any():
        print(f"[!] {name} table has {dups.sum()} duplicate rows")
        issues_log.append(f"{name}: Duplicate rows")
        flagged_rows.append(df[dups])
    else:
        print(f"[✓] No duplicates in {name}")

# 3. MISSING TRANSACTION DATES
missing_date = trans[trans["transaction_date"].isnull()]
if not missing_date.empty:
    print(f"[!] Found {len(missing_date)} transactions with missing transaction_date")
    issues_log.append("Transactions: Missing transaction_date")
    flagged_rows.append(missing_date)

# 4. NEGATIVE OR ZERO TRANSACTION AMOUNTS
neg_trans = trans[trans["amount"] <= 0]
if not neg_trans.empty:
    print(f"[!] Found {len(neg_trans)} transactions with negative or zero amount")
    issues_log.append("Transactions: Negative or zero amounts")
    flagged_rows.append(neg_trans)

# 5. FUTURE DATES
future_trans = trans[trans["transaction_date"] > datetime.now()]
if not future_trans.empty:
    print(f"[!] Found {len(future_trans)} transactions dated in the future")
    issues_log.append("Transactions: Future-dated records")
    flagged_rows.append(future_trans)

# 6. INVALID PRODUCT REFERENCES
bad_product_refs = trans[~trans["product_id"].isin(prod["product_id"])]
if not bad_product_refs.empty:
    print(f"[!] {len(bad_product_refs)} transactions have invalid product_id")
    issues_log.append("Transactions: Invalid product_id")
    flagged_rows.append(bad_product_refs)

# 7. BAD PRICES IN PRODUCTS
bad_prices = prod[(prod["price"].isnull()) | (prod["price"] == 0)]
if not bad_prices.empty:
    print(f"[!] Found {len(bad_prices)} products with missing or 0 price")
    issues_log.append("Products: Missing or zero price")
    flagged_rows.append(bad_prices)

# 8. PRODUCT ID STARTS FROM 101 (anomaly?)
if prod["product_id"].min() > 1:
    expected_start = 1
    actual_start = prod["product_id"].min()
    print(f"[!] Product IDs start from {actual_start} (expected {expected_start})")
    issues_log.append(f"Products: Product ID starts from {actual_start}, expected {expected_start}")

# 9. INVALID EMAILS
email_pattern = r'^[\w\.-]+@[\w\.-]+\.\w{2,4}$'
invalid_emails = cust[~cust["email"].str.match(email_pattern)]
if not invalid_emails.empty:
    print(f"[!] Found {len(invalid_emails)} invalid emails")
    issues_log.append("Customers: Invalid emails")
    flagged_rows.append(invalid_emails)

# -------------------------------
# EXPORT
if flagged_rows:
    combined_flags = pd.concat(flagged_rows, ignore_index=True)
    combined_flags.to_csv("flagged_transactions.csv", index=False)
    print("[+] Flagged rows exported to 'flagged_transactions.csv'")

# EXPORT ISSUE SUMMARY (optional)
# Export final report
if issues_log:
    with open("quality_issues_log.txt", "w") as f:
        f.write("DATA QUALITY ISSUES REPORT\n")
        f.write("===========================\n\n")
        f.write(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        f.write("The following potential data quality issues were found:\n\n")
        for issue in issues_log:
            f.write(f"- {issue}\n")
    print("[+] Detailed summary saved to 'quality_issues_log.txt'")
else:
    print("[✓] All checks passed. No critical data issues found.")
