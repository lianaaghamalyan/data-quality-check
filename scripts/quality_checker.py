import pandas as pd
import re
from datetime import datetime
import os
from dotenv import load_dotenv
load_dotenv()


def run_checks():
    data_path = os.getenv("DATA_PATH", "data/")

    cust = pd.read_csv(os.path.join(data_path, "customers.csv"), sep=";")
    prod = pd.read_csv(os.path.join(data_path, "products.csv"), sep=";")
    trans = pd.read_csv(os.path.join(data_path, "transactions.csv"), sep=";")

    trans["transaction_date"] = pd.to_datetime(trans["transaction_date"], errors="coerce")

    report = []
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    report.append(f"DATA QUALITY ISSUES REPORT\n{'='*27}\n")
    report.append(f"Generated on: {timestamp}\n\n")
    report.append("The following data quality checks were performed:\n")

    def log(msg, found):
        return f"  {'[!] ' if found else '[✓] '} {msg}"

    # --- CUSTOMER TABLE
    report.append("Customer Table Checks:\n")

    null_cust = cust.isnull().sum()
    report.extend([log(f"Missing values in column '{col}': {count}", count > 0)
                   for col, count in null_cust.items()])

    dupe_cust = cust.duplicated().sum()
    report.append(log(f"Duplicate rows: {dupe_cust}", dupe_cust > 0))

    email_pattern = r'^[\w\.-]+@[\w\.-]+\.\w{2,4}$'
    invalid_emails = cust[~cust["email"].str.match(email_pattern)]
    report.append(log(f"Invalid email addresses: {len(invalid_emails)}", not invalid_emails.empty))

    # --- PRODUCT TABLE
    report.append("\nProduct Table Checks:\n")

    null_prod = prod.isnull().sum()
    report.extend([log(f"Missing values in column '{col}': {count}", count > 0)
                   for col, count in null_prod.items()])

    dupe_prod = prod.duplicated().sum()
    report.append(log(f"Duplicate rows: {dupe_prod}", dupe_prod > 0))

    zero_price = prod[(prod["price"].isnull()) | (prod["price"] == 0)]
    report.append(log(f"Products with missing or 0 price: {len(zero_price)}", not zero_price.empty))

    if prod["product_id"].min() > 1:
        report.append(log(f"Product IDs start from {prod['product_id'].min()} (expected 1)", True))
    else:
        report.append(log("Product ID starts correctly from 1", False))

    # --- TRANSACTION TABLE
    report.append("\nTransaction Table Checks:\n")

    null_trans = trans.isnull().sum()
    report.extend([log(f"Missing values in column '{col}': {count}", count > 0)
                   for col, count in null_trans.items()])

    dupe_trans = trans.duplicated().sum()
    report.append(log(f"Duplicate rows: {dupe_trans}", dupe_trans > 0))

    neg_trans = trans[trans["amount"] <= 0]
    report.append(log(f"Negative or zero transaction amounts: {len(neg_trans)}", not neg_trans.empty))

    future_trans = trans[trans["transaction_date"] > datetime.now()]
    report.append(log(f"Transactions dated in the future: {len(future_trans)}", not future_trans.empty))

    missing_date = trans[trans["transaction_date"].isnull()]
    report.append(log(f"Missing transaction dates: {len(missing_date)}", not missing_date.empty))

    invalid_pid = trans[~trans["product_id"].isin(prod["product_id"])]
    report.append(log(f"Transactions with invalid product_id references: {len(invalid_pid)}", not invalid_pid.empty))

    # --- SUMMARY
    report.append("\nSummary of Issues Found:\n")
    summary = [line for line in report if line.startswith("  [!]")]
    if summary:
        report.extend(summary)
    else:
        report.append("  No critical data quality issues found.\n")

    # Save report
    today = datetime.now().strftime("%Y_%m_%d")
    out_path = f"reports/quality_check_{today}.txt"
    os.makedirs("reports", exist_ok=True)
    with open(out_path, "w") as f:
        f.write("\n".join(report))

    print(f"[+] Report generated: {out_path}")


# Airflow entry point
if __name__ == "__main__":
    run_checks()
