import json
from datetime import datetime
import os
from collections import defaultdict
import re

# File Configuration
# Assumes the JSON files are in the same directory as the script.
ITEM_BRANDS_FILE = 'brands.json'
RECEIPTS_FILE = 'receipts.json'
USERS_FILE = 'users.json'
OUTPUT_FILE = 'data_quality_report.txt' # File to save the report

# Helper Functions
def parse_oid(data):
    """Parses OID objects safely. e.g., {'$oid': '...'}. Returns the ID string."""
    if isinstance(data, dict) and '$oid' in data:
        return data['$oid']
    return None

def parse_date(data):
    """Parses value in date objects and converts to datetime. e.g., {'$date': ...}. Returns a datetime object."""
    if isinstance(data, dict) and '$date' in data:
        try:
            # Timestamp is in milliseconds, divide by 1000.
            return datetime.fromtimestamp(int(data['$date']) / 1000)
        except (ValueError, TypeError):
            return None
    return

# Main DQ Class
class DataQualityChecker:
    """A class to encapsulate data quality checks for the Fetch Rewards dataset."""

    def __init__(self):
        self.issues = defaultdict(set) # Using set here since there's a good amount of duplicate errors
        self.user_ids = set()
        self.brand_codes = set()
        self.processed_ids = defaultdict(set)

    def log_issue(self, file, issue_type, record_id, message):
        """Logs a data quality issue for later reporting."""
        self.issues[issue_type].add(f"[{file}] Record ID {record_id}: {message}")

    def pre_scan_files(self):
        """Scans users and brands files to build sets of valid IDs for cross-referencing."""
        print("Pre-scanning files to build reference ID sets...")
        # Pre-scan users to get all valid user IDs
        with open(USERS_FILE, 'r') as f:
            for line in f:
                try:
                    user = json.loads(line)
                    user_id = parse_oid(user.get('_id'))
                    if user_id:
                        self.user_ids.add(user_id)
                except json.JSONDecodeError:
                    continue # This will be caught in the main check

        # Pre-scan item brand data to get all valid brand codes
        with open(ITEM_BRANDS_FILE, 'r') as f:
            for line in f:
                try:
                    brand = json.loads(line)
                    brand_code = brand.get('brandCode')
                    if brand_code:
                        self.brand_codes.add(brand_code)
                except json.JSONDecodeError:
                    continue
        print(f"Found {len(self.user_ids)} unique user IDs and {len(self.brand_codes)} unique brand codes.")

    def check_users(self):
        """Performs data quality checks on users.json."""
        print("Analyzing users.json...")
        with open(USERS_FILE, 'r') as f:
            for i, line in enumerate(f, 1):
                try:
                    user = json.loads(line)
                    record_id = parse_oid(user.get('_id'))

                    # ID and Duplicate Checks
                    if not record_id:
                        self.log_issue("Users", "Missing ID", f"line {i}", "Record has no valid '_id'.")
                        continue
                    if record_id in self.processed_ids['users']:
                        self.log_issue("Users", "Duplicate ID", record_id, "User ID is duplicated in the file.")
                    self.processed_ids['users'].add(record_id)

                    # Field Presence and Validity
                    if 'active' not in user:
                        self.log_issue("Users", "Missing Field", record_id, "User is missing the 'active' field.")
                    if 'createdDate' not in user:
                        self.log_issue("Users", "Missing Field", record_id, "User is missing the 'createdDate' field.")
                    if 'role' not in user or user.get('role') not in ('consumer'):
                         self.log_issue("Users", "Invalid Value", record_id, f"User has an invalid (not 'consumer') or missing role: '{user.get('role')}'.")
                    if 'state' in user and (user.get('state') is None or not isinstance(user['state'], str) or len(user['state']) != 2):
                        self.log_issue("Users", "Invalid Value", record_id, f"User 'state' is not a 2-letter code: '{user.get('state')}'.")

                    # Logical Consistency
                    created_date = parse_date(user.get('createdDate'))
                    last_login = parse_date(user.get('lastLogin'))
                    if created_date and last_login and last_login < created_date:
                        self.log_issue("Users", "Logical Inconsistency", record_id, "lastLogin date is before createdDate.")

                except json.JSONDecodeError:
                    self.log_issue("Users", "Invalid JSON", f"line {i}", "Line is not a valid JSON object.")

    def check_brands(self):
        """Performs data quality checks on brands.json."""
        print("Analyzing brands.json...")
        with open(ITEM_BRANDS_FILE, 'r') as f:
            for i, line in enumerate(f, 1):
                try:
                    brand = json.loads(line)
                    record_id = parse_oid(brand.get('_id'))

                    # ID and Duplicate Checks
                    if not record_id:
                        self.log_issue("Brands", "Missing ID", f"line {i}", "Record has no valid '_id'.")
                        continue
                    if record_id in self.processed_ids['brands']:
                        self.log_issue("Brands", "Duplicate ID", record_id, "ID is duplicated in the file.")
                    self.processed_ids['brands'].add(record_id)

                    # Field Presence and Validity
                    if 'barcode' not in brand:
                        self.log_issue("Brands", "Missing Field", record_id, "Row is missing the 'barcode' field.")
                    if 'brandCode' not in brand:
                        self.log_issue("Brands", "Missing Field", record_id, "Row is missing the 'brandCode' field.")
                    if 'name' not in brand:
                        self.log_issue("Brands", "Missing Field", record_id, "Row is missing the 'name' field.")

                    # Special Case Checks
                    if brand.get('name') and 'test' in brand['name']:
                        self.log_issue("Brands", "Test Data", record_id, f"Brand name '{brand['name']}' appears to be a test entry.")
                    if ('category' in brand) ^ ('categoryCode' in brand): # XOR check
                        self.log_issue("Brands", "Inconsistent Fields", record_id, "Has 'category' or 'categoryCode' but not both.")
                    cpg_data = brand.get('cpg')
                    if cpg_data:
                        cpg_id = parse_oid(cpg_data.get('$id'))
                        cpg_ref = cpg_data.get('$ref')
                        if (cpg_id) ^ (cpg_ref): # XOR check
                            self.log_issue("Brands", "Inconsistent Fields", record_id, "cpg field has '$id' or '$ref' in JSON data but not both.")

                except json.JSONDecodeError:
                    self.log_issue("Brands", "Invalid JSON", f"line {i}", "Line is not a valid JSON object.")

    def check_receipts(self):
        """Performs data quality checks on receipts.json."""
        print("Analyzing receipts.json...")
        with open(RECEIPTS_FILE, 'r') as f:
            for i, line in enumerate(f, 1):
                try:
                    receipt = json.loads(line)
                    record_id = parse_oid(receipt.get('_id'))

                    # ID and Duplicate Checks
                    if not record_id:
                        self.log_issue("Receipts", "Missing ID", f"line {i}", "Record has no valid '_id'.")
                        continue
                    if record_id in self.processed_ids['receipts']:
                        self.log_issue("Receipts", "Duplicate ID", record_id, "Receipt ID is duplicated in the file.")
                    self.processed_ids['receipts'].add(record_id)

                    # Referential Integrity
                    user_id = receipt.get('userId')
                    if not user_id or user_id not in self.user_ids:
                        self.log_issue("Receipts", "Orphan Record", record_id, f"Contains userId '{user_id}' which does not exist in users.json.")

                    # Logical Consistency
                    date_scanned = parse_date(receipt.get('dateScanned'))
                    purchase_date = parse_date(receipt.get('purchaseDate'))
                    if date_scanned and purchase_date and purchase_date > date_scanned:
                        self.log_issue("Receipts", "Logical Inconsistency", record_id, f"purchaseDate {purchase_date} is after dateScanned {date_scanned}.")

                    # Financial and Item Consistency
                    item_list = receipt.get('rewardsReceiptItemList', [])
                    purchased_item_count = receipt.get('purchasedItemCount')
                    if purchased_item_count is not None and purchased_item_count != len(item_list):
                        self.log_issue("Receipts", "Inconsistent Count", record_id, f"purchasedItemCount ({purchased_item_count}) does not match rewardsReceiptItemList count ({len(item_list)}).")

                    calculated_total = 0
                    has_price = False
                    for item in item_list:
                        # Check for orphan item-brand links
                        brand_code = item.get('brandCode')
                        if brand_code and brand_code not in self.brand_codes:
                             self.log_issue("Receipts", "Orphan Item-Brand Link", record_id, f"Item has brandCode '{brand_code}' which does not exist in brands.json.")
                        
                        # Sum up prices for financial check
                        try:
                            # Use finalPrice if available, otherwise fall back to itemPrice
                            price_to_use = item.get('finalPrice') if item.get('finalPrice') is not None else item.get('itemPrice')
                            if price_to_use is not None:
                                final_price = float(price_to_use)
                                calculated_total += final_price
                                has_price = True
                        except (ValueError, TypeError):
                            continue
                    
                    if has_price:
                        try:
                            total_spent = float(receipt.get('totalSpent', 0))
                            # Check if sum of item prices matches receipt total, allowing for small floating point differences
                            if not (abs(calculated_total - total_spent) < 0.01):
                                self.log_issue("Receipts", "Financial Inconsistency", record_id, f"totalSpent ({total_spent}) does not match sum of item prices ({calculated_total:.2f}).")
                        except (ValueError, TypeError):
                             self.log_issue("Receipts", "Invalid Value", record_id, f"totalSpent is not a valid number: '{receipt.get('totalSpent')}'.")

                except json.JSONDecodeError:
                    self.log_issue("Receipts", "Invalid JSON", f"line {i}", "Line is not a valid JSON object.")

    def print_report(self):
        """Writes a formatted summary of all found data quality issues to a text file."""
        print(f"\nGenerating data quality report at '{OUTPUT_FILE}'...")
        try:
            with open(OUTPUT_FILE, 'w') as f:
                f.write("--- Data Quality Report ---\n")
                if not self.issues:
                    f.write("\nCongratulations! No data quality issues were found.\n")
                    print("Report generated. No issues were found.")
                    return

                total_issues = 0
                for issue_type, messages in sorted(self.issues.items()):
                    f.write(f"\n[+] Issue Type: {issue_type} ({len(messages)} found)\n")
                    total_issues += len(messages)
                    for msg in messages:
                        f.write(f"  - {msg}\n")
                
                f.write("\n--- End of Report ---\n")
                f.write(f"\nTotal Issues Found: {total_issues}\n")
            
            print(f"Report successfully written to '{OUTPUT_FILE}'.")
        except IOError as e:
            print(f"\nFATAL ERROR: Could not write report to file. Reason: {e}")


    def run(self):
        """Executes the full data quality check suite."""
        self.pre_scan_files()
        self.check_users()
        self.check_brands()
        self.check_receipts()
        self.print_report()


if __name__ == '__main__':
    checker = DataQualityChecker()
    checker.run()
