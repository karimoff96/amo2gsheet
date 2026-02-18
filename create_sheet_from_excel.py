"""
Script to read data.xlsx and create/update a Google Sheet with the same columns and data.
"""

import os
import ssl
import pandas as pd
import gspread
from dotenv import load_dotenv

# Workaround for SSL certificate verification issues
import certifi
os.environ['REQUESTS_CA_BUNDLE'] = certifi.where()
os.environ['SSL_CERT_FILE'] = certifi.where()

# Load environment variables
load_dotenv()

# Configuration
EXCEL_FILE = "data.xlsx"
SERVICE_ACCOUNT_JSON = os.getenv("GS_SERVICE_ACCOUNT_JSON", "gsheet.json")
SPREADSHEET_ID = os.getenv("GS_SPREADSHEET_ID", "")
WORKSHEET_NAME = os.getenv("GS_WORKSHEET_NAME", "Data")


def read_excel_file(filepath: str) -> pd.DataFrame:
    """Read the Excel file and return a DataFrame."""
    print(f"Reading Excel file: {filepath}")
    df = pd.read_excel(filepath)
    
    # Remove unnamed columns (empty columns)
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
    
    print(f"Found {len(df)} rows and {len(df.columns)} columns")
    print(f"Columns: {list(df.columns)}")
    
    return df


def create_or_update_sheet(df: pd.DataFrame, service_account_json: str, spreadsheet_id: str, worksheet_name: str):
    """Create or update a Google Sheet with the DataFrame columns and data."""
    
    # Connect to Google Sheets
    print(f"\nConnecting to Google Sheets...")
    client = gspread.service_account(filename=service_account_json)
    spreadsheet = client.open_by_key(spreadsheet_id)
    
    # Try to get the worksheet, or create it if it doesn't exist
    try:
        sheet = spreadsheet.worksheet(worksheet_name)
        print(f"Found existing worksheet: {worksheet_name}")
        # Clear existing content
        sheet.clear()
        print(f"Cleared existing content")
    except gspread.exceptions.WorksheetNotFound:
        sheet = spreadsheet.add_worksheet(title=worksheet_name, rows=len(df) + 1, cols=len(df.columns))
        print(f"Created new worksheet: {worksheet_name}")
    
    # Prepare the column headers only
    headers = df.columns.tolist()
    
    # Update the sheet with headers only
    print(f"\nUploading column headers to Google Sheets...")
    sheet.update([headers], value_input_option='RAW')
    
    print(f"✓ Successfully created/updated sheet '{worksheet_name}' with column headers")
    print(f"✓ Columns ({len(headers)}): {', '.join(headers)}")
    print(f"\nSheet URL: https://docs.google.com/spreadsheets/d/{spreadsheet_id}")
    
    return sheet


def main():
    """Main function to execute the script."""
    
    if not SPREADSHEET_ID:
        print("Error: GS_SPREADSHEET_ID environment variable is not set!")
        print("Please set it in your .env file or environment variables.")
        return
    
    # Read the Excel file
    df = read_excel_file(EXCEL_FILE)
    
    # Create or update the Google Sheet
    create_or_update_sheet(df, SERVICE_ACCOUNT_JSON, SPREADSHEET_ID, WORKSHEET_NAME)


if __name__ == "__main__":
    main()
