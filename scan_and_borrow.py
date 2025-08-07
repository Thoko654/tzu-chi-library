import pandas as pd
from datetime import datetime, timedelta
import os

# Load student records
students_df = pd.read_csv("Student_records.csv")

# Load borrow log (or create if not exists)
if os.path.exists("Borrow_log.csv"):
    borrow_log = pd.read_csv("Borrow_log.csv")
else:
    borrow_log = pd.DataFrame(columns=["Name", "Surname", "Book Title", "Borrow Date", "Return Due", "Returned"])

# Ask user to input scanned barcode or full name
student_input = input("📷 Scan Student Barcode (or type full name like David_Smith): ").strip()

# Parse student input
if "_" in student_input:
    name_parts = student_input.split("_")
    if len(name_parts) == 2:
        name, surname = name_parts
    else:
        print("❌ Invalid name format. Use Name_Surname.")
        exit()
else:
    print("❌ Please enter full name in format Name_Surname or scan a barcode.")
    exit()

# Match student
matched = students_df[
    (students_df["Name"].str.strip().str.lower() == name.strip().lower()) &
    (students_df["Surname"].str.strip().str.lower() == surname.strip().lower())
]

if matched.empty:
    print("❌ Student not found.")
    exit()

print(f"👋 Hello {name} {surname}!")

# Get book title
book_title = input("📚 Enter book title: ").strip()

# Record borrow time
borrow_date = datetime.today().strftime('%Y-%m-%d')
return_date = (datetime.today() + timedelta(days=14)).strftime('%Y-%m-%d')

# Create new entry as DataFrame
new_entry = pd.DataFrame([{
    "Name": name,
    "Surname": surname,
    "Book Title": book_title,
    "Borrow Date": borrow_date,
    "Return Due": return_date,
    "Returned": "No"
}])

# Append new entry to the log
borrow_log = pd.concat([borrow_log, new_entry], ignore_index=True)

# Save updated log
borrow_log.to_csv("Borrow_log.csv", index=False)

print(f"✅ {book_title} has been borrowed by {name} {surname}. Due back on {return_date}.")
