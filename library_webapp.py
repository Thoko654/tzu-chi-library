import os
import base64
import hashlib
from datetime import datetime, timedelta
import pandas as pd
import streamlit as st
import plotly.express as px

# ======================================================
# Config
# ======================================================
st.set_page_config(page_title="Tzu Chi Library", layout="wide")

# Use a stable data folder to avoid path issues
DATA_DIR = os.path.join(os.getcwd(), "data")
os.makedirs(DATA_DIR, exist_ok=True)

STUDENT_CSV = os.path.join(DATA_DIR, "Student_records.csv")
BOOKS_CSV   = os.path.join(DATA_DIR, "Library_books.csv")
LOG_CSV     = os.path.join(DATA_DIR, "Borrow_log.csv")

# ======================================================
# Simple Credential Store
# ======================================================
def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()

USERS = {
    "admin":   hash_password("admin123"),
    "teacher": hash_password("tzuchi2025"),
}

def verify_login(username, password):
    return USERS.get(username) == hash_password(password)

def login_form():
    st.markdown("## 🔐 Login")
    username = st.text_input("Username")
    password = st.text_input("Password", type="password")
    if st.button("Login"):
        if verify_login(username, password):
            st.session_state["logged_in"] = True
            st.session_state["username"] = username
            st.success("✅ Login successful")
            st.rerun()
        else:
            st.error("❌ Invalid credentials")

# ======================================================
# CSV Utilities (no caching → always read latest)
# ======================================================
def ensure_files():
    # Create with modern headers if files don't exist
    if not os.path.exists(STUDENT_CSV):
        pd.DataFrame(columns=["Code", "Name", "Surname", "Gender"]).to_csv(STUDENT_CSV, index=False, encoding="utf-8")
    if not os.path.exists(BOOKS_CSV):
        pd.DataFrame(columns=["Book ID", "Book Title", "Author", "Status"]).to_csv(BOOKS_CSV, index=False, encoding="utf-8")
    if not os.path.exists(LOG_CSV):
        pd.DataFrame(columns=["Student", "Book Title", "Book ID", "Date Borrowed", "Due Date", "Returned"]).to_csv(LOG_CSV, index=False, encoding="utf-8")

def load_students():
    df = pd.read_csv(STUDENT_CSV, dtype=str).fillna("")
    df.columns = df.columns.str.strip()

    # Backward compatibility: map old columns to new
    rename_map = {}
    if "Boy / Girl" in df.columns and "Gender" not in df.columns:
        rename_map["Boy / Girl"] = "Gender"
    if "First Name" in df.columns and "Name" not in df.columns:
        rename_map["First Name"] = "Name"
    if "Last Name" in df.columns and "Surname" not in df.columns:
        rename_map["Last Name"] = "Surname"
    if "Student Code" in df.columns and "Code" not in df.columns:
        rename_map["Student Code"] = "Code"
    if "ID" in df.columns and "Code" not in df.columns:
        rename_map["ID"] = "Code"
    df = df.rename(columns=rename_map)

    # If file has no Code column, create an empty one (still works, but scanning needs Code)
    if "Code" not in df.columns:
        df["Code"] = ""

    for c in df.columns:
        df[c] = df[c].astype(str).str.strip()
    return df

def load_books():
    df = pd.read_csv(BOOKS_CSV, dtype=str).fillna("")
    df.columns = df.columns.str.strip()

    # Normalize headers & fields
    if "Status" not in df.columns:
        df["Status"] = "Available"
    for c in df.columns:
        df[c] = df[c].astype(str).str.strip()

    # Remove blank rows (no ID and no Title)
    if "Book Title" in df.columns and "Book ID" in df.columns:
        df = df[~((df["Book Title"] == "") & (df["Book ID"] == ""))].copy()

    # Normalize status values
    df["Status"] = (
        df["Status"].str.lower()
        .map({"available": "Available", "borrowed": "Borrowed", "out": "Borrowed", "issued": "Borrowed", "": "Available"})
        .fillna("Available")
    )
    return df

def load_logs():
    df = pd.read_csv(LOG_CSV, dtype=str).fillna("")
    df.columns = df.columns.str.strip()
    for c in df.columns:
        df[c] = df[c].astype(str).str.strip()
    return df

def save_students(df):
    df.to_csv(STUDENT_CSV, index=False, encoding="utf-8")

def save_books(df):
    df.to_csv(BOOKS_CSV, index=False, encoding="utf-8")

def save_logs(df):
    df.to_csv(LOG_CSV, index=False, encoding="utf-8")

def df_append(df, row_dict):
    return pd.concat([df, pd.DataFrame([row_dict])], ignore_index=True)

# ======================================================
# Main Library System
# ======================================================
def main():
    ensure_files()
    students = load_students()
    books = load_books()
    logs = load_logs()

    # Optional logo
    logo_path = os.path.join("assets", "chi-logo.png")
    if os.path.exists(logo_path):
        with open(logo_path, "rb") as image_file:
            encoded = base64.b64encode(image_file.read()).decode()
        st.markdown(f"<div style='text-align: center;'><img src='data:image/png;base64,{encoded}' width='200'></div>", unsafe_allow_html=True)

    st.markdown("<h1 style='text-align: center;'>📚 Tzu Chi Foundation — Saturday Tutor Class Library System</h1>", unsafe_allow_html=True)

    # Top metrics (only count books with a title)
    total_books = books["Book Title"].str.strip().ne("").sum() if "Book Title" in books.columns else 0
    available_count = (((books["Status"] == "Available") & books["Book Title"].str.strip().ne("")).sum()
                       if "Status" in books.columns and "Book Title" in books.columns else 0)
    open_borrows = logs["Returned"].str.lower().eq("no").sum() if "Returned" in logs.columns else 0

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Students", len(students))
    col2.metric("Books", int(total_books))
    col3.metric("Available", int(available_count))
    col4.metric("Borrowed (open)", int(open_borrows))

    tabs = st.tabs(["📖 Borrow", "📦 Return", "➕ Add", "🗑️ Delete", "📜 Logs", "📈 Analytics"])

    # ---------------------- Borrow ----------------------
    with tabs[0]:
        st.subheader("Borrow a Book")

        # Option to include borrowed books for back-capture
        include_borrowed = st.checkbox("Show borrowed books (for back capture / corrections)", value=False)

        # Students
        if {"Name", "Surname"}.issubset(students.columns):
            student_names = (students["Name"].str.strip() + " " + students["Surname"].str.strip()).dropna().tolist()
        else:
            student_names = []
        selected_student_
