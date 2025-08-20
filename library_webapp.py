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
STUDENT_CSV = "Student_records.csv"
BOOKS_CSV = "Library_books.csv"
LOG_CSV = "Borrow_log.csv"

# ======================================================
# Simple Credential Store
# ======================================================
def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode()).hexdigest()

USERS = {
    "admin": hash_password("admin123"),
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
# CSV Utilities
# ======================================================
def ensure_files():
    # Create with correct headers if missing
    if not os.path.exists(STUDENT_CSV):
        pd.DataFrame(columns=["Code", "Name", "Surname", "Gender"]).to_csv(STUDENT_CSV, index=False)
    if not os.path.exists(BOOKS_CSV):
        pd.DataFrame(columns=["Book ID", "Book Title", "Author", "Status"]).to_csv(BOOKS_CSV, index=False)
    if not os.path.exists(LOG_CSV):
        pd.DataFrame(columns=["Student", "Book Title", "Date Borrowed", "Due Date", "Returned"]).to_csv(LOG_CSV, index=False)

@st.cache_data(ttl=3)
def load_students() -> pd.DataFrame:
    df = pd.read_csv(STUDENT_CSV, dtype=str).fillna("")
    # Clean column names
    df.columns = df.columns.str.strip()

    # Map common variants to expected headers
    rename_map = {}
    if "Boy / Girl" in df.columns: rename_map["Boy / Girl"] = "Gender"
    if "First Name" in df.columns: rename_map["First Name"] = "Name"
    if "Last Name" in df.columns:  rename_map["Last Name"]  = "Surname"
    if "Student Code" in df.columns: rename_map["Student Code"] = "Code"
    if "ID" in df.columns and "Code" not in df.columns: rename_map["ID"] = "Code"
    df = df.rename(columns=rename_map)

    # Validate required columns
    required = {"Code", "Name", "Surname"}
    missing = required - set(df.columns)
    if missing:
        st.error(
            "Student_records.csv is missing columns: "
            + ", ".join(sorted(missing))
            + ". Required headers are: Code, Name, Surname[, Gender]."
        )
        st.stop()

    # Clean whitespace
    for c in df.columns:
        df[c] = df[c].astype(str).str.strip()

    return df

@st.cache_data(ttl=3)
def load_books() -> pd.DataFrame:
    df = pd.read_csv(BOOKS_CSV, dtype=str).fillna("")
    if "Status" not in df.columns:
        df["Status"] = "Available"
    df["Status"] = df["Status"].replace("", "Available")
    return df

@st.cache_data(ttl=3)
def load_logs() -> pd.DataFrame:
    return pd.read_csv(LOG_CSV, dtype=str).fillna("")

def save_students(df: pd.DataFrame):
    df.to_csv(STUDENT_CSV, index=False)
    load_students.clear()

def save_books(df: pd.DataFrame):
    df.to_csv(BOOKS_CSV, index=False)
    load_books.clear()

def save_logs(df: pd.DataFrame):
    df.to_csv(LOG_CSV, index=False)
    load_logs.clear()

# Small helper for safe concat (instead of deprecated _append)
def _append_row(df: pd.DataFrame, row: dict) -> pd.DataFrame:
    return pd.concat([df, pd.DataFrame([row])], ignore_index=True)

# ======================================================
# Main Library System
# ======================================================
def main():
    ensure_files()
    students = load_students()
    books = load_books()
    logs = load_logs()

    # Logo
    logo_path = os.path.join("assets", "chi-logo.png")
    if os.path.exists(logo_path):
        with open(logo_path, "rb") as image_file:
            encoded = base64.b64encode(image_file.read()).decode()
        st.markdown(
            "<div style='text-align: center;'><img src='data:image/png;base64,"
            + encoded + "' width='200'></div>",
            unsafe_allow_html=True
        )

    st.markdown("<h1 style='text-align: center;'>📚 Tzu Chi Foundation — Saturday Tutor Class Library System</h1>", unsafe_allow_html=True)

    # Top metrics
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Students", len(students))
    col2.metric("Books", books["Book Title"].dropna().str.strip().ne("").sum() if "Book Title" in books.columns else 0)
    col3.metric("Available", (books["Status"] == "Available").sum() if "Status" in books.columns else 0)
    open_borrows = logs["Returned"].str.lower().eq("no").sum() if "Returned" in logs.columns else 0
    col4.metric("Borrowed (open)", open_borrows)

    tabs = st.tabs(["📖 Borrow", "📦 Return", "➕ Add", "🗑️ Delete", "📜 Logs", "📈 Analytics"])

    # ---------------------- Borrow ----------------------
    with tabs[0]:
        st.subheader("Borrow a Book")

        # Build display names
        students_display = (students["Name"] + " " + students["Surname"]).tolist()

        # Optional scan/enter code
        scanned_code = st.text_input("📷 Scan / Enter Student Code (optional)")
        default_name = None
        if scanned_code.strip():
            # Normalize: keep leading zeros (use 3 as typical width; adjust if you use other lengths)
            code_norm = scanned_code.strip().zfill(3)
            match = students.loc[students["Code"].str.zfill(3) == code_norm]
            if match.empty:
                st.error("No student found with that code.")
            else:
                default_name = f"{match.iloc[0]['Name']} {match.iloc[0]['Surname']}"

        # Selectbox (auto-select if code matched)
        sorted_names = sorted(students_display)
        default_idx = sorted_names.index(default_name) if default_name in sorted_names else 0
        selected_student = st.selectbox("👩‍🎓 Student", sorted_names, index=default_idx)

        # Books
        available_books = books[books["Status"] == "Available"]["Book Title"].dropna().str.strip().tolist() \
            if "Status" in books.columns else []
        selected_book = st.selectbox("📚 Book Title", sorted(available_books), placeholder="Scan or type book title...")
        days = st.slider("Borrow Days", 1, 30, 14)

        if st.button("✅ Confirm Borrow"):
            if selected_student and selected_book:
                now = datetime.now()
                due = now + timedelta(days=days)
                new_row = {
                    "Student": selected_student,
                    "Book Title": selected_book,
                    "Date Borrowed": now.strftime("%Y-%m-%d %H:%M:%S"),
                    "Due Date": due.strftime("%Y-%m-%d %H:%M:%S"),
                    "Returned": "No"
                }
                logs = _append_row(logs, new_row)
                save_logs(logs)

                if "Status" in books.columns:
                    books.loc[books["Book Title"] == selected_book, "Status"] = "Borrowed"
                    save_books(books)

                st.success(f"{selected_book} borrowed by {selected_student}. Due on {due.date()}")

    # ---------------------- Return ----------------------
    with tabs[1]:
        st.subheader("Return a Book")
        if logs.empty or "Returned" not in logs.columns:
            st.info("No books currently borrowed.")
        else:
            open_logs = logs[logs["Returned"].str.lower() == "no"].copy()
            if open_logs.empty:
                st.info("No books currently borrowed.")
            else:
                open_logs["Label"] = open_logs["Student"] + " - " + open_logs["Book Title"]
                selected_return = st.selectbox("Choose to Return", open_logs["Label"])
                if st.button("📦 Mark as Returned"):
                    row = open_logs[open_logs["Label"] == selected_return].iloc[0]
                    idx = logs[
                        (logs["Student"] == row["Student"]) &
                        (logs["Book Title"] == row["Book Title"]) &
                        (logs["Date Borrowed"] == row["Date Borrowed"])
                    ].index
                    logs.loc[idx, "Returned"] = "Yes"
                    save_logs(logs)

                    if "Status" in books.columns:
                        books.loc[books["Book Title"] == row["Book Title"], "Status"] = "Available"
                        save_books(books)

                    st.success(f"{row['Book Title']} returned by {row['Student']}")

    # ---------------------- Add ----------------------
    with tabs[2]:
        st.subheader("➕ Add Student or Book")
        opt = st.radio("Add:", ["Student", "Book"], horizontal=True)
        if opt == "Student":
            code = st.text_input("Student Code (e.g., 001)")
            name = st.text_input("First Name")
            surname = st.text_input("Surname")
            gender = st.selectbox("Gender", ["Boy", "Girl"])
            if st.button("Add Student"):
                new_row = {
                    "Code": (code or "").strip(),
                    "Name": (name or "").strip(),
                    "Surname": (surname or "").strip(),
                    "Gender": gender
                }
                students = _append_row(students, new_row)
                save_students(students)
                st.success("Student added.")
        else:
            title = st.text_input("Book Title")
            author = st.text_input("Author")
            book_id = st.text_input("Book ID")
            if st.button("Add Book"):
                new_row = {"Book ID": book_id.strip(), "Book Title": title.strip(), "Author": author.strip(), "Status": "Available"}
                books = _append_row(books, new_row)
                save_books(books)
                st.success("Book added.")

    # ---------------------- Delete ----------------------
    with tabs[3]:
        st.subheader("🗑️ Delete Student or Book")
        opt = st.radio("Delete:", ["Student", "Book"], horizontal=True)
        if opt == "Student":
            student_list = sorted((students["Name"] + " " + students["Surname"]).tolist())
            to_delete = st.selectbox("Select student to delete", student_list)
            if st.button("Delete Student"):
                # Split on the LAST space so first names can include spaces
                name_part, surname_part = to_delete.rsplit(" ", 1)
                mask = (students["Name"] == name_part) & (students["Surname"] == surname_part)
                students = students[~mask]
                save_students(students)
                st.success("Student deleted.")
        else:
            book_titles = sorted(books["Book Title"].dropna().tolist()) if "Book Title" in books.columns else []
            to_delete = st.selectbox("Select book to delete", book_titles)
            if st.button("Delete Book"):
                books = books[books["Book Title"] != to_delete]
                save_books(books)
                st.success("Book deleted.")

    # ---------------------- Logs ----------------------
    with tabs[4]:
        st.subheader("📜 Borrow Log")
        logs_display = logs.copy()

        now = datetime.now()
        if not logs_display.empty:
            if "Due Date" in logs_display.columns:
                logs_display["Due Date"] = pd.to_datetime(logs_display["Due Date"], errors="coerce")
            if "Returned" in logs_display.columns:
                logs_display["Returned"] = logs_display["Returned"].fillna("No")

            def days_overdue(row):
                if "Returned" in row and "Due Date" in row and pd.notna(row["Due Date"]):
                    return (now - row["Due Date"]).days if str(row["Returned"]).lower() == "no" and row["Due Date"] < now else 0
                return 0

            logs_display["Days Overdue"] = logs_display.apply(days_overdue, axis=1)

            def highlight_overdue(row):
                if str(row.get("Returned", "no")).lower() == "no" and pd.notna(row.get("Due Date")) and row["Due Date"] < now:
                    return ['background-color: #ffdddd'] * len(row)
                return [''] * len(row)

            st.dataframe(logs_display.style.apply(highlight_overdue, axis=1))
            st.download_button("Download CSV", logs_display.to_csv(index=False), file_name="Borrow_log.csv", mime="text/csv")
        else:
            st.info("No logs yet.")

    # ---------------------- Analytics ----------------------
    with tabs[5]:
        st.subheader("📈 Library Analytics Dashboard")
        if logs.empty:
            st.info("No data available yet to display analytics.")
        else:
            # Top 5 Most Borrowed Books
            if "Book Title" in logs.columns:
                top_books = logs["Book Title"].value_counts().nlargest(5).reset_index()
                top_books.columns = ["Book Title", "Borrow Count"]
                fig1 = px.bar(top_books, x="Book Title", y="Borrow Count", title="📚 Top 5 Most Borrowed Books")
                st.plotly_chart(fig1)

            # Active vs Inactive Students
            active_students = logs["Student"].value_counts() if "Student" in logs.columns else pd.Series(dtype=int)
            active_count = active_students[active_students > 0].count()
            inactive_count = max(0, len(students) - active_count)
            pie_df = pd.DataFrame({"Status": ["Active", "Inactive"], "Count": [active_count, inactive_count]})
            fig2 = px.pie(pie_df, values="Count", names="Status", title="👩‍🎓 Active vs Inactive Students")
            st.plotly_chart(fig2)

            # Overdue Books
            today = datetime.now()
            logs_od = logs.copy()
            if "Due Date" in logs_od.columns:
                logs_od["Due Date"] = pd.to_datetime(logs_od["Due Date"], errors="coerce")
            if "Returned" in logs_od.columns:
                overdue = logs_od[(logs_od["Returned"].str.lower() == "no") & (logs_od["Due Date"] < today)]
                if not overdue.empty:
                    overdue = overdue.copy()
                    overdue["Days Overdue"] = (today - overdue["Due Date"]).dt.days
                    st.warning(f"⏰ {len(overdue)} books overdue!")
                    st.dataframe(overdue[["Student", "Book Title", "Due Date", "Days Overdue"]])
                else:
                    st.success("✅ No overdue books!")

            # Borrowing Trends
            logs_trend = logs.copy()
            if "Date Borrowed" in logs_trend.columns:
                logs_trend["Date Borrowed"] = pd.to_datetime(logs_trend["Date Borrowed"], errors="coerce")
                trend = logs_trend.dropna(subset=["Date Borrowed"]).groupby(
                    logs_trend["Date Borrowed"].dt.to_period("M")
                ).size().reset_index(name="Borrows")
                trend["Month"] = trend["Date Borrowed"].astype(str)
                fig4 = px.line(trend, x="Month", y="Borrows", title="📈 Borrowing Trends Over Time")
                st.plotly_chart(fig4)

# ------------------------------------------------------
# Run App
# ------------------------------------------------------
if __name__ == "__main__":
    if "logged_in" not in st.session_state or not st.session_state["logged_in"]:
        login_form()
    else:
        st.sidebar.success(f"🔓 Logged in as: {st.session_state['username']}")
        if st.sidebar.button("🚪 Logout"):
            st.session_state.clear()
            st.rerun()
        else:
            main()
