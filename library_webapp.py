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
def hash_password(password):
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
    if not os.path.exists(STUDENT_CSV):
        pd.DataFrame(columns=["Name", "Surname", "Boy / Girl"]).to_csv(STUDENT_CSV, index=False)
    if not os.path.exists(BOOKS_CSV):
        pd.DataFrame(columns=["Book ID", "Book Title", "Author", "Status"]).to_csv(BOOKS_CSV, index=False)
    if not os.path.exists(LOG_CSV):
        pd.DataFrame(columns=["Student", "Book Title", "Date Borrowed", "Due Date", "Returned"]).to_csv(LOG_CSV, index=False)

@st.cache_data(ttl=3)
def load_students(): return pd.read_csv(STUDENT_CSV).fillna("")

@st.cache_data(ttl=3)
def load_books():
    df = pd.read_csv(BOOKS_CSV).fillna("")
    if "Status" not in df.columns:
        df["Status"] = "Available"
    return df

@st.cache_data(ttl=3)
def load_logs(): return pd.read_csv(LOG_CSV).fillna("")

def save_students(df): df.to_csv(STUDENT_CSV, index=False); load_students.clear()
def save_books(df): df.to_csv(BOOKS_CSV, index=False); load_books.clear()
def save_logs(df): df.to_csv(LOG_CSV, index=False); load_logs.clear()


# ======================================================
# Main Library System
# ======================================================
def main():
    ensure_files()
    students = load_students()
    books = load_books()
    logs = load_logs()

    logo_path = os.path.join("assets", "chi-logo.png")
    if os.path.exists(logo_path):
        with open(logo_path, "rb") as image_file:
            encoded = base64.b64encode(image_file.read()).decode()
        st.markdown(f"<div style='text-align: center;'><img src='data:image/png;base64,{encoded}' width='200'></div>", unsafe_allow_html=True)

    st.markdown("<h1 style='text-align: center;'>📚 Tzu Chi Foundation — Saturday Tutor Class Library System</h1>", unsafe_allow_html=True)

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Students", len(students))
    col2.metric("Books", books["Book Title"].dropna().str.strip().ne("").sum())
    col3.metric("Available", (books["Status"] == "Available").sum())
    col4.metric("Borrowed (open)", (logs["Returned"].str.lower() == "no").sum())

    tabs = st.tabs(["📖 Borrow", "📦 Return", "➕ Add", "🗑️ Delete", "📜 Logs", "📈 Analytics"])

    # Borrow
    with tabs[0]:
        st.subheader("Borrow a Book")
        student_names = (students["Name"].str.strip() + " " + students["Surname"].str.strip()).dropna().tolist()
        selected_student = st.selectbox("📷 Scan or Type Student Name", sorted(student_names), placeholder="Scan or type student name...")
        available_books = books[books["Status"] == "Available"]["Book Title"].dropna().str.strip().tolist()
        selected_book = st.selectbox("📚 Scan or Type Book Title", sorted(available_books), placeholder="Scan or type book title...")
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
                logs = logs._append(new_row, ignore_index=True)
                save_logs(logs)
                books.loc[books["Book Title"] == selected_book, "Status"] = "Borrowed"
                save_books(books)
                st.success(f"{selected_book} borrowed by {selected_student}. Due on {due.date()}")

    # Return
    with tabs[1]:
        st.subheader("Return a Book")
        open_logs = logs[logs["Returned"].str.lower() == "no"]
        if open_logs.empty:
            st.info("No books currently borrowed.")
        else:
            open_logs["Label"] = open_logs["Student"] + " - " + open_logs["Book Title"]
            selected_return = st.selectbox("Choose to Return", open_logs["Label"])
            if st.button("📦 Mark as Returned"):
                row = open_logs[open_logs["Label"] == selected_return].iloc[0]
                idx = logs[(logs["Student"] == row["Student"]) & (logs["Book Title"] == row["Book Title"]) & (logs["Date Borrowed"] == row["Date Borrowed"])].index
                logs.loc[idx, "Returned"] = "Yes"
                save_logs(logs)
                books.loc[books["Book Title"] == row["Book Title"], "Status"] = "Available"
                save_books(books)
                st.success(f"{row['Book Title']} returned by {row['Student']}")

    # Add
    with tabs[2]:
        st.subheader("➕ Add Student or Book")
        opt = st.radio("Add:", ["Student", "Book"], horizontal=True)
        if opt == "Student":
            name = st.text_input("First Name")
            surname = st.text_input("Surname")
            gender = st.selectbox("Gender", ["Boy", "Girl"])
            if st.button("Add Student"):
                students = students._append({"Name": name, "Surname": surname, "Boy / Girl": gender}, ignore_index=True)
                save_students(students)
                st.success("Student added.")
        else:
            title = st.text_input("Book Title")
            author = st.text_input("Author")
            book_id = st.text_input("Book ID")
            if st.button("Add Book"):
                books = books._append({"Book ID": book_id, "Book Title": title, "Author": author, "Status": "Available"}, ignore_index=True)
                save_books(books)
                st.success("Book added.")

    # Delete
    with tabs[3]:
        st.subheader("🗑️ Delete Student or Book")
        opt = st.radio("Delete:", ["Student", "Book"], horizontal=True)
        if opt == "Student":
            student_list = (students["Name"].str.strip() + " " + students["Surname"].str.strip()).tolist()
            to_delete = st.selectbox("Select student to delete", sorted(student_list))
            if st.button("Delete Student"):
                split = to_delete.split(" ")
                students = students[~((students["Name"] == split[0]) & (students["Surname"] == split[1]))]
                save_students(students)
                st.success("Student deleted.")
        else:
            to_delete = st.selectbox("Select book to delete", sorted(books["Book Title"]))
            if st.button("Delete Book"):
                books = books[books["Book Title"] != to_delete]
                save_books(books)
                st.success("Book deleted.")

    # ========== Logs ==========
    with tabs[4]:
        st.subheader("📜 Borrow Log")

        logs_display = logs.copy()

        # Calculate Days Overdue
        now = datetime.now()
        logs_display["Due Date"] = pd.to_datetime(logs_display["Due Date"], errors='coerce')
        logs_display["Returned"] = logs_display["Returned"].fillna("No")
        logs_display["Days Overdue"] = logs_display.apply(
            lambda row: (now - row["Due Date"]).days if row["Returned"].lower() == "no" and row[
                "Due Date"] < now else 0,
            axis=1
        )

        def highlight_overdue(row):
            if row["Returned"].lower() == "no" and row["Due Date"] < now:
                return ['background-color: #ffdddd'] * len(row)
            else:
                return [''] * len(row)

        st.dataframe(logs_display.style.apply(highlight_overdue, axis=1))

        st.download_button("Download CSV", logs_display.to_csv(index=False), file_name="Borrow_log.csv",
                           mime="text/csv")

    # Analytics
    with tabs[5]:
        st.subheader("📈 Library Analytics Dashboard")
        if logs.empty:
            st.info("No data available yet to display analytics.")
        else:
            # Top 5 Most Borrowed Books
            top_books = logs["Book Title"].value_counts().nlargest(5).reset_index()
            top_books.columns = ["Book Title", "Borrow Count"]
            fig1 = px.bar(top_books, x="Book Title", y="Borrow Count", title="📚 Top 5 Most Borrowed Books")
            st.plotly_chart(fig1)

            # Active vs Inactive Students
            active_students = logs["Student"].value_counts()
            active_count = active_students[active_students > 0].count()
            inactive_count = len(students) - active_count
            pie_df = pd.DataFrame({
                "Status": ["Active", "Inactive"],
                "Count": [active_count, max(0, inactive_count)]
            })
            fig2 = px.pie(pie_df, values="Count", names="Status", title="👩‍🎓 Active vs Inactive Students")
            st.plotly_chart(fig2)

            # Overdue Books
            today = datetime.now()
            logs["Due Date"] = pd.to_datetime(logs["Due Date"])
            overdue = logs[(logs["Returned"].str.lower() == "no") & (logs["Due Date"] < today)]
            if not overdue.empty:
                st.warning(f"⏰ {len(overdue)} books overdue!")
                overdue["Days Overdue"] = (today - overdue["Due Date"]).dt.days
                st.dataframe(overdue[["Student", "Book Title", "Due Date", "Days Overdue"]])
            else:
                st.success("✅ No overdue books!")

            # Borrowing Trends
            logs["Date Borrowed"] = pd.to_datetime(logs["Date Borrowed"])
            trend = logs.groupby(logs["Date Borrowed"].dt.to_period("M")).size().reset_index(name="Borrows")
            trend["Month"] = trend["Date Borrowed"].astype(str)
            fig4 = px.line(trend, x="Month", y="Borrows", title="📈 Borrowing Trends Over Time")
            st.plotly_chart(fig4)

# Run App
if __name__ == "__main__":
    if "logged_in" not in st.session_state or not st.session_state["logged_in"]:
        login_form()
    else:
        st.sidebar.success(f"🔓 Logged in as: {st.session_state['username']}")
        if st.sidebar.button("🚪 Logout"):
            st.session_state.clear()
            st.experimental_rerun()
        else:
            main()
