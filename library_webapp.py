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
# + One-time migration from legacy files in repo root
# ======================================================
def _file_rowcount(path: str) -> int:
    if not os.path.exists(path):
        return 0
    try:
        return len(pd.read_csv(path, dtype=str))
    except Exception:
        return 0

def ensure_files():
    """Create data files if missing. If the new data file is empty but a legacy
    file exists in the repo root, migrate it into data/ (one-time)."""
    # Create empty, modern-scheme files if missing
    if not os.path.exists(STUDENT_CSV):
        pd.DataFrame(columns=["Code", "Name", "Surname", "Gender"]).to_csv(STUDENT_CSV, index=False, encoding="utf-8")
    if not os.path.exists(BOOKS_CSV):
        pd.DataFrame(columns=["Book ID", "Book Title", "Author", "Status"]).to_csv(BOOKS_CSV, index=False, encoding="utf-8")
    if not os.path.exists(LOG_CSV):
        pd.DataFrame(columns=["Student", "Book Title", "Book ID", "Date Borrowed", "Due Date", "Returned"]).to_csv(LOG_CSV, index=False, encoding="utf-8")

    # Legacy locations (repo root)
    legacy_students = "Student_records.csv"
    legacy_books    = "Library_books.csv"
    legacy_logs     = "Borrow_log.csv"

    # Students migration
    if _file_rowcount(STUDENT_CSV) == 0 and os.path.exists(legacy_students):
        try:
            df = pd.read_csv(legacy_students, dtype=str).fillna("")
            # normalize headers
            rename_map = {"Boy / Girl":"Gender", "First Name":"Name", "Last Name":"Surname", "Student Code":"Code", "ID":"Code"}
            df = df.rename(columns={k:v for k,v in rename_map.items() if k in df.columns})
            for c in df.columns: df[c] = df[c].astype(str).str.strip()
            if "Code" not in df.columns: df["Code"] = ""
            if "Gender" not in df.columns: df["Gender"] = ""
            df.to_csv(STUDENT_CSV, index=False, encoding="utf-8")
        except Exception as e:
            st.warning(f"Could not migrate legacy students file: {e}")

    # Books migration
    if _file_rowcount(BOOKS_CSV) == 0 and os.path.exists(legacy_books):
        try:
            df = pd.read_csv(legacy_books, dtype=str).fillna("")
            for c in df.columns: df[c] = df[c].astype(str).str.strip()
            if "Status" not in df.columns: df["Status"] = "Available"
            df.to_csv(BOOKS_CSV, index=False, encoding="utf-8")
        except Exception as e:
            st.warning(f"Could not migrate legacy books file: {e}")

    # Logs migration
    if _file_rowcount(LOG_CSV) == 0 and os.path.exists(legacy_logs):
        try:
            df = pd.read_csv(legacy_logs, dtype=str).fillna("")
            for c in df.columns: df[c] = df[c].astype(str).str.strip()
            if "Book ID" not in df.columns: df["Book ID"] = ""
            if "Returned" not in df.columns: df["Returned"] = "No"
            df.to_csv(LOG_CSV, index=False, encoding="utf-8")
        except Exception as e:
            st.warning(f"Could not migrate legacy logs file: {e}")

def load_students():
    df = pd.read_csv(STUDENT_CSV, dtype=str).fillna("")
    df.columns = df.columns.str.strip()
    # Backward compatibility
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
    if "Code" not in df.columns:
        df["Code"] = ""
    for c in df.columns:
        df[c] = df[c].astype(str).str.strip()
    return df

def load_books():
    df = pd.read_csv(BOOKS_CSV, dtype=str).fillna("")
    df.columns = df.columns.str.strip()
    if "Status" not in df.columns:
        df["Status"] = "Available"
    for c in df.columns:
        df[c] = df[c].astype(str).str.strip()
    if "Book Title" in df.columns and "Book ID" in df.columns:
        df = df[~((df["Book Title"] == "") & (df["Book ID"] == ""))].copy()
    df["Status"] = (
        df["Status"].str.lower()
        .map({"available":"Available","borrowed":"Borrowed","out":"Borrowed","issued":"Borrowed","":"Available"})
        .fillna("Available")
    )
    return df

def load_logs():
    df = pd.read_csv(LOG_CSV, dtype=str).fillna("")
    df.columns = df.columns.str.strip()
    for c in df.columns:
        df[c] = df[c].astype(str).str.strip()
    return df

def save_students(df): df.to_csv(STUDENT_CSV, index=False, encoding="utf-8")
def save_books(df):   df.to_csv(BOOKS_CSV,   index=False, encoding="utf-8")
def save_logs(df):    df.to_csv(LOG_CSV,    index=False, encoding="utf-8")

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

    # Sidebar status
    with st.sidebar:
        st.success(f"🔓 Logged in as: {st.session_state.get('username','')}")
        if st.button("🚪 Logout"):
            st.session_state.clear()
            st.rerun()
        st.markdown("### 🧪 Data health")
        st.caption(f"Students: `{STUDENT_CSV}` → **{len(students)}** rows")
        st.caption(f"Books: `{BOOKS_CSV}` → **{len(books)}** rows")
        st.caption(f"Logs: `{LOG_CSV}` → **{len(logs)}** rows")

    # Optional logo
    logo_path = os.path.join("assets", "chi-logo.png")
    if os.path.exists(logo_path):
        with open(logo_path, "rb") as image_file:
            encoded = base64.b64encode(image_file.read()).decode()
        st.markdown(
            "<div style='text-align:center; margin-top:8px;'>"
            f"<img src='data:image/png;base64,{encoded}' width='150'>"
            "</div>",
            unsafe_allow_html=True,
        )

    st.markdown("<h1 style='text-align:center;'>📚 Tzu Chi Foundation — Saturday Tutor Class Library System</h1>", unsafe_allow_html=True)

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

        include_borrowed = st.checkbox("Show borrowed books (for back capture / corrections)", value=False)

        # Students
        if {"Name", "Surname"}.issubset(students.columns):
            student_names = (students["Name"].str.strip() + " " + students["Surname"].str.strip()).dropna().tolist()
        else:
            student_names = []
        selected_student = st.selectbox("👩‍🎓 Scan or Type Student Name", sorted(student_names), placeholder="Scan or type student name...")

        # Books
        if include_borrowed:
            book_candidates = books["Book Title"].dropna().str.strip()
        else:
            book_candidates = books.loc[books["Status"] == "Available", "Book Title"].dropna().str.strip()
        selected_book = st.selectbox("📚 Scan or Type Book Title", sorted(book_candidates.unique().tolist()), placeholder="Scan or type book title...")

        days = st.slider("Borrow Days", 1, 30, 14)
        allow_override = st.checkbox("Allow borrow even if this book is marked Borrowed (back capture)")

        if st.button("✅ Confirm Borrow"):
            if selected_student and selected_book:
                # Check status
                if not books.empty and "Status" in books.columns:
                    s = books.loc[books["Book Title"] == selected_book, "Status"]
                    current_status = s.iloc[0] if len(s) else "Available"
                else:
                    current_status = "Available"

                if current_status == "Borrowed" and not allow_override:
                    st.error("This book is marked as Borrowed. Tick the override checkbox to capture anyway.")
                else:
                    now = datetime.now()
                    due = now + timedelta(days=days)
                    book_id = ""
                    if "Book ID" in books.columns:
                        sel = books.loc[books["Book Title"] == selected_book, "Book ID"]
                        if len(sel): book_id = sel.iloc[0]

                    new_row = {
                        "Student": selected_student,
                        "Book Title": selected_book,
                        "Book ID": book_id,
                        "Date Borrowed": now.strftime("%Y-%m-%d %H:%M:%S"),
                        "Due Date": due.strftime("%Y-%m-%d %H:%M:%S"),
                        "Returned": "No"
                    }
                    logs = df_append(logs, new_row)
                    save_logs(logs)

                    # Mark as borrowed in catalog
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
                    if len(idx):
                        logs.loc[idx, "Returned"] = "Yes"
                        save_logs(logs)
                        if "Status" in books.columns:
                            books.loc[books["Book Title"] == row["Book Title"], "Status"] = "Available"
                            save_books(books)
                        st.success(f"{row['Book Title']} returned by {row['Student']}")
                    else:
                        st.error("Could not find the matching borrow record.")

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
                students = df_append(students, {
                    "Code": (code or "").strip(),
                    "Name": (name or "").strip(),
                    "Surname": (surname or "").strip(),
                    "Gender": gender
                })
                save_students(students)
                st.success("Student added.")
        else:
            title = st.text_input("Book Title")
            author = st.text_input("Author")
            book_id = st.text_input("Book ID")
            if st.button("Add Book"):
                if not title.strip():
                    st.error("Please enter a Book Title.")
                else:
                    books = df_append(books, {
                        "Book ID": (book_id or "").strip(),
                        "Book Title": title.strip(),
                        "Author": (author or "").strip(),
                        "Status": "Available"
                    })
                    save_books(books)
                    st.success("Book added.")

    # ---------------------- Delete ----------------------
    with tabs[3]:
        st.subheader("🗑️ Delete Student or Book")
        opt = st.radio("Delete:", ["Student", "Book"], horizontal=True)

        if opt == "Student":
            if {"Name","Surname"}.issubset(students.columns):
                student_list = sorted((students["Name"] + " " + students["Surname"]).str.strip().tolist())
            else:
                student_list = []
            to_delete = st.selectbox("Select student to delete", student_list)
            if st.button("Delete Student"):
                if to_delete:
                    name_part, surname_part = to_delete.rsplit(" ", 1)
                    mask = (students["Name"] == name_part) & (students["Surname"] == surname_part)
                    students = students[~mask]
                    save_students(students)
                    st.success("Student deleted.")
        else:
            titles = sorted(books.get("Book Title", pd.Series(dtype=str)).str.strip().replace("", pd.NA).dropna().unique().tolist())
            to_delete = st.selectbox("Select book to delete", titles)
            if st.button("Delete Book"):
                books = books[books["Book Title"] != to_delete]
                save_books(books)
                st.success("Book deleted.")

    # ---------------------- Logs (View + Add/Back-Capture + Edit/Delete) ----------------------
    with tabs[4]:
        st.subheader("📜 Borrow Log")

        # Always reload latest when changing logs
        logs = load_logs()
        books = load_books()
        students = load_students()

        # ---------- VIEW ----------
        logs_display = logs.copy()
        if logs_display.empty:
            st.info("No logs yet.")
        else:
            now = datetime.now()
            logs_display["Due Date"] = pd.to_datetime(logs_display["Due Date"], errors='coerce')
            logs_display["Returned"] = logs_display["Returned"].fillna("No")
            logs_display["Days Overdue"] = logs_display.apply(
                lambda row: (now - row["Due Date"]).days
                if str(row["Returned"]).lower() == "no" and pd.notna(row["Due Date"]) and row["Due Date"] < now
                else 0,
                axis=1
            )

            def highlight_overdue(row):
                if str(row.get("Returned","no")).lower() == "no" and pd.notna(row.get("Due Date")) and row["Due Date"] < now:
                    return ['background-color: #ffdddd'] * len(row)
                return [''] * len(row)

            st.dataframe(logs_display.style.apply(highlight_overdue, axis=1), use_container_width=True)
            st.download_button("Download CSV", logs_display.to_csv(index=False), file_name="Borrow_log.csv", mime="text/csv")

        st.markdown("---")

        def _ts(d, t):
            return datetime.combine(d, t).strftime("%Y-%m-%d %H:%M:%S")

        # ---------- ADD / BACK-CAPTURE ----------
        with st.expander("➕ Add / Back-capture a Borrow"):
            student_names2 = []
            if {"Name", "Surname"}.issubset(students.columns):
                student_names2 = sorted((students["Name"].str.strip() + " " + students["Surname"].str.strip()).tolist())
            sel_student = st.selectbox("👩‍🎓 Student", student_names2, key="add_student")

            book_titles2 = sorted(books.get("Book Title", pd.Series(dtype=str)).str.strip().replace("", pd.NA).dropna().unique().tolist())
            sel_book = st.selectbox("📚 Book Title", book_titles2, key="add_book")

            col_a, col_b = st.columns(2)
            d_borrow = col_a.date_input("Date Borrowed", value=datetime.now().date(), key="add_d_borrow")
            t_borrow = col_b.time_input("Time Borrowed", value=datetime.now().time().replace(second=0, microsecond=0), key="add_t_borrow")

            col_c, col_d = st.columns(2)
            d_due = col_c.date_input("Due Date", value=(datetime.now() + timedelta(days=14)).date(), key="add_d_due")
            t_due = col_d.time_input("Due Time", value=datetime.now().time().replace(second=0, microsecond=0), key="add_t_due")

            returned_now = st.checkbox("Mark as returned already?", value=False, key="add_returned")

            if st.button("💾 Save Borrow (back-capture)"):
                if not sel_student or not sel_book:
                    st.error("Please choose both a student and a book.")
                else:
                    book_id = ""
                    if "Book ID" in books.columns:
                        sel = books.loc[books["Book Title"] == sel_book, "Book ID"]
                        if len(sel): book_id = sel.iloc[0]

                    new_row = {
                        "Student": sel_student,
                        "Book Title": sel_book,
                        "Book ID": book_id,
                        "Date Borrowed": _ts(d_borrow, t_borrow),
                        "Due Date": _ts(d_due, t_due),
                        "Returned": "Yes" if returned_now else "No",
                    }
                    logs2 = pd.concat([logs, pd.DataFrame([new_row])], ignore_index=True)
                    save_logs(logs2)

                    if not returned_now and "Status" in books.columns:
                        books.loc[books["Book Title"] == sel_book, "Status"] = "Borrowed"
                        save_books(books)

                    st.success("Back-captured borrow saved.")
                    st.rerun()

        # ---------- EDIT EXISTING ----------
        with st.expander("✏️ Edit an Existing Log"):
            if logs.empty:
                st.info("Nothing to edit yet.")
            else:
                logs_sel = logs.copy()
                label = logs_sel["Student"] + " | " + logs_sel["Book Title"] + " | " + logs_sel["Date Borrowed"]
                sel_label = st.selectbox("Choose a log entry", label.tolist(), key="edit_pick")
                row = logs_sel[label == sel_label].iloc[0]

                st.write("**Edit fields:**")
                e_student = st.text_input("Student", value=row["Student"], key="edit_student")
                e_book    = st.text_input("Book Title", value=row["Book Title"], key="edit_book")

                try:
                    rb = pd.to_datetime(row["Date Borrowed"])
                except:
                    rb = datetime.now()
                try:
                    rd = pd.to_datetime(row["Due Date"])
                except:
                    rd = datetime.now() + timedelta(days=14)

                col1, col2 = st.columns(2)
                e_db = col1.date_input("Date Borrowed", value=rb.date(), key="edit_db")
                e_tb = col2.time_input("Time Borrowed", value=rb.time().replace(microsecond=0), key="edit_tb")

                col3, col4 = st.columns(2)
                e_dd = col3.date_input("Due Date", value=rd.date(), key="edit_dd")
                e_td = col4.time_input("Due Time", value=rd.time().replace(microsecond=0), key="edit_td")

                e_returned = st.selectbox("Returned", ["No", "Yes"], index=0 if str(row["Returned"]).lower()=="no" else 1, key="edit_ret")

                colA, colB = st.columns(2)
                if colA.button("💾 Save Changes"):
                    idx = logs_sel[label == sel_label].index[0]
                    logs.loc[idx, "Student"]       = e_student.strip()
                    logs.loc[idx, "Book Title"]    = e_book.strip()
                    if "Book ID" in logs.columns:
                        old_id = logs.loc[idx, "Book ID"]
                        if old_id == "" and "Book ID" in books.columns:
                            match = books.loc[books["Book Title"] == e_book, "Book ID"]
                            logs.loc[idx, "Book ID"] = match.iloc[0] if len(match) else ""
                    logs.loc[idx, "Date Borrowed"] = _ts(e_db, e_tb)
                    logs.loc[idx, "Due Date"]      = _ts(e_dd, e_td)
                    logs.loc[idx, "Returned"]      = e_returned

                    save_logs(logs)

                    if "Status" in books.columns:
                        books.loc[books["Book Title"] == e_book, "Status"] = "Available" if e_returned=="Yes" else "Borrowed"
                        save_books(books)

                    st.success("Log updated.")
                    st.rerun()

                if colB.button("🗑️ Delete This Log"):
                    idx = logs_sel[label == sel_label].index[0]
                    title = logs.loc[idx, "Book Title"]
                    logs = logs.drop(index=idx).reset_index(drop=True)
                    save_logs(logs)
                    if "Status" in books.columns:
                        books.loc[books["Book Title"] == title, "Status"] = "Available"
                        save_books(books)
                    st.warning("Log deleted.")
                    st.rerun()

    # ---------------------- Analytics ----------------------
    with tabs[5]:
        st.subheader("📈 Library Analytics Dashboard")
        if logs.empty:
            st.info("No data available yet to display analytics.")
        else:
            if "Book Title" in logs.columns:
                top_books = logs["Book Title"].value_counts().nlargest(5).reset_index()
                top_books.columns = ["Book Title", "Borrow Count"]
                st.plotly_chart(px.bar(top_books, x="Book Title", y="Borrow Count", title="📚 Top 5 Most Borrowed Books"))

            active_students = logs["Student"].value_counts() if "Student" in logs.columns else pd.Series(dtype=int)
            active_count = active_students[active_students > 0].count()
            inactive_count = max(0, len(load_students()) - active_count)
            pie_df = pd.DataFrame({"Status": ["Active", "Inactive"], "Count": [active_count, inactive_count]})
            st.plotly_chart(px.pie(pie_df, values="Count", names="Status", title="👩‍🎓 Active vs Inactive Students"))

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

            logs_trend = logs.copy()
            if "Date Borrowed" in logs_trend.columns:
                logs_trend["Date Borrowed"] = pd.to_datetime(logs_trend["Date Borrowed"], errors="coerce")
                trend = logs_trend.dropna(subset=["Date Borrowed"]).groupby(
                    logs_trend["Date Borrowed"].dt.to_period("M")
                ).size().reset_index(name="Borrows")
                trend["Month"] = trend["Date Borrowed"].astype(str)
                st.plotly_chart(px.line(trend, x="Month", y="Borrows", title="📈 Borrowing Trends Over Time"))

# ------------------------------------------------------
# Run App
# ------------------------------------------------------
if __name__ == "__main__":
    if "logged_in" not in st.session_state or not st.session_state["logged_in"]:
        login_form()
    else:
        main()
