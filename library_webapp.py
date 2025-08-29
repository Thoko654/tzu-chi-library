import os
import base64
import hashlib
from datetime import datetime, timedelta, date, time
import pandas as pd
import streamlit as st
import plotly.express as px
import requests

# ======================================================
# Config
# ======================================================
st.set_page_config(page_title="Tzu Chi Library", layout="wide")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
os.makedirs(DATA_DIR, exist_ok=True)

STUDENT_CSV = os.path.join(DATA_DIR, "Student_records.csv")
BOOKS_CSV   = os.path.join(DATA_DIR, "Library_books.csv")
LOG_CSV     = os.path.join(DATA_DIR, "Borrow_log.csv")

# ======================================================
# Auth (simple)
# ======================================================
def hash_password(p):
    return hashlib.sha256(p.encode()).hexdigest()

USERS = {
    "admin":   hash_password("admin123"),
    "teacher": hash_password("tzuchi2025"),
}

def verify_login(username, password):
    return USERS.get(username) == hash_password(password)

def login_form():
    st.markdown("## 🔐 Login")
    u = st.text_input("Username")
    p = st.text_input("Password", type="password")
    if st.button("Login"):
        if verify_login(u, p):
            st.session_state["logged_in"] = True
            st.session_state["username"] = u
            st.success("✅ Login successful")
            st.rerun()
        else:
            st.error("❌ Invalid credentials")

# ======================================================
# Helpers
# ======================================================
def _safe_to_datetime(s):
    try:
        dt = pd.to_datetime(s, errors="coerce")
        if pd.isna(dt):
            return None
        return dt.to_pydatetime()
    except Exception:
        return None

def _ts(d: date, t: time):
    return datetime.combine(d, t).strftime("%Y-%m-%d %H:%M:%S")

def df_append(df, row_dict):
    return pd.concat([df, pd.DataFrame([row_dict])], ignore_index=True)

# ======================================================
# GitHub Sync (optional)
# ======================================================
def _gh_enabled() -> bool:
    return "github_store" in st.secrets

def _gh_conf():
    s = st.secrets["github_store"]
    return s.get("token", ""), s.get("repo", ""), s.get("branch", "main"), s.get("base_path", "data")

def _gh_headers():
    token, *_ = _gh_conf()
    return {"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"}

def _gh_paths():
    _, repo, branch, base_path = _gh_conf()
    return repo, branch, base_path

def _gh_get_sha(repo, branch, path):
    url = f"https://api.github.com/repos/{repo}/contents/{path}?ref={branch}"
    r = requests.get(url, headers=_gh_headers(), timeout=20)
    if r.status_code == 200:
        return r.json().get("sha")
    return None

def _gh_put_file(repo, branch, path, content_bytes, message):
    url = f"https://api.github.com/repos/{repo}/contents/{path}"
    sha = _gh_get_sha(repo, branch, path)
    payload = {
        "message": message,
        "branch": branch,
        "content": base64.b64encode(content_bytes).decode("utf-8"),
        "committer": {"name": "Streamlit Bot", "email": "actions@users.noreply.github.com"},
    }
    if sha:
        payload["sha"] = sha
    r = requests.put(url, headers=_gh_headers(), json=payload, timeout=30)
    if r.status_code not in (200, 201):
        try:
            body = r.json()
            msg = body.get("message", "")
            doc = body.get("documentation_url", "")
        except Exception:
            msg, doc = r.text, ""
        raise RuntimeError(
            f"GitHub save failed ({r.status_code}). Repo='{repo}', branch='{branch}', path='{path}'. {msg} {doc}"
        )
    return r.json()

def _gh_self_test():
    if not _gh_enabled():
        return "GitHub OFF", "gray"
    try:
        token, repo, branch, base_path = _gh_conf()
        if not token or not repo:
            return "GitHub secrets incomplete", "red"
        r = requests.get(f"https://api.github.com/repos/{repo}", headers=_gh_headers(), timeout=15)
        if r.status_code != 200:
            return f"GH {r.status_code}: cannot see '{repo}'", "red"
        rb = requests.get(f"https://api.github.com/repos/{repo}/branches/{branch}", headers=_gh_headers(), timeout=15)
        if rb.status_code != 200:
            return f"GH {rb.status_code}: branch '{branch}' missing", "red"
        return f"GitHub OK → {repo}@{branch}/{base_path}", "green"
    except Exception as e:
        return f"GH check failed: {e}", "red"

def _gh_put_csv(local_path, repo_rel_path, message):
    with open(local_path, "rb") as f:
        csv_bytes = f.read()
    repo, branch, base_path = _gh_paths()
    path = f"{base_path}/{repo_rel_path}".lstrip("/")
    return _gh_put_file(repo, branch, path, csv_bytes, message)

def _gh_fetch_bytes(repo_rel_path: str):
    """Return file bytes from GitHub (raw first, then Contents API)."""
    if not _gh_enabled():
        return None
    try:
        token, repo, branch, base_path = _gh_conf()
        rel_path = f"{base_path}/{repo_rel_path}".lstrip("/")

        # Try raw (public)
        raw_url = f"https://raw.githubusercontent.com/{repo}/{branch}/{rel_path}"
        r = requests.get(raw_url, timeout=20)
        if r.status_code == 200:
            return r.content

        # Fallback: Contents API (private)
        api_url = f"https://api.github.com/repos/{repo}/contents/{rel_path}?ref={branch}"
        r = requests.get(api_url, headers=_gh_headers(), timeout=20)
        if r.status_code == 200:
            j = r.json()
            if j.get("encoding") == "base64" and j.get("content"):
                return base64.b64decode(j["content"])
    except Exception:
        pass
    return None

def _refresh_from_github(local_path: str, repo_filename: str):
    """If possible, pull newest CSV from GitHub into local_path."""
    b = _gh_fetch_bytes(repo_filename)
    if b:
        try:
            with open(local_path, "wb") as f:
                f.write(b)
        except Exception:
            pass

# ======================================================
# CSV Utilities (+ one-time migration)
# ======================================================
def _file_rowcount(path: str) -> int:
    if not os.path.exists(path):
        return 0
    try:
        return len(pd.read_csv(path, dtype=str))
    except Exception:
        return 0

def ensure_files():
    # create empty modern files if missing
    if not os.path.exists(STUDENT_CSV):
        pd.DataFrame(columns=["Code", "Name", "Surname", "Gender"]).to_csv(STUDENT_CSV, index=False, encoding="utf-8")
    if not os.path.exists(BOOKS_CSV):
        pd.DataFrame(columns=["Book ID", "Book Title", "Author", "Status"]).to_csv(BOOKS_CSV, index=False, encoding="utf-8")
    if not os.path.exists(LOG_CSV):
        pd.DataFrame(columns=["Student", "Book Title", "Book ID", "Date Borrowed", "Due Date", "Returned"]).to_csv(LOG_CSV, index=False, encoding="utf-8")

    # pull from GitHub only if the local files are still empty
    if _gh_enabled():
        if _file_rowcount(STUDENT_CSV) == 0:
            _refresh_from_github(STUDENT_CSV, "Student_records.csv")
        if _file_rowcount(BOOKS_CSV) == 0:
            _refresh_from_github(BOOKS_CSV, "Library_books.csv")
        if _file_rowcount(LOG_CSV) == 0:
            _refresh_from_github(LOG_CSV, "Borrow_log.csv")

    # migrate legacy files in repo root → data/ (only if still empty)
    legacy_students = os.path.join(BASE_DIR, "Student_records.csv")
    legacy_books    = os.path.join(BASE_DIR, "Library_books.csv")
    legacy_logs     = os.path.join(BASE_DIR, "Borrow_log.csv")

    if _file_rowcount(STUDENT_CSV) == 0 and os.path.exists(legacy_students):
        try:
            df = pd.read_csv(legacy_students, dtype=str).fillna("")
            df = df.rename(columns={"Boy / Girl":"Gender","First Name":"Name","Last Name":"Surname","Student Code":"Code","ID":"Code"})
            for c in df.columns:
                df[c] = df[c].astype(str).str.strip()
            if "Code" not in df.columns: df["Code"] = ""
            if "Gender" not in df.columns: df["Gender"] = ""
            df.to_csv(STUDENT_CSV, index=False, encoding="utf-8")
        except Exception as e:
            st.warning(f"Could not migrate legacy students file: {e}")

    if _file_rowcount(BOOKS_CSV) == 0 and os.path.exists(legacy_books):
        try:
            df = pd.read_csv(legacy_books, dtype=str).fillna("")
            for c in df.columns:
                df[c] = df[c].astype(str).str.strip()
            if "Status" not in df.columns: df["Status"] = "Available"
            df.to_csv(BOOKS_CSV, index=False, encoding="utf-8")
        except Exception as e:
            st.warning(f"Could not migrate legacy books file: {e}")

    if _file_rowcount(LOG_CSV) == 0 and os.path.exists(legacy_logs):
        try:
            df = pd.read_csv(legacy_logs, dtype=str).fillna("")
            for c in df.columns:
                df[c] = df[c].astype(str).str.strip()
            if "Book ID" not in df.columns: df["Book ID"] = ""
            if "Returned" not in df.columns: df["Returned"] = "No"
            df.to_csv(LOG_CSV, index=False, encoding="utf-8")
        except Exception as e:
            st.warning(f"Could not migrate legacy logs file: {e}")

def load_students():
    df = pd.read_csv(STUDENT_CSV, dtype=str).fillna("")
    df.columns = df.columns.str.strip()
    df = df.rename(columns={"Boy / Girl":"Gender","First Name":"Name","Last Name":"Surname","Student Code":"Code","ID":"Code"})
    if "Code" not in df.columns: df["Code"] = ""
    for c in df.columns: df[c] = df[c].astype(str).str.strip()
    df = df.loc[:, ~df.columns.str.match(r"^Unnamed")]
    return df

def load_books():
    df = pd.read_csv(BOOKS_CSV, dtype=str).fillna("")
    df.columns = df.columns.str.strip()
    df = df.loc[:, ~df.columns.str.match(r"^Unnamed")]
    if "Status" not in df.columns: df["Status"] = "Available"
    for c in df.columns: df[c] = df[c].astype(str).str.strip()
    if "Book Title" in df.columns and "Book ID" in df.columns:
        df = df[~((df["Book Title"] == "") & (df["Book ID"] == ""))].copy()
    df["Status"] = (
        df["Status"].str.lower()
        .map({"available":"Available","borrowed":"Borrowed","out":"Borrowed","issued":"Borrowed","":"Available"})
        .fillna("Available")
    )
    return df

# ---------- CLEAN, NORMALIZE, AND LOAD LOGS ----------
def load_logs():
    """Load Borrow_log.csv and aggressively normalize it."""
    df = pd.read_csv(LOG_CSV, dtype=str, on_bad_lines="skip").fillna("")
    df.columns = df.columns.str.strip()

    # Salvage Book Title from any Unnamed:* columns if Book Title is missing/blank
    unnamed_cols = [c for c in df.columns if c.startswith("Unnamed")]
    if "Book Title" not in df.columns:
        df["Book Title"] = ""
    if unnamed_cols:
        tmp = df[unnamed_cols].replace("", pd.NA)
        fill_from = tmp.bfill(axis=1).iloc[:, -1].fillna("")
        df["Book Title"] = df["Book Title"].astype(str).str.strip()
        df.loc[df["Book Title"] == "", "Book Title"] = fill_from.astype(str).str.strip()

    # Drop unnamed columns now that we've salvaged
    df = df.loc[:, ~df.columns.str.match(r"^Unnamed")].copy()

    # Normalize headers
    rename_map = {
        "Book Tittle": "Book Title",
        "Book Title ": "Book Title",
        "Date Due": "Due Date",
        "Borrow Date": "Date Borrowed",
        "Borrowed Date": "Date Borrowed",
        "Return": "Returned",
        "Is Returned": "Returned",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})
    df = df.loc[:, ~df.columns.duplicated()].copy()

    # Ensure required columns
    required = ["Student", "Book Title", "Book ID", "Date Borrowed", "Due Date", "Returned"]
    for c in required:
        if c not in df.columns:
            df[c] = ""
    df = df[required].copy()
    for c in required:
        df[c] = df[c].astype(str).str.strip()

    # Normalize Returned
    df["Returned"] = df["Returned"].str.lower().map(
        {"yes":"Yes","y":"Yes","true":"Yes","1":"Yes","no":"No","n":"No","false":"No","0":"No"}
    ).fillna("No")

    # Drop fully-blank rows
    mask_all_blank = (df[["Student", "Book Title", "Book ID"]]
                      .apply(lambda s: s.astype(str).str.strip() == "")
                      .all(axis=1))
    df = df[~mask_all_blank].reset_index(drop=True)

    return df

def save_logs(df):
    cols = ["Student", "Book Title", "Book ID", "Date Borrowed", "Due Date", "Returned"]
    out = df.copy()
    for c in cols:
        if c not in out.columns:
            out[c] = ""
    out = out[cols]
    out.to_csv(LOG_CSV, index=False, encoding="utf-8")
    if _gh_enabled():
        _gh_put_csv(LOG_CSV, "Borrow_log.csv", "Update Borrow_log.csv via Streamlit app")

def save_students(df):
    df.to_csv(STUDENT_CSV, index=False, encoding="utf-8")
    if _gh_enabled():
        _gh_put_csv(STUDENT_CSV, "Student_records.csv", "Update Student_records.csv via Streamlit app")

def save_books(df):
    df.to_csv(BOOKS_CSV, index=False, encoding="utf-8")
    if _gh_enabled():
        _gh_put_csv(BOOKS_CSV, "Library_books.csv", "Update Library_books.csv via Streamlit app")

# ======================================================
# Catalog↔Log sync helper (borrower-assignment)
# ======================================================
def build_missing_open_logs(books_df: pd.DataFrame, logs_df: pd.DataFrame, title_to_student: dict):
    """
    Build open log rows for any Catalog book with Status=Borrowed that has no open log (Returned = No).
    Uses title_to_student mapping to set Student.
    """
    borrowed_titles = set(
        books_df.loc[books_df["Status"].str.lower() == "borrowed", "Book Title"]
        .astype(str).str.strip()
    )
    open_log_titles = set(
        logs_df.loc[logs_df["Returned"].str.lower() == "no", "Book Title"]
        .astype(str).str.strip()
    )
    to_create = sorted(borrowed_titles - open_log_titles)
    if not to_create:
        return logs_df, []

    now = datetime.now()
    due = now + timedelta(days=14)

    new_rows = []
    for title in to_create:
        bid = ""
        if "Book ID" in books_df.columns:
            sel = books_df.loc[books_df["Book Title"].astype(str).str.strip() == title, "Book ID"]
            if len(sel):
                bid = str(sel.iloc[0]).strip()
        new_rows.append({
            "Student": str(title_to_student.get(title, "")).strip(),
            "Book Title": title,
            "Book ID": bid,
            "Date Borrowed": now.strftime("%Y-%m-%d %H:%M:%S"),
            "Due Date": due.strftime("%Y-%m-%d %H:%M:%S"),
            "Returned": "No",
        })

    merged = pd.concat([logs_df, pd.DataFrame(new_rows)], ignore_index=True)
    return merged, to_create

# ======================================================
# Main App
# ======================================================
def main():
    ensure_files()
    students = load_students()
    books = load_books()
    logs = load_logs()

    # Sidebar
    with st.sidebar:
        st.success(f"🔓 Logged in as: {st.session_state.get('username','')}")
        if st.button("🚪 Logout"):
            st.session_state.clear()
            st.rerun()
        st.markdown("### 🧪 Data health")
        storage = "GitHub + Local CSV" if _gh_enabled() else "Local CSV"
        st.caption(f"Storage: **{storage}**")
        st.caption(f"Students rows: **{len(students)}**")
        st.caption(f"Books rows: **{len(books)}**")
        st.caption(f"Logs rows: **{len(logs)}**")
        status, color = _gh_self_test()
        st.markdown(f"**GitHub:** <span style='color:{color}'>{status}</span>", unsafe_allow_html=True)

    # Logo (optional)
    logo_path = os.path.join("assets", "chi-logo.png")
    if os.path.exists(logo_path):
        with open(logo_path, "rb") as f:
            encoded = base64.b64encode(f.read()).decode()
        st.markdown(
            "<div style='text-align:center; margin-top:8px;'>"
            f"<img src='data:image/png;base64,{encoded}' width='150'>"
            "</div>", unsafe_allow_html=True
        )

    st.markdown("<h1 style='text-align:center;'>📚 Tzu Chi Foundation — Saturday Tutor Class Library System</h1>", unsafe_allow_html=True)

    # ---------- Top metrics from Catalog ----------
    book_titles = books.get("Book Title", pd.Series(dtype=str)).astype(str).str.strip()
    book_status = books.get("Status", pd.Series(dtype=str)).astype(str).str.lower()
    total_books     = (book_titles != "").sum()
    available_count = ((book_titles != "") & (book_status == "available")).sum()
    borrowed_open   = (book_status == "borrowed").sum()

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Students", len(students))
    c2.metric("Books", int(total_books))
    c3.metric("Available", int(available_count))
    c4.metric("Borrowed (open)", int(borrowed_open))

    # ---------- Health check + borrower assignment ----------
    open_logs_df = pd.DataFrame()
    if not logs.empty and "Returned" in logs.columns:
        open_logs_df = logs.loc[logs["Returned"].str.lower() == "no", ["Book Title"]].copy()
        open_logs_df["Book Title"] = open_logs_df["Book Title"].astype(str).str.strip()
    borrowed_in_catalog = books.loc[books["Status"].str.lower() == "borrowed", "Book Title"].astype(str).str.strip()
    missing_log = sorted(set(borrowed_in_catalog) - set(open_logs_df.get("Book Title", pd.Series(dtype=str))))
    available_titles = set(books.loc[books["Status"].str.lower() == "available", "Book Title"].astype(str).str.strip())
    log_but_available = sorted(set(open_logs_df.get("Book Title", pd.Series(dtype=str))) & available_titles)

    if missing_log or log_but_available:
        with st.expander("⚠️ Status health check"):
            if missing_log:
                st.warning("Books **Borrowed** in Catalog but no open log (assign a borrower for each):")

                # Student choices
                if {"Name", "Surname"}.issubset(students.columns):
                    student_choices = sorted((students["Name"].str.strip() + " " + students["Surname"].str.strip()).tolist())
                else:
                    student_choices = []

                with st.form("assign_missing_borrowers"):
                    title_to_student = {}
                    for title in missing_log:
                        title_to_student[title] = st.selectbox(
                            f"Borrower for: {title}",
                            options=[""] + student_choices,
                            index=0,
                            key=f"assign_{title}"
                        )
                    submitted = st.form_submit_button("🔗 Create open logs with selected borrowers")
                if submitted:
                    new_logs, created_titles = build_missing_open_logs(books, load_logs(), title_to_student)
                    if created_titles:
                        save_logs(new_logs)
                        st.success(f"Created {len(created_titles)} open log(s).")
                        st.rerun()
                    else:
                        st.info("Nothing to sync — all borrowed books already have open logs.")

            if log_but_available:
                st.warning("Books have an **open log** but Catalog says **Available**:")
                st.write(log_but_available)
    else:
        st.caption("✅ Catalog and Log statuses look consistent.")

    tabs = st.tabs(["📖 Borrow", "📦 Return", "➕ Add", "🗑️ Delete", "📘 Catalog", "📜 Logs", "📈 Analytics"])

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
                current_status = "Available"
                if not books.empty and "Status" in books.columns:
                    s = books.loc[books["Book Title"] == selected_book, "Status"]
                    if len(s): current_status = s.iloc[0]

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
                    logs2 = df_append(load_logs(), new_row)  # reload to avoid race
                    save_logs(logs2)

                    if "Status" in books.columns:
                        books.loc[books["Book Title"] == selected_book, "Status"] = "Borrowed"
                        save_books(books)

                    st.success(f"{selected_book} borrowed by {selected_student}. Due on {due.date()}")
                    st.rerun()

    # ---------------------- Return ----------------------
    with tabs[1]:
        st.subheader("Return a Book")
        logs = load_logs()
        if logs.empty or "Returned" not in logs.columns:
            st.info("No books currently borrowed.")
        else:
            open_logs_view = logs[logs["Returned"].str.lower() == "no"].copy()
            if open_logs_view.empty:
                st.info("No books currently borrowed.")
            else:
                open_logs_view["Label"] = open_logs_view["Student"] + " - " + open_logs_view["Book Title"]
                selected_return = st.selectbox("Choose to Return", open_logs_view["Label"])
                if st.button("📦 Mark as Returned"):
                    row = open_logs_view[open_logs_view["Label"] == selected_return].iloc[0]
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
                        st.rerun()
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
                    parts = to_delete.strip().split()
                    name_part = " ".join(parts[:-1]) if len(parts) > 1 else parts[0]
                    surname_part = parts[-1] if len(parts) > 1 else ""
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

    # ---------------------- Catalog (View / Edit Books) ----------------------
    with tabs[4]:
        st.subheader("📘 Catalog — View & Edit Books")

        books = load_books().copy()
        if books.empty or "Book Title" not in books.columns:
            st.info("No books yet. Use the ➕ Add tab to add some.")
        else:
            col_f1, col_f2 = st.columns([2, 1])
            search = col_f1.text_input("🔍 Search by title/author/ID", "")
            only_available = col_f2.checkbox("Show only Available", value=False)

            df = books.copy()
            if search.strip():
                q = search.strip().lower()
                df = df[
                    df.get("Book Title", "").str.lower().str.contains(q, na=False)
                    | df.get("Author", "").str.lower().str.contains(q, na=False)
                    | df.get("Book ID", "").str.lower().str.contains(q, na=False)
                ].copy()
            if only_available and "Status" in df.columns:
                df = df[df["Status"].str.lower().eq("available")].copy()

            for c in ["Book ID", "Book Title", "Author", "Status"]:
                if c not in df.columns:
                    df[c] = ""

            df["_row_id"] = df.index
            st.caption("Tip: edit cells directly; add/remove rows with the table toolbar. Click **Save changes** to persist.")
            edited = st.data_editor(
                df[["Book ID", "Book Title", "Author", "Status", "_row_id"]],
                num_rows="dynamic",
                use_container_width=True,
                column_config={
                    "Book ID": {"help": "Optional unique ID/barcode"},
                    "Book Title": {"help": "Required"},
                    "Author": {"help": "Optional"},
                    "Status": {"help": "Available or Borrowed", "required": False, "editable": True},
                    "_row_id": {"hidden": True},
                },
                key="catalog_editor",
            )

            save_col1, _ = st.columns([1, 5])
            if save_col1.button("💾 Save changes"):
                updated = books.copy()

                to_update = edited.dropna(subset=["_row_id"]).copy()
                to_update["_row_id"] = to_update["_row_id"].astype(int)
                for _, r in to_update.iterrows():
                    ridx = r["_row_id"]
                    if ridx in updated.index:
                        updated.loc[ridx, "Book ID"] = str(r.get("Book ID", "")).strip()
                        updated.loc[ridx, "Book Title"] = str(r.get("Book Title", "")).strip()
                        updated.loc[ridx, "Author"] = str(r.get("Author", "")).strip()
                        status = str(r.get("Status", "")).strip().lower()
                        updated.loc[ridx, "Status"] = "Borrowed" if status in {"borrowed","out","issued"} else "Available"

                new_rows = edited[edited["_row_id"].isna() | ~edited["_row_id"].astype("Int64").isin(updated.index)]
                for _, r in new_rows.iterrows():
                    new_rec = {
                        "Book ID": str(r.get("Book ID", "")).strip(),
                        "Book Title": str(r.get("Book Title", "")).strip(),
                        "Author": str(r.get("Author", "")).strip(),
                        "Status": "Borrowed" if str(r.get("Status", "")).strip().lower() in {"borrowed","out","issued"} else "Available",
                    }
                    if new_rec["Book Title"]:
                        updated = pd.concat([updated, pd.DataFrame([new_rec])], ignore_index=True)

                for c in ["Book ID", "Book Title", "Author", "Status"]:
                    if c not in updated.columns:
                        updated[c] = ""
                    updated[c] = updated[c].astype(str).str.strip()

                updated["Status"] = updated["Status"].str.lower().map(
                    {"available": "Available", "borrowed": "Borrowed", "out": "Borrowed", "issued": "Borrowed"}
                ).fillna("Available")

                save_books(updated)
                st.success("Catalog saved.")
                st.rerun()

    # ---------------------- Logs (view/add/edit/delete) ----------------------
    with tabs[5]:
        st.subheader("📜 Borrow Log")

        if st.button("🛠 Clean log columns (fix headers/unnamed)"):
            fixed = load_logs()
            save_logs(fixed)
            st.success("Borrow_log.csv cleaned and normalized.")
            st.rerun()

        logs = load_logs()
        books = load_books()
        students = load_students()

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

            show_cols = ["Student", "Book Title", "Book ID", "Date Borrowed", "Due Date", "Returned", "Days Overdue"]
            st.dataframe(logs_display[show_cols].style.apply(highlight_overdue, axis=1), use_container_width=True)
            st.download_button("Download CSV", logs_display.to_csv(index=False), file_name="Borrow_log.csv", mime="text/csv")

        st.markdown("---")

        # Add / back-capture
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

        # Edit
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

                rb = _safe_to_datetime(row.get("Date Borrowed", "")) or datetime.now()
                rd = _safe_to_datetime(row.get("Due Date", "")) or (datetime.now() + timedelta(days=14))

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
                    logs.loc[idx, "Student"]    = e_student.strip()
                    logs.loc[idx, "Book Title"] = e_book.strip()
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
    with tabs[6]:
        st.subheader("📈 Library Analytics Dashboard")
        logs = load_logs()
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
