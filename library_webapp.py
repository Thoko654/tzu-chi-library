# library_webapp.py
# Streamlit app for Tzu Chi Library
# - Multiple copies per title supported (each CSV row is a copy)
# - One open borrow per student enforced
# - Scanner mode: student code + book barcode
# - Optional GitHub sync (set st.secrets["github_store"] in Streamlit)

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

def _canon(s: str) -> str:
    """Normalize scanner inputs: keep alphanumerics, strip spaces, uppercase."""
    if s is None:
        return ""
    s = str(s)
    return "".join(ch for ch in s if ch.isalnum()).upper()

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
    if not _gh_enabled():
        return None
    try:
        token, repo, branch, base_path = _gh_conf()
        rel_path = f"{base_path}/{repo_rel_path}".lstrip("/")
        raw_url = f"https://raw.githubusercontent.com/{repo}/{branch}/{rel_path}"
        r = requests.get(raw_url, timeout=20)
        if r.status_code == 200:
            return r.content
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
        pd.DataFrame(columns=["Book ID", "Book Title", "Author", "Status", "Barcode"]).to_csv(BOOKS_CSV, index=False, encoding="utf-8")
    if not os.path.exists(LOG_CSV):
        pd.DataFrame(columns=["Student", "Book Title", "Book ID", "Date Borrowed", "Due Date", "Returned", "Barcode", "Copy Key"]).to_csv(LOG_CSV, index=False, encoding="utf-8")

    # pull from GitHub only if the local files are still empty
    if _gh_enabled():
        if _file_rowcount(STUDENT_CSV) == 0:
            _refresh_from_github(STUDENT_CSV, "Student_records.csv")
        if _file_rowcount(BOOKS_CSV) == 0:
            _refresh_from_github(BOOKS_CSV, "Library_books.csv")
        if _file_rowcount(LOG_CSV) == 0:
            _refresh_from_github(LOG_CSV, "Borrow_log.csv")

    # migrate legacy repo-root files → data/ if still empty
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
            df = pd.read_csv(legacy_logs, dtype=str, on_bad_lines="skip").fillna("")
            for c in df.columns:
                df[c] = df[c].astype(str).str.strip()
            if "Book ID" not in df.columns: df["Book ID"] = ""
            if "Returned" not in df.columns: df["Returned"] = "No"
            if "Barcode" not in df.columns: df["Barcode"] = ""
            if "Copy Key" not in df.columns: df["Copy Key"] = ""
            df.to_csv(LOG_CSV, index=False, encoding="utf-8")
        except Exception as e:
            st.warning(f"Could not migrate legacy logs file: {e}")

# ---------- books helpers ----------
def _normalize_barcode_headers(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    rename_barcode = {
        "Barcode/ISBN": "Barcode",
        "Barcode / ISBN": "Barcode",
        "ISBN": "Barcode",
        "Bar Code": "Barcode",
        "BARCODE": "Barcode",
    }
    for k, v in rename_barcode.items():
        if k in df.columns and "Barcode" not in df.columns:
            df = df.rename(columns={k: v})
    if "Barcode" not in df.columns:
        df["Barcode"] = ""
    return df

def _apply_book_helpers(df: pd.DataFrame) -> pd.DataFrame:
    """Make sure helper columns exist and are consistent; DO NOT show them in UI."""
    df = df.copy()
    df.columns = df.columns.str.strip()
    df = _normalize_barcode_headers(df)
    df = df.loc[:, ~df.columns.str.match(r"^Unnamed")]

    if "Status" not in df.columns:
        df["Status"] = "Available"
    for c in df.columns:
        df[c] = df[c].astype(str).str.strip()

    df["Status"] = (
        df["Status"].str.lower()
        .map({"available":"Available","borrowed":"Borrowed","out":"Borrowed","issued":"Borrowed","":"Available"})
        .fillna("Available")
    )

    # Row UID – stable per row; create for rows that don't have it
    if "_ROW_UID" not in df.columns:
        df["_ROW_UID"] = ""
    # assign any missing uids with an incrementing sequence
    current_max = 0
    try:
        current_max = pd.to_numeric(df["_ROW_UID"], errors="coerce").max()
        if pd.isna(current_max):
            current_max = 0
    except Exception:
        current_max = 0
    need_uid = df["_ROW_UID"].astype(str).str.strip() == ""
    n = need_uid.sum()
    if n:
        new_vals = list(range(int(current_max) + 1, int(current_max) + 1 + n))
        df.loc[need_uid, "_ROW_UID"] = [str(v) for v in new_vals]

    df["_BARCODE_CANON"] = df["Barcode"].map(_canon)
    df["_COPY_KEY"] = (
        df.get("Book ID", "").astype(str) + "|" +
        df["Barcode"].astype(str) + "|" +
        df["_ROW_UID"].astype(str)
    )

    # Keep rows that have at least a title or a barcode
    if "Book Title" in df.columns:
        keep = (df["Book Title"].astype(str).str.strip() != "") | (df["Barcode"].astype(str).str.strip() != "")
        df = df[keep].copy()

    return df

def load_students():
    df = pd.read_csv(STUDENT_CSV, dtype=str).fillna("")
    df.columns = df.columns.str.strip()
    df = df.rename(columns={"Boy / Girl":"Gender","First Name":"Name","Last Name":"Surname","Student Code":"Code","ID":"Code"})
    if "Code" not in df.columns: df["Code"] = ""
    for c in df.columns: df[c] = df[c].astype(str).str.strip()
    df["_CODE_CANON"] = df["Code"].map(_canon)
    df = df.loc[:, ~df.columns.str.match(r"^Unnamed")]
    return df

def load_books():
    raw = pd.read_csv(BOOKS_CSV, dtype=str, on_bad_lines="skip").fillna("")
    return _apply_book_helpers(raw)

def load_logs():
    df = pd.read_csv(LOG_CSV, dtype=str, on_bad_lines="skip").fillna("")
    df.columns = df.columns.str.strip()

    rename_map = {
        "Book Tittle": "Book Title",
        "Book Title ": "Book Title",
        "Date Due": "Due Date",
        "Borrow Date": "Date Borrowed",
        "Borrowed Date": "Date Borrowed",
        "Return": "Returned",
        "Is Returned": "Returned",
        "Barcode/ISBN": "Barcode",
        "ISBN": "Barcode",
        "CopyKey": "Copy Key",
        "CopyKey ": "Copy Key",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})
    df = df.loc[:, ~df.columns.duplicated()].copy()

    for c in ["Student","Book Title","Book ID","Date Borrowed","Due Date","Returned"]:
        if c not in df.columns:
            df[c] = ""
    if "Barcode" not in df.columns:
        df["Barcode"] = ""
    if "Copy Key" not in df.columns:
        df["Copy Key"] = ""

    for c in df.columns:
        df[c] = df[c].astype(str).str.strip()

    df["Returned"] = df["Returned"].str.lower().map(
        {"yes":"Yes","y":"Yes","true":"Yes","1":"Yes","no":"No","n":"No","false":"No","0":"No"}
    ).fillna("No")

    key_cols = ["Student","Book Title","Book ID","Barcode","Copy Key"]
    mask_all_blank = df[key_cols].apply(lambda s: s.astype(str).str.strip() == "").all(axis=1)
    df = df[~mask_all_blank].reset_index(drop=True)
    return df

def save_logs(df):
    cols = ["Student", "Book Title", "Book ID", "Barcode", "Copy Key", "Date Borrowed", "Due Date", "Returned"]
    out = df.copy()
    for c in cols:
        if c not in out.columns:
            out[c] = ""
    out = out[cols]
    out.to_csv(LOG_CSV, index=False, encoding="utf-8")
    if _gh_enabled():
        try:
            _gh_put_csv(LOG_CSV, "Borrow_log.csv", "Update Borrow_log.csv via Streamlit app")
        except Exception as e:
            st.error(f"GitHub sync failed: {e}")

def save_students(df):
    out = df.drop(columns=["_CODE_CANON"], errors="ignore").copy()
    out.to_csv(STUDENT_CSV, index=False, encoding="utf-8")
    if _gh_enabled():
        try:
            _gh_put_csv(STUDENT_CSV, "Student_records.csv", "Update Student_records.csv via Streamlit app")
        except Exception as e:
            st.error(f"GitHub sync failed: {e}")

def save_books(df):
    # Ensure helper columns up-to-date, then save
    out = _apply_book_helpers(df)
    out.to_csv(BOOKS_CSV, index=False, encoding="utf-8")
    if _gh_enabled():
        try:
            _gh_put_csv(BOOKS_CSV, "Library_books.csv", "Update Library_books.csv via Streamlit app")
        except Exception as e:
            st.error(f"GitHub sync failed: {e}")

# ======================================================
# Catalog↔Log sync helper
# ======================================================
def sync_missing_open_logs(books_df: pd.DataFrame, logs_df: pd.DataFrame):
    borrowed_copies = books_df.loc[books_df["Status"].str.lower()=="borrowed", ["Book Title","Book ID","Barcode","_COPY_KEY"]].copy()
    open_copy_keys = set(logs_df.loc[logs_df["Returned"].str.lower()=="no", "Copy Key"].astype(str).str.strip())
    to_create = borrowed_copies[~borrowed_copies["_COPY_KEY"].isin(open_copy_keys)]

    if to_create.empty:
        return logs_df, []

    now = datetime.now()
    due = now + timedelta(days=14)

    new_rows = []
    for _, r in to_create.iterrows():
        new_rows.append({
            "Student": "",
            "Book Title": r.get("Book Title",""),
            "Book ID": r.get("Book ID",""),
            "Barcode": r.get("Barcode",""),
            "Copy Key": r.get("_COPY_KEY",""),
            "Date Borrowed": now.strftime("%Y-%m-%d %H:%M:%S"),
            "Due Date": due.strftime("%Y-%m-%d %H:%M:%S"),
            "Returned": "No",
        })
    merged = pd.concat([logs_df, pd.DataFrame(new_rows)], ignore_index=True)
    return merged, to_create["_COPY_KEY"].tolist()

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

    # ---------- Top metrics ----------
    total_books     = len(books)
    available_count = (books["Status"].str.lower() == "available").sum()
    borrowed_open   = (books["Status"].str.lower() == "borrowed").sum()

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Students", len(students))
    c2.metric("Books", int(total_books))
    c3.metric("Available", int(available_count))
    c4.metric("Borrowed (open)", int(borrowed_open))

    # ---------- Health check ----------
    logged_open_keys = set(logs.loc[logs["Returned"].str.lower()=="no","Copy Key"].astype(str))
    borrowed_copies  = books.loc[books["Status"].str.lower()=="borrowed","_COPY_KEY"].astype(str)
    missing_log_keys = sorted(set(borrowed_copies) - logged_open_keys)

    avail_keys = set(books.loc[books["Status"].str.lower()=="available","_COPY_KEY"].astype(str))
    open_but_avail_keys = sorted(logged_open_keys & avail_keys)

    with st.expander("⚠️ Status health check"):
        if missing_log_keys:
            st.warning("Copies **Borrowed** in Catalog but no open log:")
            st.write(missing_log_keys[:50])
        if open_but_avail_keys:
            st.warning("Copies have an **open log** but Catalog says **Available**:")
            st.write(open_but_avail_keys[:50])
        if st.button("🔗 Create open logs for borrowed copies (quick sync)"):
            logs_new, created = sync_missing_open_logs(books, logs)
            if created:
                save_logs(logs_new)
                st.success(f"Created {len(created)} open log(s).")
                st.rerun()
            else:
                st.info("Nothing to sync — all borrowed copies already have open logs.")

    # ---- Tabs ----
    tabs = st.tabs([
        "📖 Borrow",
        "📦 Return",
        "📋 Borrowed now",
        "➕ Add",
        "🗑️ Delete",
        "📘 Catalog",
        "📜 Logs",
        "📈 Analytics",
    ])

    # ---------------------- Borrow ----------------------
    with tabs[0]:
        st.subheader("Borrow a Book")

        # ======== Scanner mode ========
        st.markdown("**Scanner mode (optional)** — scan or type, press Enter.")
        sc1, sc2 = st.columns(2)
        scan_student_code = sc1.text_input("Scan/enter Student Code", key="scan_student_code").strip()
        scan_book_barcode = sc2.text_input("Scan/enter Book Barcode", key="scan_book_barcode").strip()

        selected_student = ""
        if scan_student_code:
            hit = students.loc[students["_CODE_CANON"] == _canon(scan_student_code)]
            if not hit.empty:
                selected_student = (hit.iloc[0]["Name"] + " " + hit.iloc[0]["Surname"]).strip()
                st.success(f"Student found: {selected_student}")
            else:
                st.error("Student code not found.")

        selected_copy_key = ""
        selected_copy_label = ""
        if scan_book_barcode:
            canon_bar = _canon(scan_book_barcode)
            cand = books[(books["_BARCODE_CANON"] == canon_bar) & (books["Status"].str.lower()=="available")]
            if not cand.empty:
                r = cand.iloc[0]
                selected_copy_key = r["_COPY_KEY"]
                selected_copy_label = f"{r['Book Title']} [ID:{r.get('Book ID','') or '-'} | BC:{r.get('Barcode','') or '-'}]"
                st.success(f"Book found: {selected_copy_label}")
            else:
                st.error("Available copy with this barcode not found.")

        st.markdown("---")

        student_names = (students["Name"].str.strip() + " " + students["Surname"].str.strip()).tolist()
        sel_student_dropdown = st.selectbox("👩‍🎓 Pick Student (optional if you scanned)", [""] + sorted(set(student_names)), index=0)

        avail = books[books["Status"].str.lower()=="available"].copy()
        if avail.empty:
            st.info("No available copies right now.")
        else:
            avail["_label"] = (
                avail["Book Title"].astype(str)
                + "  [ID:" + avail.get("Book ID","").astype(str).replace("", "-", regex=False)
                + " | BC:" + avail.get("Barcode","").astype(str).replace("", "-", regex=False) + "]"
            )
        sel_copy_label = st.selectbox("📚 Pick Book Copy (optional if you scanned)", [""] + avail["_label"].tolist(), index=0)

        final_student = selected_student or sel_student_dropdown
        if sel_copy_label:
            if sel_copy_label in avail["_label"].values:
                selected_copy_key = avail.loc[avail["_label"] == sel_copy_label, "_COPY_KEY"].iloc[0]

        days = st.slider("Borrow Days", 1, 30, 14)
        allow_override = st.checkbox("Allow borrow even if this copy is marked Borrowed (back capture)")

        if st.button("✅ Confirm Borrow"):
            if not final_student or not selected_copy_key:
                st.error("Please provide both a student (scan or pick) and a book copy (scan or pick).")
            else:
                # HARD CHECK: one open borrow per student
                has_open = logs[(logs["Student"] == final_student) & (logs["Returned"].str.lower() == "no")]
                if not has_open.empty:
                    st.error(f"🚫 {final_student} already has a book out. Please return it first.")
                    st.stop()

                row = books[books["_COPY_KEY"] == selected_copy_key]
                if row.empty:
                    st.error("Could not locate the selected copy in catalog.")
                    st.stop()

                current_status = row.iloc[0]["Status"]
                if current_status == "Borrowed" and not allow_override:
                    st.error("This copy is marked as Borrowed. Tick the override checkbox to capture anyway.")
                    st.stop()

                now = datetime.now()
                due = now + timedelta(days=days)
                book_title = row.iloc[0]["Book Title"]
                book_id    = row.iloc[0].get("Book ID","")
                barcode    = row.iloc[0].get("Barcode","")
                copy_key   = row.iloc[0]["_COPY_KEY"]

                logs_latest = load_logs()
                new_row = {
                    "Student": final_student,
                    "Book Title": book_title,
                    "Book ID": book_id,
                    "Barcode": barcode,
                    "Copy Key": copy_key,
                    "Date Borrowed": now.strftime("%Y-%m-%d %H:%M:%S"),
                    "Due Date": due.strftime("%Y-%m-%d %H:%M:%S"),
                    "Returned": "No",
                }
                logs_latest = df_append(logs_latest, new_row)
                save_logs(logs_latest)

                books.loc[books["_COPY_KEY"] == copy_key, "Status"] = "Borrowed"
                save_books(books)

                st.success(f"✅ Borrowed: “{book_title}” to {final_student}. Due on {due.date()}")
                st.rerun()

    # ---------------------- Return ----------------------
    with tabs[1]:
        st.subheader("Return a Book")
        logs = load_logs()
        if logs.empty or "Returned" not in logs.columns:
            st.info("No books currently borrowed.")
        else:
            barcode_return = st.text_input("Scan/enter Book Barcode to return (optional)").strip()
            open_logs_view = logs[logs["Returned"].str.lower() == "no"].copy()

            hits = pd.DataFrame()
            if barcode_return:
                canon_bar = _canon(barcode_return)
                hits = open_logs_view[open_logs_view["Barcode"].map(_canon) == canon_bar]
                if not hits.empty:
                    st.success(f"Matched copy by barcode: {hits.iloc[0]['Book Title']}")

            if open_logs_view.empty:
                st.info("No books currently borrowed.")
            else:
                open_logs_view["Label"] = (
                    open_logs_view["Student"] + " | " +
                    open_logs_view["Book Title"] + " | " +
                    open_logs_view["Date Borrowed"]
                )
                default_idx = 0
                if not hits.empty:
                    target = hits.iloc[0]["Label"]
                    try:
                        default_idx = open_logs_view["Label"].tolist().index(target)
                    except Exception:
                        default_idx = 0

                selected_return = st.selectbox("Choose to Return", open_logs_view["Label"], index=default_idx)
                if st.button("📦 Mark as Returned"):
                    row = open_logs_view[open_logs_view["Label"] == selected_return].iloc[0]
                    mask = (
                        (logs["Student"] == row["Student"]) &
                        (logs["Book Title"] == row["Book Title"]) &
                        (logs["Date Borrowed"] == row["Date Borrowed"])
                    )
                    logs.loc[mask, "Returned"] = "Yes"
                    save_logs(logs)

                    copy_key = row.get("Copy Key","")
                    if copy_key:
                        books = load_books()
                        books.loc[books["_COPY_KEY"] == copy_key, "Status"] = "Available"
                        save_books(books)
                    st.success(f"Returned: {row['Book Title']} from {row['Student']}")
                    st.rerun()

    # ---------------------- Borrowed now ----------------------
    with tabs[2]:
        st.subheader("📋 Borrowed now (not returned)")
        logs_live = load_logs()
        if logs_live.empty or "Returned" not in logs_live.columns:
            st.info("No borrow records yet.")
        else:
            open_df = logs_live[logs_live["Returned"].str.lower() == "no"].copy()
            if open_df.empty:
                st.success("✅ No books currently out.")
            else:
                show_cols = ["Student","Book Title","Book ID","Barcode","Date Borrowed","Due Date"]
                for c in show_cols:
                    if c not in open_df.columns:
                        open_df[c] = ""

                c1, c2 = st.columns(2)
                with c1:
                    sel_student = st.selectbox("Filter by student (optional)", ["(All)"] + sorted([s for s in open_df["Student"].astype(str).str.strip().unique() if s]), index=0)
                with c2:
                    sel_book = st.selectbox("Filter by book (optional)", ["(All)"] + sorted(open_df["Book Title"].astype(str).str.strip().unique().tolist()), index=0)

                filt = open_df.copy()
                if sel_student != "(All)":
                    filt = filt[filt["Student"].astype(str).str.strip() == sel_student]
                if sel_book != "(All)":
                    filt = filt[filt["Book Title"].astype(str).str.strip() == sel_book]

                now_ = datetime.now()
                filt["Due Date"] = pd.to_datetime(filt["Due Date"], errors="coerce")
                def _style(row):
                    if pd.notna(row["Due Date"]) and row["Due Date"] < now_:
                        return ['background-color:#ffefef'] * len(row)
                    return [''] * len(row)

                st.dataframe(filt[show_cols].style.apply(_style, axis=1), use_container_width=True)
                st.download_button("⬇️ Download current borrowers (CSV)", filt[show_cols].to_csv(index=False), "borrowed_now.csv", "text/csv")

    # ---------------------- Add ----------------------
    with tabs[3]:
        st.subheader("➕ Add Student or Book Copy")
        opt = st.radio("Add:", ["Student", "Book copy"], horizontal=True)

        if opt == "Student":
            code = st.text_input("Student Code (e.g., 001)")
            name = st.text_input("First Name")
            surname = st.text_input("Surname")
            gender = st.selectbox("Gender", ["Boy", "Girl"])
            if st.button("Add Student"):
                students_now = load_students()
                students_now = df_append(students_now, {
                    "Code": (code or "").strip(),
                    "Name": (name or "").strip(),
                    "Surname": (surname or "").strip(),
                    "Gender": gender
                })
                save_students(students_now)
                st.success("Student added.")
                st.rerun()
        else:
            title = st.text_input("Book Title")
            author = st.text_input("Author")
            book_id = st.text_input("Book ID")
            barcode  = st.text_input("Barcode / ISBN")
            if st.button("Add Book Copy"):
                if not (title.strip() or barcode.strip()):
                    st.error("Please enter at least a Book Title or a Barcode.")
                else:
                    books_now = load_books()
                    # allocate a new stable row uid
                    try:
                        current_max = pd.to_numeric(books_now["_ROW_UID"], errors="coerce").max()
                        if pd.isna(current_max): current_max = 0
                    except Exception:
                        current_max = 0
                    next_uid = str(int(current_max) + 1)

                    new = {
                        "Book ID": (book_id or "").strip(),
                        "Book Title": title.strip(),
                        "Author": (author or "").strip(),
                        "Status": "Available",
                        "Barcode": (barcode or "").strip(),
                        "_ROW_UID": next_uid
                    }
                    books_now = df_append(books_now, new)
                    # IMPORTANT: rebuild helpers and save (do NOT reload before saving)
                    save_books(books_now)
                    st.success("Book copy added.")
                    st.rerun()

    # ---------------------- Delete ----------------------
    with tabs[4]:
        st.subheader("🗑️ Delete Student or Book Copy")
        opt = st.radio("Delete:", ["Student", "Book copy"], horizontal=True)

        if opt == "Student":
            students_now = load_students()
            student_list = sorted((students_now["Name"] + " " + students_now["Surname"]).str.strip().tolist())
            to_delete = st.selectbox("Select student to delete", [""] + student_list)
            if st.button("Delete Student"):
                if to_delete:
                    parts = to_delete.strip().split()
                    name_part = " ".join(parts[:-1]) if len(parts) > 1 else parts[0]
                    surname_part = parts[-1] if len(parts) > 1 else ""
                    mask = (students_now["Name"] == name_part) & (students_now["Surname"] == surname_part)
                    students_now = students_now[~mask]
                    save_students(students_now)
                    st.success("Student deleted.")
                    st.rerun()
        else:
            books_now = load_books()
            books_now["_label"] = (
                books_now["Book Title"].astype(str)
                + "  [ID:" + books_now.get("Book ID","").astype(str).replace("", "-", regex=False)
                + " | BC:" + books_now.get("Barcode","").astype(str).replace("", "-", regex=False) + "]"
            )
            pick = st.selectbox("Select book copy to delete", [""] + books_now["_label"].tolist())
            if st.button("Delete Book Copy"):
                if pick:
                    key = books_now.loc[books_now["_label"]==pick, "_COPY_KEY"].iloc[0]
                    books_now = books_now[books_now["_COPY_KEY"] != key]
                    save_books(books_now)
                    st.success("Book copy deleted.")
                    st.rerun()

    # ---------------------- Catalog ----------------------
    with tabs[5]:
        st.subheader("📘 Catalog — View & Edit Copies")

        books_now = load_books().copy()
        if books_now.empty:
            st.info("No books yet. Use the ➕ Add tab to add some.")
        else:
            col_f1, col_f2 = st.columns([2, 1])
            search = col_f1.text_input("🔍 Search by title/author/ID/barcode", "")
            only_available = col_f2.checkbox("Show only Available", value=False)

            df = books_now.copy()
            if search.strip():
                q = search.strip().lower()
                df = df[
                    df.get("Book Title", "").str.lower().str.contains(q, na=False)
                    | df.get("Author", "").str.lower().str.contains(q, na=False)
                    | df.get("Book ID", "").str.lower().str.contains(q, na=False)
                    | df.get("Barcode", "").str.lower().str.contains(q, na=False)
                ].copy()
            if only_available:
                df = df[df["Status"].str.lower().eq("available")].copy()

            # we DO NOT show helper columns in the editor
            for c in ["Book ID", "Book Title", "Author", "Status", "Barcode"]:
                if c not in df.columns:
                    df[c] = ""

            # we keep an internal pointer to original rows by index (hidden)
            df["_row_id"] = df.index

            st.caption("Tip: edit cells directly; add/remove rows with the table toolbar. Click **Save changes** to persist.")
            edited = st.data_editor(
                df[["Book ID","Book Title","Author","Status","Barcode","_row_id"]],
                num_rows="dynamic",
                use_container_width=True,
                column_config={
                    "Book ID": {"help":"Optional library ID / internal ID"},
                    "Book Title": {"help":"Required unless Barcode present"},
                    "Author": {"help":"Optional"},
                    "Status": {"help":"Available or Borrowed"},
                    "Barcode": {"help":"Scanner barcode / ISBN"},
                    "_row_id": {"hidden": True},
                },
                key="catalog_editor",
            )

            if st.button("💾 Save changes"):
                updated = books_now.copy()

                # updates to existing rows
                to_update = edited.dropna(subset=["_row_id"]).copy()
                to_update["_row_id"] = to_update["_row_id"].astype(int)
                for _, r in to_update.iterrows():
                    ridx = r["_row_id"]
                    if ridx in updated.index:
                        updated.loc[ridx, "Book ID"] = str(r.get("Book ID","")).strip()
                        updated.loc[ridx, "Book Title"] = str(r.get("Book Title","")).strip()
                        updated.loc[ridx, "Author"] = str(r.get("Author","")).strip()
                        updated.loc[ridx, "Barcode"] = str(r.get("Barcode","")).strip()
                        status = str(r.get("Status","")).strip().lower()
                        updated.loc[ridx, "Status"] = "Borrowed" if status in {"borrowed","out","issued"} else "Available"

                # brand new rows (no _row_id)
                new_rows = edited[edited["_row_id"].isna()]
                if not new_rows.empty:
                    try:
                        current_max = pd.to_numeric(updated["_ROW_UID"], errors="coerce").max()
                        if pd.isna(current_max): current_max = 0
                    except Exception:
                        current_max = 0
                    for _, r in new_rows.iterrows():
                        current_max += 1
                        rec = {
                            "Book ID": str(r.get("Book ID","")).strip(),
                            "Book Title": str(r.get("Book Title","")).strip(),
                            "Author": str(r.get("Author","")).strip(),
                            "Barcode": str(r.get("Barcode","")).strip(),
                            "Status": "Borrowed" if str(r.get("Status","")).strip().lower() in {"borrowed","out","issued"} else "Available",
                            "_ROW_UID": str(int(current_max)),
                        }
                        if rec["Book Title"] or rec["Barcode"]:
                            updated = pd.concat([updated, pd.DataFrame([rec])], ignore_index=True)

                # rebuild helpers & save
                save_books(updated)
                st.success("Catalog saved.")
                st.rerun()

    # ---------------------- Logs ----------------------
    with tabs[6]:
        st.subheader("📜 Borrow Log")

        if st.button("🛠 Clean log columns (fix headers/unnamed)"):
            fixed = load_logs()
            save_logs(fixed)
            st.success("Borrow_log.csv cleaned and normalized.")
            st.rerun()

        logs_now = load_logs()
        if logs_now.empty:
            st.info("No logs yet.")
        else:
            now = datetime.now()
            logs_now["Due Date"] = pd.to_datetime(logs_now["Due Date"], errors="coerce")
            logs_now["Returned"] = logs_now["Returned"].fillna("No")
            logs_now["Days Overdue"] = logs_now.apply(
                lambda row: (now - row["Due Date"]).days
                if str(row["Returned"]).lower() == "no" and pd.notna(row["Due Date"]) and row["Due Date"] < now
                else 0,
                axis=1
            )
            def highlight_overdue(row):
                if str(row.get("Returned","no")).lower() == "no" and pd.notna(row.get("Due Date")) and row["Due Date"] < now:
                    return ['background-color: #ffdddd'] * len(row)
                return [''] * len(row)

            show_cols = ["Student","Book Title","Book ID","Barcode","Copy Key","Date Borrowed","Due Date","Returned","Days Overdue"]
            for c in show_cols:
                if c not in logs_now.columns:
                    logs_now[c] = ""
            st.dataframe(logs_now[show_cols].style.apply(highlight_overdue, axis=1), use_container_width=True)
            st.download_button("Download CSV", logs_now.to_csv(index=False), file_name="Borrow_log.csv", mime="text/csv")

        st.markdown("---")

        # Add / back-capture
        with st.expander("➕ Add / Back-capture a Borrow"):
            students_now = load_students()
            books_now = load_books()
            student_names2 = sorted((students_now["Name"].str.strip() + " " + students_now["Surname"].str.strip()).tolist())
            sel_student = st.selectbox("👩‍🎓 Student", [""] + student_names2, key="add_student")

            books_now["_label"] = (
                books_now["Book Title"].astype(str)
                + "  [ID:" + books_now.get("Book ID","").astype(str).replace("", "-", regex=False)
                + " | BC:" + books_now.get("Barcode","").astype(str).replace("", "-", regex=False) + "]"
            )
            sel_copy = st.selectbox("📚 Book Copy", [""] + books_now["_label"].tolist(), key="add_copy")

            col_a, col_b = st.columns(2)
            d_borrow = col_a.date_input("Date Borrowed", value=datetime.now().date(), key="add_d_borrow")
            t_borrow = col_b.time_input("Time Borrowed", value=datetime.now().time().replace(second=0, microsecond=0), key="add_t_borrow")

            col_c, col_d = st.columns(2)
            d_due = col_c.date_input("Due Date", value=(datetime.now() + timedelta(days=14)).date(), key="add_d_due")
            t_due = col_d.time_input("Due Time", value=datetime.now().time().replace(second=0, microsecond=0), key="add_t_due")

            returned_now = st.checkbox("Mark as returned already?", value=False, key="add_returned")

            if st.button("💾 Save Borrow (back-capture)"):
                if not sel_student or not sel_copy:
                    st.error("Please choose both a student and a book copy.")
                else:
                    # one-open-borrow-per-student
                    has_open = logs_now[(logs_now["Student"]==sel_student) & (logs_now["Returned"].str.lower()=="no")]
                    if not has_open.empty and not returned_now:
                        st.error(f"🚫 {sel_student} already has a book out. Please return it first.")
                        st.stop()

                    r = books_now.loc[books_now["_label"]==sel_copy].iloc[0]
                    new_row = {
                        "Student": sel_student,
                        "Book Title": r["Book Title"],
                        "Book ID": r.get("Book ID",""),
                        "Barcode": r.get("Barcode",""),
                        "Copy Key": r["_COPY_KEY"],
                        "Date Borrowed": _ts(d_borrow, t_borrow),
                        "Due Date": _ts(d_due, t_due),
                        "Returned": "Yes" if returned_now else "No",
                    }
                    logs2 = pd.concat([logs_now, pd.DataFrame([new_row])], ignore_index=True)
                    save_logs(logs2)
                    if not returned_now:
                        books_now.loc[books_now["_COPY_KEY"]==r["_COPY_KEY"], "Status"] = "Borrowed"
                        save_books(books_now)
                    st.success("Back-captured borrow saved.")
                    st.rerun()

        # Edit
        with st.expander("✏️ Edit an Existing Log"):
            if logs_now.empty:
                st.info("Nothing to edit yet.")
            else:
                logs_sel = load_logs()
                label = logs_sel["Student"] + " | " + logs_sel["Book Title"] + " | " + logs_sel["Date Borrowed"]
                sel_label = st.selectbox("Choose a log entry", label.tolist(), key="edit_pick")
                row = logs_sel[label == sel_label].iloc[0]

                st.write("**Edit fields:**")
                e_student = st.text_input("Student", value=row["Student"], key="edit_student")
                e_book    = st.text_input("Book Title", value=row["Book Title"], key="edit_book")
                e_barcode = st.text_input("Barcode", value=row.get("Barcode",""), key="edit_barcode")
                e_copykey = st.text_input("Copy Key", value=row.get("Copy Key",""), key="edit_copykey")

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
                    logs_sel.loc[idx, "Student"]    = e_student.strip()
                    logs_sel.loc[idx, "Book Title"] = e_book.strip()
                    logs_sel.loc[idx, "Barcode"]    = e_barcode.strip()
                    logs_sel.loc[idx, "Copy Key"]   = e_copykey.strip()
                    logs_sel.loc[idx, "Date Borrowed"] = _ts(e_db, e_tb)
                    logs_sel.loc[idx, "Due Date"]      = _ts(e_dd, e_td)
                    logs_sel.loc[idx, "Returned"]      = e_returned
                    save_logs(logs_sel)

                    if e_copykey.strip():
                        books_now = load_books()
                        books_now.loc[books_now["_COPY_KEY"] == e_copykey.strip(), "Status"] = "Available" if e_returned=="Yes" else "Borrowed"
                        save_books(books_now)

                    st.success("Log updated.")
                    st.rerun()

                if colB.button("🗑️ Delete This Log"):
                    idx = logs_sel[label == sel_label].index[0]
                    copy_key = logs_sel.loc[idx, "Copy Key"]
                    logs_sel = logs_sel.drop(index=idx).reset_index(drop=True)
                    save_logs(logs_sel)
                    if copy_key:
                        books_now = load_books()
                        books_now.loc[books_now["_COPY_KEY"] == copy_key, "Status"] = "Available"
                        save_books(books_now)
                    st.warning("Log deleted.")
                    st.rerun()

    # ---------------------- Analytics ----------------------
    with tabs[7]:
        st.subheader("📈 Library Analytics Dashboard")
        logs_a = load_logs()
        if logs_a.empty:
            st.info("No data available yet to display analytics.")
        else:
            if "Book Title" in logs_a.columns:
                top_books = logs_a["Book Title"].value_counts().nlargest(5).reset_index()
                top_books.columns = ["Book Title", "Borrow Count"]
                st.plotly_chart(px.bar(top_books, x="Book Title", y="Borrow Count", title="📚 Top 5 Most Borrowed Books"))

            active_students = logs_a["Student"].value_counts() if "Student" in logs_a.columns else pd.Series(dtype=int)
            active_count = active_students[active_students > 0].count()
            inactive_count = max(0, len(load_students()) - active_count)
            pie_df = pd.DataFrame({"Status": ["Active", "Inactive"], "Count": [active_count, inactive_count]})
            st.plotly_chart(px.pie(pie_df, values="Count", names="Status", title="👩‍🎓 Active vs Inactive Students"))

            today = datetime.now()
            logs_od = logs_a.copy()
            if "Due Date" in logs_od.columns:
                logs_od["Due Date"] = pd.to_datetime(logs_od["Due Date"], errors="coerce")
            if "Returned" in logs_od.columns:
                overdue = logs_od[(logs_od["Returned"].str.lower() == "no") & (logs_od["Due Date"] < today)]
                if not overdue.empty:
                    overdue = overdue.copy()
                    overdue["Days Overdue"] = (today - overdue["Due Date"]).dt.days
                    st.warning(f"⏰ {len(overdue)} books overdue!")
                    st.dataframe(overdue[["Student","Book Title","Due Date","Days Overdue"]])
                else:
                    st.success("✅ No overdue books!")

            logs_trend = logs_a.copy()
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
