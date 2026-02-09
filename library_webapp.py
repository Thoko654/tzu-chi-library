# library_webapp.py
# Tzu Chi Library — Streamlit
# - Multiple copies (each CSV row is a copy)
# - One open borrow per person
# - Scanner mode (student code + book barcode)
# - Catalog ↔ Log smart sync (no blank student rows)
# - Optional GitHub CSV sync (via st.secrets["github_store"])

import os
import base64
import hashlib
from datetime import datetime, timedelta, date, time

import pandas as pd
import streamlit as st
import plotly.express as px
import requests

# ===============================
# Basic config / paths
# ===============================
st.set_page_config(page_title="Tzu Chi Library", layout="wide")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
os.makedirs(DATA_DIR, exist_ok=True)

STUDENT_CSV = os.path.join(DATA_DIR, "Student_records.csv")
BOOKS_CSV   = os.path.join(DATA_DIR, "Library_books.csv")
LOG_CSV     = os.path.join(DATA_DIR, "Borrow_log.csv")

# ===============================
# Simple Auth
# ===============================
def hash_password(p: str) -> str:
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

def is_admin() -> bool:
    return st.session_state.get("username", "").strip().lower() == "admin"

# ===============================
# Helpers
# ===============================
def _canon(s: str) -> str:
    """Keep alphanumerics only, uppercase (for scanner normalization)."""
    if s is None:
        return ""
    return "".join(ch for ch in str(s) if ch.isalnum()).upper()

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

def _file_rowcount(path: str) -> int:
    if not os.path.exists(path):
        return 0
    try:
        return len(pd.read_csv(path, dtype=str))
    except Exception:
        return 0

# ===============================
# Optional GitHub sync
# ===============================
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

def _gh_put_csv(local_path, repo_rel_path, message):
    with open(local_path, "rb") as f:
        csv_bytes = f.read()
    repo, branch, base_path = _gh_paths()
    path = f"{base_path}/{repo_rel_path}".lstrip("/")
    return _gh_put_file(repo, branch, path, csv_bytes, message)

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

# ===============================
# CSV bootstrap + migration
# ===============================
def ensure_files():
    if not os.path.exists(STUDENT_CSV):
        pd.DataFrame(columns=["Code", "Name", "Surname", "Gender"]).to_csv(STUDENT_CSV, index=False, encoding="utf-8")
    if not os.path.exists(BOOKS_CSV):
        pd.DataFrame(columns=["Book ID", "Book Title", "Author", "Status", "Barcode"]).to_csv(BOOKS_CSV, index=False, encoding="utf-8")
    if not os.path.exists(LOG_CSV):
        pd.DataFrame(columns=["Student", "Book Title", "Book ID", "Date Borrowed", "Due Date", "Returned", "Barcode", "Copy Key"]).to_csv(LOG_CSV, index=False, encoding="utf-8")

    legacy_students = os.path.join(BASE_DIR, "Student_records.csv")
    legacy_books    = os.path.join(BASE_DIR, "Library_books.csv")
    legacy_logs     = os.path.join(BASE_DIR, "Borrow_log.csv")

    if _file_rowcount(STUDENT_CSV) == 0 and os.path.exists(legacy_students):
        try:
            df = pd.read_csv(legacy_students, dtype=str).fillna("")
            df = df.rename(columns={
                "Boy / Girl":"Gender",
                "First Name":"Name",
                "Last Name":"Surname",
                "Student Code":"Code",
                "ID":"Code"
            })
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

    if "_ROW_UID" not in df.columns:
        df["_ROW_UID"] = ""

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

    if "Book Title" in df.columns:
        keep = (df["Book Title"].astype(str).str.strip() != "") | (df["Barcode"].astype(str).str.strip() != "")
        df = df[keep].copy()

    return df

# ===============================
# Load/Save
# ===============================
def load_students():
    df = pd.read_csv(STUDENT_CSV, dtype=str).fillna("")
    df.columns = df.columns.str.strip()
    df = df.rename(columns={"Boy / Girl":"Gender","First Name":"Name","Last Name":"Surname","Student Code":"Code","ID":"Code"})

    if "Code" not in df.columns:
        df["Code"] = ""
    for c in df.columns:
        df[c] = df[c].astype(str).str.strip()

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
    out = _apply_book_helpers(df)
    out.to_csv(BOOKS_CSV, index=False, encoding="utf-8")
    if _gh_enabled():
        try:
            _gh_put_csv(BOOKS_CSV, "Library_books.csv", "Update Library_books.csv via Streamlit app")
        except Exception as e:
            st.error(f"GitHub sync failed: {e}")

# ===============================
# Catalog ↔ Log smart sync
# ===============================
def sync_missing_open_logs(books_df: pd.DataFrame, logs_df: pd.DataFrame):
    books_borrowed = books_df.loc[books_df["Status"].str.lower()=="borrowed", ["Book Title","Book ID","Barcode","_COPY_KEY"]].copy()
    open_logs = logs_df[logs_df["Returned"].str.lower()=="no"].copy()
    open_keys = set(open_logs["Copy Key"].astype(str).str.strip())

    to_fix = books_borrowed[~books_borrowed["_COPY_KEY"].isin(open_keys)]
    if to_fix.empty:
        return logs_df, [], []

    created, patched = [], []
    now = datetime.now()
    due = now + timedelta(days=14)
    logs_new = logs_df.copy()

    for _, r in to_fix.iterrows():
        key = r["_COPY_KEY"]
        bid = r.get("Book ID","")
        bc  = r.get("Barcode","")

        patch_mask = (
            (logs_new["Returned"].str.lower()=="no")
            & (logs_new["Copy Key"].astype(str).str.strip() == "")
            & (logs_new["Book ID"].astype(str).str.strip() == str(bid).strip())
            & (logs_new["Barcode"].astype(str).str.strip() == str(bc).strip())
        )
        if patch_mask.any():
            logs_new.loc[patch_mask, "Copy Key"] = key
            patched.append(key)
            continue

        new_row = {
            "Student": "",
            "Book Title": r.get("Book Title",""),
            "Book ID": bid,
            "Barcode": bc,
            "Copy Key": key,
            "Date Borrowed": now.strftime("%Y-%m-%d %H:%M:%S"),
            "Due Date": due.strftime("%Y-%m-%d %H:%M:%S"),
            "Returned": "No",
        }
        logs_new = df_append(logs_new, new_row)
        created.append(key)

    return logs_new, created, patched

# ===============================
# Learners Tab (NEW)
# ===============================
def learners_tab():
    st.subheader("👩‍🎓 Learners (Students)")

    students = load_students().copy()
    # Ensure columns exist
    for c in ["Code", "Name", "Surname", "Gender"]:
        if c not in students.columns:
            students[c] = ""

    # Search
    q = st.text_input("Search (Code / Name / Surname)", "").strip().lower()
    view = students.copy()
    if q:
        view = view[
            view["Code"].astype(str).str.lower().str.contains(q, na=False) |
            view["Name"].astype(str).str.lower().str.contains(q, na=False) |
            view["Surname"].astype(str).str.lower().str.contains(q, na=False)
        ]

    view = view.sort_values(["Code","Name","Surname"], kind="stable").reset_index(drop=True)

    st.caption(f"Total learners: {len(students)} | Showing: {len(view)}")
    st.dataframe(view[["Code","Name","Surname","Gender"]], use_container_width=True, hide_index=True)

    st.divider()

    if not is_admin():
        st.info("Only admin can add/delete learners.")
        return

    left, right = st.columns(2)

    # Add learner
    with left:
        st.markdown("### ➕ Add learner")
        with st.form("add_learner_form", clear_on_submit=True):
            code = st.text_input("Student Code (unique) *")
            name = st.text_input("First Name *")
            surname = st.text_input("Surname *")
            gender = st.selectbox("Gender", ["Boy", "Girl", "Other"])
            ok = st.form_submit_button("Add")

        if ok:
            code = (code or "").strip()
            name = (name or "").strip()
            surname = (surname or "").strip()

            if not code or not name or not surname:
                st.error("Code, Name and Surname are required.")
            elif (students["Code"].astype(str).str.strip().str.lower() == code.lower()).any():
                st.error("That student code already exists.")
            else:
                new_row = {"Code": code, "Name": name, "Surname": surname, "Gender": gender}
                updated = pd.concat([students.drop(columns=["_CODE_CANON"], errors="ignore"), pd.DataFrame([new_row])], ignore_index=True)
                save_students(updated)
                st.success(f"Added: {code} - {name} {surname}")
                st.rerun()

    # Delete learner (by Code)
    with right:
        st.markdown("### 🗑️ Delete learner (by Code)")
        options = students["Code"].astype(str).str.strip()
        options = [c for c in sorted(options.unique().tolist()) if c]

        if not options:
            st.warning("No learners available to delete.")
            return

        pick = st.selectbox("Select Student Code", options)
        confirm = st.checkbox("I confirm deletion (cannot undo)")

        if st.button("Delete", disabled=not confirm):
            updated = students[students["Code"].astype(str).str.strip().str.lower() != pick.strip().lower()].copy()
            # Drop helper column before saving
            updated = updated.drop(columns=["_CODE_CANON"], errors="ignore")
            save_students(updated)
            st.success(f"Deleted student code: {pick}")
            st.rerun()

# ===============================
# Main App
# ===============================
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

        with st.expander("Paths"):
            st.caption(f"Students: {STUDENT_CSV}")
            st.caption(f"Books: {BOOKS_CSV}")
            st.caption(f"Logs: {LOG_CSV}")

    # Optional logo
    logo_path = os.path.join("assets", "chi-logo.png")
    if os.path.exists(logo_path):
        with open(logo_path, "rb") as f:
            encoded = base64.b64encode(f.read()).decode()
        st.markdown(
            "<div style='text-align:center; margin-top:10px;'>"
            f"<img src='data:image/png;base64,{encoded}' width='150'>"
            "</div>", unsafe_allow_html=True
        )

    st.markdown("<h1 style='text-align:center;'>📚 Tzu Chi Foundation — Tutor Class Library System</h1>", unsafe_allow_html=True)

    # Top metrics
    total_books     = len(books)
    available_count = (books["Status"].str.lower() == "available").sum()
    borrowed_count  = (books["Status"].str.lower() == "borrowed").sum()
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Students", len(students))
    c2.metric("Books", int(total_books))
    c3.metric("Available", int(available_count))
    c4.metric("Borrowed (open)", int(borrowed_count))

    # Health check & quick sync
    logged_open_keys = set(logs.loc[logs["Returned"].str.lower()=="no","Copy Key"].astype(str).str.strip())
    borrowed_copies  = set(books.loc[books["Status"].str.lower()=="borrowed","_COPY_KEY"].astype(str).str.strip())
    missing_log_keys = sorted(borrowed_copies - logged_open_keys)

    with st.expander("⚠️ Status health check"):
        if missing_log_keys:
            st.warning("Copies **Borrowed** in Catalog but no open log (by Copy Key):")
            st.write(missing_log_keys[:50])
        if st.button("🔗 Create open logs for borrowed copies (quick sync)"):
            logs_new, created, patched = sync_missing_open_logs(books, logs)
            if created or patched:
                save_logs(logs_new)
                st.success(f"Patched {len(patched)} log(s); created {len(created)} new log(s).")
                st.rerun()
            else:
                st.info("Nothing to sync — all borrowed copies already have open logs.")

    # Tabs
    tabs = st.tabs(["Borrow", "Return", "Borrowed now", "Learners", "Add", "Delete", "Catalog", "Logs", "Analytics"])

    # ---------------- Borrow ----------------
    with tabs[0]:
        st.subheader("Borrow a Book")

        st.markdown("**Scanner mode (optional)** — scan or type, press Enter.")
        colA, colB = st.columns(2)
        scan_student_code = colA.text_input("Scan/enter Student Code").strip()
        scan_book_barcode = colB.text_input("Scan/enter Book Barcode").strip()

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

        student_names = (students["Name"].fillna("").str.strip() + " " + students["Surname"].fillna("").str.strip()).str.strip()
        student_names = sorted([s for s in student_names.tolist() if s])
        sel_student_dropdown = st.selectbox("👩‍🎓 Pick Student (optional if you scanned)", [""] + student_names, index=0)

        avail = books[books["Status"].str.lower()=="available"].copy()
        if avail.empty:
            st.info("No available copies right now.")
            sel_copy_label = ""
        else:
            avail["_label"] = (
                avail["Book Title"].astype(str)
                + "  [ID:" + avail.get("Book ID","").astype(str).replace("", "-", regex=False)
                + " | BC:" + avail.get("Barcode","").astype(str).replace("", "-", regex=False) + "]"
            )
            sel_copy_label = st.selectbox("📚 Pick Book Copy (optional if you scanned)", [""] + avail["_label"].tolist(), index=0)

        final_student = selected_student or sel_student_dropdown
        if sel_copy_label and not avail.empty and sel_copy_label in avail["_label"].values:
            selected_copy_key = avail.loc[avail["_label"] == sel_copy_label, "_COPY_KEY"].iloc[0]

        days = st.slider("Borrow Days", 1, 30, 14)
        allow_override = st.checkbox("Allow borrow even if copy is marked Borrowed (back-capture)")

        if st.button("✅ Confirm Borrow"):
            if not final_student or not selected_copy_key:
                st.error("Please provide both a student and a book copy.")
            else:
                logs_latest = load_logs()
                has_open = logs_latest[(logs_latest["Student"] == final_student) & (logs_latest["Returned"].str.lower() == "no")]
                if not has_open.empty:
                    st.error(f"🚫 {final_student} already has a book out. Return it first.")
                else:
                    row = books[books["_COPY_KEY"] == selected_copy_key]
                    if row.empty:
                        st.error("Could not locate the selected copy in catalog.")
                        st.stop()
                    if row.iloc[0]["Status"] == "Borrowed" and not allow_override:
                        st.error("This copy is marked as Borrowed. Tick override for back-capture.")
                        st.stop()

                    now = datetime.now()
                    due = now + timedelta(days=days)
                    book_title = row.iloc[0]["Book Title"]
                    book_id    = row.iloc[0].get("Book ID","")
                    barcode    = row.iloc[0].get("Barcode","")
                    copy_key   = row.iloc[0]["_COPY_KEY"]

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

    # ---------------- Return ----------------
    with tabs[1]:
        st.subheader("Return a Book")

        logs_now = load_logs()
        if logs_now.empty:
            st.info("No books currently borrowed.")
        else:
            barcode_return = st.text_input("Scan/enter Book Barcode (optional)").strip()
            open_logs_view = logs_now[logs_now["Returned"].str.lower() == "no"].copy()

            hits = pd.DataFrame()
            if barcode_return:
                canon_bar = _canon(barcode_return)
                hits = open_logs_view[open_logs_view["Barcode"].map(_canon) == canon_bar]
                if not hits.empty:
                    st.success(f"Matched: {hits.iloc[0]['Book Title']}")

            if open_logs_view.empty:
                st.success("✅ No books currently out.")
            else:
                open_logs_view["Label"] = (
                    open_logs_view["Student"] + " | " +
                    open_logs_view["Book Title"] + " | " +
                    open_logs_view["Date Borrowed"]
                )

                default_idx = 0
                if not hits.empty:
                    target = hits.iloc[0]["Label"]
                    if target in open_logs_view["Label"].tolist():
                        default_idx = open_logs_view["Label"].tolist().index(target)

                selected_return = st.selectbox("Choose to Return", open_logs_view["Label"], index=default_idx)

                if st.button("📦 Mark as Returned"):
                    row = open_logs_view[open_logs_view["Label"] == selected_return].iloc[0]
                    mask = (
                        (logs_now["Student"] == row["Student"]) &
                        (logs_now["Book Title"] == row["Book Title"]) &
                        (logs_now["Date Borrowed"] == row["Date Borrowed"])
                    )
                    logs_now.loc[mask, "Returned"] = "Yes"
                    save_logs(logs_now)

                    copy_key = row.get("Copy Key","")
                    if copy_key:
                        books_now = load_books()
                        books_now.loc[books_now["_COPY_KEY"] == copy_key, "Status"] = "Available"
                        save_books(books_now)

                    st.success(f"Returned: {row['Book Title']} from {row['Student']}")
                    st.rerun()

    # ---------------- Borrowed now ----------------
    with tabs[2]:
        st.subheader("📋 Borrowed now (not returned)")
        logs_live = load_logs()
        open_df = logs_live[logs_live["Returned"].str.lower() == "no"].copy()

        if open_df.empty:
            st.success("✅ No books currently out.")
        else:
            show_cols = ["Student","Book Title","Book ID","Barcode","Date Borrowed","Due Date"]
            for c in show_cols:
                if c not in open_df.columns:
                    open_df[c] = ""

            now_ = datetime.now()
            open_df["Due Date"] = pd.to_datetime(open_df["Due Date"], errors="coerce")

            def _style(row):
                if pd.notna(row["Due Date"]) and row["Due Date"] < now_:
                    return ['background-color:#ffefef'] * len(row)
                return [''] * len(row)

            st.dataframe(open_df[show_cols].style.apply(_style, axis=1), use_container_width=True)
            st.download_button("⬇️ Download current borrowers (CSV)", open_df[show_cols].to_csv(index=False), "borrowed_now.csv", "text/csv")

    # ---------------- Learners (NEW) ----------------
    with tabs[3]:
        learners_tab()

    # ---------------- Add ----------------
    with tabs[4]:
        st.subheader("➕ Add Student or Book Copy")
        opt = st.radio("Add:", ["Student", "Book copy"], horizontal=True)

        if opt == "Student":
            code = st.text_input("Student Code (e.g., 001)")
            name = st.text_input("First Name")
            surname = st.text_input("Surname")
            gender = st.selectbox("Gender", ["Boy", "Girl", "Other"])
            if st.button("Add Student"):
                if not is_admin():
                    st.error("Only admin can add students.")
                else:
                    students_now = load_students()
                    if (students_now["Code"].astype(str).str.strip().str.lower() == (code or "").strip().lower()).any():
                        st.error("Student code already exists.")
                    else:
                        students_now = df_append(students_now.drop(columns=["_CODE_CANON"], errors="ignore"), {
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
                    save_books(books_now)
                    st.success("Book copy added.")
                    st.rerun()

    # ---------------- Delete ----------------
    with tabs[5]:
        st.subheader("🗑️ Delete Student or Book Copy")
        opt = st.radio("Delete:", ["Student", "Book copy"], horizontal=True)

        if opt == "Student":
            if not is_admin():
                st.info("Only admin can delete students.")
            else:
                students_now = load_students()
                codes = sorted([c for c in students_now["Code"].astype(str).str.strip().unique().tolist() if c])
                to_delete = st.selectbox("Select student code to delete", [""] + codes)
                confirm = st.checkbox("I confirm deletion")
                if st.button("Delete Student", disabled=not (to_delete and confirm)):
                    students_now = students_now[students_now["Code"].astype(str).str.strip().str.lower() != to_delete.strip().lower()]
                    students_now = students_now.drop(columns=["_CODE_CANON"], errors="ignore")
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

    # ---------------- Catalog ----------------
    with tabs[6]:
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

            for c in ["Book ID", "Book Title", "Author", "Status", "Barcode"]:
                if c not in df.columns:
                    df[c] = ""

            df["_row_id"] = df.index

            edited = st.data_editor(
                df[["Book ID","Book Title","Author","Status","Barcode","_row_id"]],
                num_rows="dynamic",
                use_container_width=True,
                column_config={"_row_id": {"hidden": True}},
                key="catalog_editor",
            )

            if st.button("💾 Save changes"):
                updated = books_now.copy()

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

                save_books(updated)
                st.success("Catalog saved.")
                st.rerun()

    # ---------------- Logs ----------------
    with tabs[7]:
        st.subheader("📜 Borrow Log")

        logs_now = load_logs()
        if logs_now.empty:
            st.info("No logs yet.")
        else:
            now = datetime.now()
            logs_now["Due Date"] = pd.to_datetime(logs_now["Due Date"], errors="coerce")

            logs_now["Days Overdue"] = logs_now.apply(
                lambda row: (now - row["Due Date"]).days
                if str(row.get("Returned","No")).lower() == "no" and pd.notna(row["Due Date"]) and row["Due Date"] < now
                else 0,
                axis=1
            )

            show_cols = ["Student","Book Title","Book ID","Barcode","Copy Key","Date Borrowed","Due Date","Returned","Days Overdue"]
            for c in show_cols:
                if c not in logs_now.columns:
                    logs_now[c] = ""

            st.dataframe(logs_now[show_cols], use_container_width=True)
            st.download_button("Download CSV", logs_now.to_csv(index=False), file_name="Borrow_log.csv", mime="text/csv")

    # ---------------- Analytics ----------------
    with tabs[8]:
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
            logs_od["Due Date"] = pd.to_datetime(logs_od.get("Due Date",""), errors="coerce")
            overdue = logs_od[(logs_od.get("Returned","No").str.lower() == "no") & (logs_od["Due Date"] < today)]
            if not overdue.empty:
                overdue = overdue.copy()
                overdue["Days Overdue"] = (today - overdue["Due Date"]).dt.days
                st.warning(f"⏰ {len(overdue)} books overdue!")
                st.dataframe(overdue[["Student","Book Title","Due Date","Days Overdue"]])
            else:
                st.success("✅ No overdue books!")

            logs_trend = logs_a.copy()
            logs_trend["Date Borrowed"] = pd.to_datetime(logs_trend.get("Date Borrowed",""), errors="coerce")
            trend = logs_trend.dropna(subset=["Date Borrowed"]).groupby(
                logs_trend["Date Borrowed"].dt.to_period("M")
            ).size().reset_index(name="Borrows")
            trend["Month"] = trend["Date Borrowed"].astype(str)
            st.plotly_chart(px.line(trend, x="Month", y="Borrows", title="📈 Borrowing Trends Over Time"))

# ===============================
# Run app
# ===============================
if __name__ == "__main__":
    if "logged_in" not in st.session_state or not st.session_state["logged_in"]:
        login_form()
    else:
        main()
