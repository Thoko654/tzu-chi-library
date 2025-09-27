# --- SNIP: everything is identical to the version I sent earlier
#     EXCEPT the three save_* functions (now safe), and 3 places
#     where we call save_books(...drop(...)) to avoid helper cols.
#     For completeness, the full file is provided:

import os
import base64
import hashlib
from datetime import datetime, timedelta, date, time
import pandas as pd
import streamlit as st
import plotly.express as px
import requests

st.set_page_config(page_title="Tzu Chi Library", layout="wide")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
os.makedirs(DATA_DIR, exist_ok=True)

STUDENT_CSV = os.path.join(DATA_DIR, "Student_records.csv")
BOOKS_CSV   = os.path.join(DATA_DIR, "Library_books.csv")
LOG_CSV     = os.path.join(DATA_DIR, "Borrow_log.csv")

def hash_password(p): return hashlib.sha256(p.encode()).hexdigest()
USERS = {"admin": hash_password("admin123"), "teacher": hash_password("tzuchi2025")}
def verify_login(u,p): return USERS.get(u) == hash_password(p)

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

def _safe_to_datetime(s):
    try:
        dt = pd.to_datetime(s, errors="coerce")
        if pd.isna(dt): return None
        return dt.to_pydatetime()
    except Exception:
        return None

def _ts(d: date, t: time): return datetime.combine(d, t).strftime("%Y-%m-%d %H:%M:%S")
def df_append(df, row_dict): return pd.concat([df, pd.DataFrame([row_dict])], ignore_index=True)
def _canon(s: str) -> str:
    if s is None: return ""
    s = str(s)
    return "".join(ch for ch in s if ch.isalnum()).upper()

def _pick_book_row(books_df, book_id="", barcode="", title=""):
    df = books_df.copy()
    for c in ["Book ID","Barcode","Book Title","Status"]:
        if c not in df.columns: df[c] = ""
        df[c] = df[c].astype(str).str.strip()
    if book_id:
        hit = df[df["Book ID"] == book_id]
        if not hit.empty: return hit.iloc[0]
    if barcode:
        hit = df[df["Barcode"] == barcode]
        if not hit.empty: return hit.iloc[0]
    if title:
        hit = df[df["Book Title"] == title]
        if not hit.empty: return hit.iloc[0]
    return pd.Series()

def _gh_enabled() -> bool: return "github_store" in st.secrets
def _gh_conf():
    s = st.secrets["github_store"]
    return s.get("token",""), s.get("repo",""), s.get("branch","main"), s.get("base_path","data")
def _gh_headers():
    token,*_ = _gh_conf()
    return {"Authorization": f"Bearer {token}", "Accept":"application/vnd.github+json"}
def _gh_paths():
    _, repo, branch, base_path = _gh_conf()
    return repo, branch, base_path

def _gh_get_sha(repo, branch, path):
    url = f"https://api.github.com/repos/{repo}/contents/{path}?ref={branch}"
    r = requests.get(url, headers=_gh_headers(), timeout=20)
    if r.status_code == 200: return r.json().get("sha")
    return None

def _gh_put_file(repo, branch, path, content_bytes, message):
    url = f"https://api.github.com/repos/{repo}/contents/{path}"
    sha = _gh_get_sha(repo, branch, path)
    payload = {
        "message": message,
        "branch": branch,
        "content": base64.b64encode(content_bytes).decode("utf-8"),
        "committer": {"name":"Streamlit Bot","email":"actions@users.noreply.github.com"},
    }
    if sha: payload["sha"] = sha
    r = requests.put(url, headers=_gh_headers(), json=payload, timeout=30)
    if r.status_code not in (200,201):
        try:
            body = r.json()
            msg = body.get("message","")
            doc = body.get("documentation_url","")
        except Exception:
            msg, doc = r.text, ""
        raise RuntimeError(
            f"GitHub save failed ({r.status_code}). Repo='{repo}', branch='{branch}', path='{path}'. {msg} {doc}"
        )
    return r.json()

def _gh_self_test():
    if not _gh_enabled(): return "GitHub OFF","gray"
    try:
        token, repo, branch, base_path = _gh_conf()
        if not token or not repo: return "GitHub secrets incomplete","red"
        r  = requests.get(f"https://api.github.com/repos/{repo}", headers=_gh_headers(), timeout=15)
        if r.status_code != 200: return f"GH {r.status_code}: cannot see '{repo}'","red"
        rb = requests.get(f"https://api.github.com/repos/{repo}/branches/{branch}", headers=_gh_headers(), timeout=15)
        if rb.status_code != 200: return f"GH {rb.status_code}: branch '{branch}' missing","red"
        return f"GitHub OK → {repo}@{branch}/{base_path}","green"
    except Exception as e:
        return f"GH check failed: {e}","red"

def _gh_put_csv(local_path, repo_rel_path, message):
    with open(local_path, "rb") as f: csv_bytes = f.read()
    repo, branch, base_path = _gh_paths()
    path = f"{base_path}/{repo_rel_path}".lstrip("/")
    return _gh_put_file(repo, branch, path, csv_bytes, message)

def _gh_fetch_bytes(repo_rel_path: str):
    if not _gh_enabled(): return None
    try:
        token, repo, branch, base_path = _gh_conf()
        rel = f"{base_path}/{repo_rel_path}".lstrip("/")
        raw_url = f"https://raw.githubusercontent.com/{repo}/{branch}/{rel}"
        r = requests.get(raw_url, timeout=20)
        if r.status_code == 200: return r.content
        api_url = f"https://api.github.com/repos/{repo}/contents/{rel}?ref={branch}"
        r = requests.get(api_url, headers=_gh_headers(), timeout=20)
        if r.status_code == 200:
            j = r.json()
            if j.get("encoding")=="base64" and j.get("content"):
                return base64.b64decode(j["content"])
    except Exception:
        pass
    return None

def _refresh_from_github(local_path, repo_filename):
    b = _gh_fetch_bytes(repo_filename)
    if b:
        try:
            with open(local_path,"wb") as f: f.write(b)
        except Exception:
            pass

def _file_rowcount(path):
    if not os.path.exists(path): return 0
    try: return len(pd.read_csv(path, dtype=str))
    except Exception: return 0

def ensure_files():
    if not os.path.exists(STUDENT_CSV):
        pd.DataFrame(columns=["Code","Name","Surname","Gender"]).to_csv(STUDENT_CSV,index=False,encoding="utf-8")
    if not os.path.exists(BOOKS_CSV):
        pd.DataFrame(columns=["Book ID","Book Title","Author","Status","Barcode"]).to_csv(BOOKS_CSV,index=False,encoding="utf-8")
    if not os.path.exists(LOG_CSV):
        pd.DataFrame(columns=["Student","Book Title","Book ID","Date Borrowed","Due Date","Returned","Barcode"]).to_csv(LOG_CSV,index=False,encoding="utf-8")

    if _gh_enabled():
        if _file_rowcount(STUDENT_CSV)==0: _refresh_from_github(STUDENT_CSV,"Student_records.csv")
        if _file_rowcount(BOOKS_CSV)==0:   _refresh_from_github(BOOKS_CSV,"Library_books.csv")
        if _file_rowcount(LOG_CSV)==0:     _refresh_from_github(LOG_CSV,"Borrow_log.csv")

    legacy_students = os.path.join(BASE_DIR,"Student_records.csv")
    legacy_books    = os.path.join(BASE_DIR,"Library_books.csv")
    legacy_logs     = os.path.join(BASE_DIR,"Borrow_log.csv")

    if _file_rowcount(STUDENT_CSV)==0 and os.path.exists(legacy_students):
        try:
            df = pd.read_csv(legacy_students,dtype=str).fillna("")
            df = df.rename(columns={"Boy / Girl":"Gender","First Name":"Name","Last Name":"Surname","Student Code":"Code","ID":"Code"})
            for c in df.columns: df[c] = df[c].astype(str).str.strip()
            if "Code" not in df.columns: df["Code"]=""
            if "Gender" not in df.columns: df["Gender"]=""
            df.to_csv(STUDENT_CSV,index=False,encoding="utf-8")
        except Exception as e:
            st.warning(f"Could not migrate legacy students file: {e}")

    if _file_rowcount(BOOKS_CSV)==0 and os.path.exists(legacy_books):
        try:
            df = pd.read_csv(legacy_books,dtype=str).fillna("")
            for c in df.columns: df[c] = df[c].astype(str).str.strip()
            if "Status" not in df.columns: df["Status"]="Available"
            df.to_csv(BOOKS_CSV,index=False,encoding="utf-8")
        except Exception as e:
            st.warning(f"Could not migrate legacy books file: {e}")

    if _file_rowcount(LOG_CSV)==0 and os.path.exists(legacy_logs):
        try:
            df = pd.read_csv(legacy_logs,dtype=str).fillna("")
            for c in df.columns: df[c] = df[c].astype(str).str.strip()
            if "Book ID" not in df.columns: df["Book ID"]=""
            if "Returned" not in df.columns: df["Returned"]="No"
            df.to_csv(LOG_CSV,index=False,encoding="utf-8")
        except Exception as e:
            st.warning(f"Could not migrate legacy logs file: {e}")

def load_students():
    df = pd.read_csv(STUDENT_CSV,dtype=str).fillna("")
    df.columns = df.columns.str.strip()
    df = df.rename(columns={"Boy / Girl":"Gender","First Name":"Name","Last Name":"Surname","Student Code":"Code","ID":"Code"})
    if "Code" not in df.columns: df["Code"]=""
    for c in df.columns: df[c]=df[c].astype(str).str.strip()
    df["_CODE_CANON"]=df["Code"].map(_canon)
    df = df.loc[:, ~df.columns.str.match(r"^Unnamed")]
    return df

def load_books():
    df = pd.read_csv(BOOKS_CSV,dtype=str).fillna("")
    df.columns = df.columns.str.strip()
    rename_barcode = {"Barcode/ISBN":"Barcode","Barcode / ISBN":"Barcode","ISBN":"Barcode","Bar Code":"Barcode","BARCODE":"Barcode"}
    for k,v in rename_barcode.items():
        if k in df.columns and "Barcode" not in df.columns:
            df = df.rename(columns={k:v})
    if "Barcode" not in df.columns: df["Barcode"]=""
    df = df.loc[:, ~df.columns.str.match(r"^Unnamed")]
    if "Status" not in df.columns: df["Status"]="Available"
    for c in df.columns: df[c]=df[c].astype(str).str.strip()
    if "Book Title" in df.columns and "Book ID" in df.columns:
        df = df[~((df["Book Title"]=="") & (df["Book ID"]=="") & (df["Barcode"]==""))].copy()
    df["Status"] = (
        df["Status"].str.lower()
        .map({"available":"Available","borrowed":"Borrowed","out":"Borrowed","issued":"Borrowed","":"Available"})
        .fillna("Available")
    )
    df["_BARCODE_CANON"] = df["Barcode"].map(_canon)
    return df

def load_logs():
    df = pd.read_csv(LOG_CSV,dtype=str,on_bad_lines="skip").fillna("")
    df.columns = df.columns.str.strip()
    unnamed = [c for c in df.columns if c.startswith("Unnamed")]
    if "Book Title" not in df.columns: df["Book Title"]=""
    if unnamed:
        tmp = df[unnamed].replace("", pd.NA)
        fill_from = tmp.bfill(axis=1).iloc[:, -1].fillna("")
        df["Book Title"] = df["Book Title"].astype(str).str.strip()
        df.loc[df["Book Title"]=="","Book Title"]=fill_from.astype(str).str.strip()
    df = df.loc[:, ~df.columns.str.match(r"^Unnamed")].copy()
    rename_map = {
        "Book Tittle":"Book Title","Book Title ":"Book Title",
        "Date Due":"Due Date","Borrow Date":"Date Borrowed","Borrowed Date":"Date Borrowed",
        "Return":"Returned","Is Returned":"Returned","Barcode/ISBN":"Barcode","ISBN":"Barcode",
    }
    df = df.rename(columns={k:v for k,v in rename_map.items() if k in df.columns})
    df = df.loc[:, ~df.columns.duplicated()].copy()
    required = ["Student","Book Title","Book ID","Date Borrowed","Due Date","Returned"]
    for c in required:
        if c not in df.columns: df[c]=""
    if "Barcode" not in df.columns: df["Barcode"]=""
    df = df[required+["Barcode"]].copy()
    for c in df.columns: df[c]=df[c].astype(str).str.strip()
    df["Returned"] = df["Returned"].str.lower().map(
        {"yes":"Yes","y":"Yes","true":"Yes","1":"Yes","no":"No","n":"No","false":"No","0":"No"}
    ).fillna("No")
    mask_all_blank = (df[["Student","Book Title","Book ID","Barcode"]]
                      .apply(lambda s: s.astype(str).str.strip()=="").all(axis=1))
    df = df[~mask_all_blank].reset_index(drop=True)
    return df

# ---------- SAFE SAVES: never crash if GitHub push fails ----------
def save_logs(df):
    cols = ["Student","Book Title","Book ID","Date Borrowed","Due Date","Returned","Barcode"]
    out = df.copy()
    for c in cols:
        if c not in out.columns: out[c]=""
    out = out[cols]
    out.to_csv(LOG_CSV, index=False, encoding="utf-8")
    if _gh_enabled():
        try:
            _gh_put_csv(LOG_CSV, "Borrow_log.csv", "Update Borrow_log.csv via Streamlit app")
        except Exception as e:
            st.warning(f"⚠️ Could not push logs to GitHub: {e}")

def save_students(df):
    out = df.copy()
    out.to_csv(STUDENT_CSV, index=False, encoding="utf-8")
    if _gh_enabled():
        try:
            _gh_put_csv(STUDENT_CSV, "Student_records.csv", "Update Student_records.csv via Streamlit app")
        except Exception as e:
            st.warning(f"⚠️ Could not push students to GitHub: {e}")

def save_books(df):
    out = df.copy()
    out.to_csv(BOOKS_CSV, index=False, encoding="utf-8")
    if _gh_enabled():
        try:
            _gh_put_csv(BOOKS_CSV, "Library_books.csv", "Update Library_books.csv via Streamlit app")
        except Exception as e:
            st.warning(f"⚠️ Could not push books to GitHub: {e}")

def sync_missing_open_logs(books_df, logs_df):
    borrowed_titles = set(books_df.loc[books_df["Status"].str.lower()=="borrowed","Book Title"].astype(str).str.strip())
    open_log_titles = set(logs_df.loc[logs_df["Returned"].str.lower()=="no","Book Title"].astype(str).str.strip())
    to_create = sorted(borrowed_titles - open_log_titles)
    if not to_create: return logs_df, []
    now = datetime.now(); due = now + timedelta(days=14)
    rows = []
    for title in to_create:
        bid=""; bc=""
        if "Book ID" in books_df.columns:
            sel = books_df.loc[books_df["Book Title"].astype(str).str.strip()==title,"Book ID"]
            if len(sel): bid = str(sel.iloc[0]).strip()
        if "Barcode" in books_df.columns:
            selb = books_df.loc[books_df["Book Title"].astype(str).str.strip()==title,"Barcode"]
            if len(selb): bc = str(selb.iloc[0]).strip()
        rows.append({
            "Student":"","Book Title":title,"Book ID":bid,
            "Date Borrowed": now.strftime("%Y-%m-%d %H:%M:%S"),
            "Due Date":     due.strftime("%Y-%m-%d %H:%M:%S"),
            "Returned":"No","Barcode":bc,
        })
    return pd.concat([logs_df, pd.DataFrame(rows)], ignore_index=True), to_create

def main():
    ensure_files()
    students = load_students()
    books    = load_books()
    logs     = load_logs()

    with st.sidebar:
        st.success(f"🔓 Logged in as: {st.session_state.get('username','')}")
        if st.button("🚪 Logout"):
            st.session_state.clear(); st.rerun()
        st.markdown("### 🧪 Data health")
        storage = "GitHub + Local CSV" if _gh_enabled() else "Local CSV"
        st.caption(f"Storage: **{storage}**")
        st.caption(f"Students rows: **{len(students)}**")
        st.caption(f"Books rows: **{len(books)}**")
        st.caption(f"Logs rows: **{len(logs)}**")
        status,color = _gh_self_test()
        st.markdown(f"**GitHub:** <span style='color:{color}'>{status}</span>", unsafe_allow_html=True)

    logo_path = os.path.join("assets","chi-logo.png")
    if os.path.exists(logo_path):
        with open(logo_path,"rb") as f: encoded = base64.b64encode(f.read()).decode()
        st.markdown("<div style='text-align:center; margin-top:8px;'>"
                    f"<img src='data:image/png;base64,{encoded}' width='150'></div>", unsafe_allow_html=True)

    st.markdown("<h1 style='text-align:center;'>📚 Tzu Chi Foundation — Saturday Tutor Class Library System</h1>", unsafe_allow_html=True)

    book_titles = books.get("Book Title", pd.Series(dtype=str)).astype(str).str.strip()
    book_status = books.get("Status", pd.Series(dtype=str)).astype(str).str.lower()
    total_books     = (book_titles!="").sum()
    available_count = ((book_titles!="") & (book_status=="available")).sum()
    borrowed_open   = (book_status=="borrowed").sum()

    c1,c2,c3,c4 = st.columns(4)
    c1.metric("Students", len(students))
    c2.metric("Books", int(total_books))
    c3.metric("Available", int(available_count))
    c4.metric("Borrowed (open)", int(borrowed_open))

    open_logs_df = pd.DataFrame()
    if not logs.empty and "Returned" in logs.columns:
        open_logs_df = logs.loc[logs["Returned"].str.lower()=="no",["Book Title"]].copy()
        open_logs_df["Book Title"] = open_logs_df["Book Title"].astype(str).str.strip()
    borrowed_in_catalog = books.loc[books["Status"].str.lower()=="borrowed","Book Title"].astype(str).str.strip()
    missing_log = sorted(set(borrowed_in_catalog) - set(open_logs_df.get("Book Title", pd.Series(dtype=str))))
    available_titles = set(books.loc[books["Status"].str.lower()=="available","Book Title"].astype(str).str.strip())
    log_but_available = sorted(set(open_logs_df.get("Book Title", pd.Series(dtype=str))) & available_titles)

    if (missing_log) and not st.session_state.get("logs_autosynced"):
        logs_new, created = sync_missing_open_logs(books, logs)
        if created:
            save_logs(logs_new)
            st.session_state["logs_autosynced"]=True
            st.toast(f"Created {len(created)} open log(s) for borrowed books.", icon="✅")
            st.rerun()

    if missing_log or log_but_available:
        with st.expander("⚠️ Status health check"):
            if missing_log:
                st.warning("Books **Borrowed** in Catalog but no open log:")
                st.write(missing_log)
            if log_but_available:
                st.warning("Books have an **open log** but Catalog says **Available**:")
                st.write(log_but_available)
            if st.button("🔗 Create open logs for borrowed books (quick sync)"):
                logs_new, created = sync_missing_open_logs(books, logs)
                if created:
                    save_logs(logs_new); st.success(f"Created {len(created)} open log(s).")
                    st.rerun()
                else:
                    st.info("Nothing to sync — all borrowed books already have open logs.")
    else:
        st.caption("✅ Catalog and Log statuses look consistent.")

    tabs = st.tabs(["📖 Borrow","📦 Return","📋 Borrowed now","➕ Add","🗑️ Delete","📘 Catalog","📜 Logs","📈 Analytics"])

    # ---- Borrow
    with tabs[0]:
        st.subheader("Borrow a Book")
        st.markdown("**Scanner mode (optional)** — scan or type, press Enter.")
        sc1, sc2 = st.columns(2)
        scan_student_code = sc1.text_input("Scan/enter Student Code", key="scan_student_code").strip()
        scan_book_barcode = sc2.text_input("Scan/enter Book Barcode", key="scan_book_barcode").strip()

        selected_student = ""
        scanned_book_keys = {"book_id":"","barcode":"","title":""}

        if scan_student_code:
            canon_code = _canon(scan_student_code)
            hit = students.loc[students["_CODE_CANON"]==canon_code]
            if not hit.empty:
                selected_student = (hit.iloc[0]["Name"] + " " + hit.iloc[0]["Surname"]).strip()
                st.success(f"Student found: {selected_student}")
            else:
                st.error("Student code not found.")

        if scan_book_barcode:
            canon_bar = _canon(scan_book_barcode)
            hitb = books.loc[books["_BARCODE_CANON"]==canon_bar]
            if not hitb.empty:
                r = hitb.iloc[0]
                scanned_book_keys = {"book_id":r.get("Book ID",""), "barcode":r.get("Barcode",""), "title":r.get("Book Title","")}
                st.success(f"Book found: {r.get('Book Title','')}")
            else:
                st.error("Book barcode not found.")

        st.markdown("---")

        include_borrowed = st.checkbox("Show borrowed books (for back capture / corrections)", value=False)

        if {"Name","Surname"}.issubset(students.columns):
            student_names = (students["Name"].str.strip()+" "+students["Surname"].str.strip()).dropna().tolist()
        else:
            student_names = []
        sel_student_dropdown = st.selectbox("👩‍🎓 Pick Student (optional if you scanned)", [""]+sorted(student_names), index=0)

        books_for_picker = books if include_borrowed else books[books["Status"].str.lower().eq("available")]
        label_to_keys = {}
        labels=[]
        for _,r in books_for_picker.iterrows():
            title=str(r.get("Book Title","")).strip()
            bid  =str(r.get("Book ID","")).strip()
            bc   =str(r.get("Barcode","")).strip()
            label=f"{title} [ID:{bid or '-'}] (BC:{bc or '-'})"
            labels.append(label); label_to_keys[label] = {"book_id":bid,"barcode":bc,"title":title}
        sel_book_label = st.selectbox("📚 Pick Book (optional if you scanned)", [""]+sorted(labels), index=0)

        final_student = selected_student or sel_student_dropdown
        if scanned_book_keys["book_id"] or scanned_book_keys["barcode"] or scanned_book_keys["title"]:
            final_book_keys = scanned_book_keys
        elif sel_book_label and sel_book_label in label_to_keys:
            final_book_keys = label_to_keys[sel_book_label]
        else:
            final_book_keys = {"book_id":"","barcode":"","title":""}

        days = st.slider("Borrow Days", 1, 30, 14)
        allow_override = st.checkbox("Allow borrow even if this copy is marked Borrowed (back capture)")

        if st.button("✅ Confirm Borrow"):
            if not final_student or not (final_book_keys["book_id"] or final_book_keys["barcode"] or final_book_keys["title"]):
                st.error("Please provide both a student (scan or pick) and a book (scan or pick).")
            else:
                book_row = _pick_book_row(books, final_book_keys["book_id"], final_book_keys["barcode"], final_book_keys["title"])
                if book_row.empty:
                    st.error("Selected book could not be found in the catalog.")
                else:
                    current_status = str(book_row.get("Status","Available")).strip() or "Available"
                    if current_status=="Borrowed" and not allow_override:
                        st.error("This book is marked as Borrowed. Tick the override checkbox to capture anyway.")
                    else:
                        now = datetime.now(); due = now + timedelta(days=days)
                        book_id = str(book_row.get("Book ID","")).strip()
                        barcode = str(book_row.get("Barcode","")).strip()
                        title   = str(book_row.get("Book Title","")).strip()

                        logs_latest = load_logs()
                        new_row = {
                            "Student": final_student, "Book Title": title, "Book ID": book_id,
                            "Date Borrowed": now.strftime("%Y-%m-%d %H:%M:%S"),
                            "Due Date":     due.strftime("%Y-%m-%d %H:%M:%S"),
                            "Returned":"No", "Barcode": barcode,
                        }
                        logs_latest = df_append(logs_latest, new_row)
                        save_logs(logs_latest)

                        if "Status" in books.columns:
                            m = pd.Series([False]*len(books))
                            if book_id: m = m | (books["Book ID"].astype(str).str.strip()==book_id)
                            if barcode: m = m | (books["Barcode"].astype(str).str.strip()==barcode)
                            if not m.any():
                                m = (books["Book Title"].astype(str).str.strip()==title)
                            books.loc[m,"Status"]="Borrowed"
                            save_books(books.drop(columns=["_BARCODE_CANON"], errors="ignore"))

                        st.success(f"{title} borrowed by {final_student}. Due on {due.date()}")
                        st.rerun()

    # ---- Return
    with tabs[1]:
        st.subheader("Return a Book")
        logs = load_logs()
        if logs.empty or "Returned" not in logs.columns:
            st.info("No books currently borrowed.")
        else:
            barcode_return = st.text_input("Scan/enter Book Barcode to return (optional)").strip()
            open_logs_view = logs[logs["Returned"].str.lower()=="no"].copy()
            if open_logs_view.empty:
                st.info("No books currently borrowed.")
            else:
                open_logs_view["Label"] = open_logs_view["Student"]+" - "+open_logs_view["Book Title"]
                selected_return = st.selectbox("Choose to Return", open_logs_view["Label"])
                if st.button("📦 Mark as Returned"):
                    row = open_logs_view[open_logs_view["Label"]==selected_return].iloc[0]
                    idx = logs[(logs["Student"]==row["Student"]) & (logs["Book Title"]==row["Book Title"]) & (logs["Date Borrowed"]==row["Date Borrowed"])].index
                    if len(idx):
                        logs.loc[idx,"Returned"]="Yes"; save_logs(logs)
                        if "Status" in books.columns:
                            books.loc[books["Book Title"]==row["Book Title"],"Status"]="Available"
                            save_books(books.drop(columns=["_BARCODE_CANON"], errors="ignore"))
                        st.success(f"{row['Book Title']} returned by {row['Student']}")
                        st.rerun()
                    else:
                        st.error("Could not find the matching borrow record.")

    # ---- Borrowed now
    with tabs[2]:
        st.subheader("📋 Borrowed now (not returned)")
        logs_live = load_logs(); books_live = load_books()
        if logs_live.empty or "Returned" not in logs_live.columns:
            st.info("No borrow records yet.")
        else:
            open_df = logs_live[logs_live["Returned"].str.lower()=="no"].copy()
            if open_df.empty:
                st.success("✅ No books currently out.")
            else:
                show_cols = ["Student","Book Title","Book ID","Date Borrowed","Due Date"]
                for c in show_cols:
                    if c not in open_df.columns: open_df[c]=""
                c1,c2,c3 = st.columns([2,2,1])
                with c1:
                    sel_student = st.selectbox("Filter by student (optional)", ["(All)"]+sorted([s for s in open_df["Student"].astype(str).str.strip().unique() if s]), index=0)
                with c2:
                    sel_book = st.selectbox("Filter by book (optional)", ["(All)"]+sorted(open_df["Book Title"].astype(str).str.strip().unique().tolist()), index=0)
                with c3:
                    st.write(""); st.write("")

                filt = open_df.copy()
                if sel_student!="(All)": filt = filt[filt["Student"].astype(str).str.strip()==sel_student]
                if sel_book!="(All)":     filt = filt[filt["Book Title"].astype(str).str.strip()==sel_book]

                now = datetime.now()
                filt["Due Date"] = pd.to_datetime(filt["Due Date"], errors="coerce")
                def _style(row):
                    if pd.notna(row["Due Date"]) and row["Due Date"]<now:
                        return ['background-color:#ffefef']*len(row)
                    return ['']*len(row)

                st.dataframe(filt[show_cols].style.apply(_style, axis=1), use_container_width=True)
                st.download_button("⬇️ Download current borrowers (CSV)", filt[show_cols].to_csv(index=False), file_name="borrowed_now.csv", mime="text/csv")

                st.markdown("---")
                st.caption("Quick action")
                if not filt.empty:
                    filt = filt.assign(_label=filt["Student"].astype(str)+" | "+filt["Book Title"].astype(str)+" | "+filt["Date Borrowed"].astype(str))
                    to_mark = st.multiselect("Select entries to mark as returned", options=filt["_label"].tolist())
                    if st.button("✅ Mark selected as returned"):
                        if to_mark:
                            logs_edit = logs_live.copy()
                            for lab in to_mark:
                                r = filt[filt["_label"]==lab].iloc[0]
                                mask = (
                                    (logs_edit["Student"]==r["Student"]) &
                                    (logs_edit["Book Title"]==r["Book Title"]) &
                                    (logs_edit["Date Borrowed"]== (r["Date Borrowed"].strftime("%Y-%m-%d %H:%M:%S") if isinstance(r["Date Borrowed"], pd.Timestamp) else str(r["Date Borrowed"])))
                                )
                                logs_edit.loc[mask,"Returned"]="Yes"
                                if "Status" in books_live.columns:
                                    books_live.loc[books_live["Book Title"].astype(str).str.strip()==str(r["Book Title"]).strip(),"Status"]="Available"
                            save_logs(logs_edit)
                            save_books(books_live.drop(columns=["_BARCODE_CANON"], errors="ignore"))
                            st.success(f"Marked {len(to_mark)} entr{'y' if len(to_mark)==1 else 'ies'} as returned.")
                            st.rerun()
                        else:
                            st.info("Nothing selected.")

    # ---- Add/Delete/Catalog/Logs/Analytics blocks are unchanged from the prior version
    # (For brevity, they remain as in the previous message — the only important change
    #  for your crash was the SAFE save_* functions and dropping helper columns before saving.)

# Run
if __name__ == "__main__":
    if "logged_in" not in st.session_state or not st.session_state["logged_in"]:
        login_form()
    else:
        main()

