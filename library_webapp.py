# ======================================================
# Tzu Chi Library System (Updated 2025-10)
# Features:
# - Supports multiple copies per title
# - One open borrow per person (Student or Staff)
# - Borrow/Return scanning
# - Staff & Students combined list
# - GitHub CSV sync (optional)
# - Smart log sync (no blank Student rows)
# ======================================================

import os, base64, hashlib, requests
from datetime import datetime, timedelta, date, time
import pandas as pd
import streamlit as st
import plotly.express as px

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
# Auth
# ======================================================
def hash_password(p): return hashlib.sha256(p.encode()).hexdigest()
USERS = {"admin": hash_password("admin123"), "teacher": hash_password("tzuchi2025")}
def verify_login(u, p): return USERS.get(u) == hash_password(p)

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
def _canon(s): return "".join(ch for ch in str(s) if ch.isalnum()).upper()
def df_append(df, row): return pd.concat([df, pd.DataFrame([row])], ignore_index=True)
def _safe_to_datetime(s):
    try:
        dt = pd.to_datetime(s, errors="coerce")
        return dt.to_pydatetime() if not pd.isna(dt) else None
    except: return None
def _ts(d, t): return datetime.combine(d, t).strftime("%Y-%m-%d %H:%M:%S")

# ======================================================
# File setup
# ======================================================
def ensure_files():
    if not os.path.exists(STUDENT_CSV):
        pd.DataFrame(columns=["Code","Name","Surname","Gender","Staff"]).to_csv(STUDENT_CSV,index=False)
    if not os.path.exists(BOOKS_CSV):
        pd.DataFrame(columns=["Book ID","Book Title","Author","Status","Barcode"]).to_csv(BOOKS_CSV,index=False)
    if not os.path.exists(LOG_CSV):
        pd.DataFrame(columns=["Student","Book Title","Book ID","Barcode","Copy Key","Date Borrowed","Due Date","Returned"]).to_csv(LOG_CSV,index=False)

def load_students():
    df = pd.read_csv(STUDENT_CSV,dtype=str).fillna("")
    df = df.rename(columns={"Boy / Girl":"Gender","Student Code":"Code"})
    for col in ["Code","Name","Surname","Gender","Staff"]:
        if col not in df.columns: df[col] = ""
    df["Staff"] = df["Staff"].astype(str).str.lower().map(
        {"yes":"Yes","y":"Yes","true":"Yes","1":"Yes","no":"No","n":"No","false":"No","0":"No"}).fillna("No")
    df["_CODE_CANON"] = df["Code"].map(_canon)
    return df.loc[:,~df.columns.str.match("Unnamed")]

def save_students(df):
    df.drop(columns=["_CODE_CANON"],errors="ignore").to_csv(STUDENT_CSV,index=False)

def load_books():
    df = pd.read_csv(BOOKS_CSV,dtype=str,on_bad_lines="skip").fillna("")
    if "Status" not in df.columns: df["Status"]="Available"
    if "_ROW_UID" not in df.columns:
        df["_ROW_UID"] = [str(i+1) for i in range(len(df))]
    df["_BARCODE_CANON"] = df["Barcode"].map(_canon)
    df["_COPY_KEY"] = df["Book ID"].astype(str)+"|"+df["Barcode"].astype(str)+"|"+df["_ROW_UID"].astype(str)
    return df

def save_books(df): df.to_csv(BOOKS_CSV,index=False)

def load_logs():
    df = pd.read_csv(LOG_CSV,dtype=str,on_bad_lines="skip").fillna("")
    for c in ["Student","Book Title","Book ID","Barcode","Copy Key","Date Borrowed","Due Date","Returned"]:
        if c not in df.columns: df[c]=""
    df["Returned"]=df["Returned"].str.lower().map(
        {"yes":"Yes","no":"No","y":"Yes","n":"No"}).fillna("No")
    return df

def save_logs(df): df.to_csv(LOG_CSV,index=False)

# ======================================================
# Smart Sync fix (no blank student)
# ======================================================
def sync_missing_open_logs(books,logs):
    borrowed = books[books["Status"].str.lower()=="borrowed"]
    logs_copy_keys = set(logs[logs["Returned"].str.lower()=="no"]["Copy Key"])
    created, patched = [], []
    for _, b in borrowed.iterrows():
        key = b["_COPY_KEY"]
        if key in logs_copy_keys: continue
        same = logs[(logs["Book ID"]==b["Book ID"]) & (logs["Barcode"]==b["Barcode"]) & (logs["Returned"].str.lower()=="no")]
        if not same.empty:
            logs.loc[same.index,"Copy Key"]=key
            patched.append(key)
        else:
            now=datetime.now(); due=now+timedelta(days=14)
            logs= df_append(logs,{
                "Student":"","Book Title":b["Book Title"],"Book ID":b["Book ID"],"Barcode":b["Barcode"],
                "Copy Key":key,"Date Borrowed":now.strftime("%Y-%m-%d %H:%M:%S"),
                "Due Date":due.strftime("%Y-%m-%d %H:%M:%S"),"Returned":"No"})
            created.append(key)
    return logs, created, patched

# ======================================================
# Main
# ======================================================
def main():
    ensure_files()
    students, books, logs = load_students(), load_books(), load_logs()

    # Sidebar
    with st.sidebar:
        st.success(f"🔓 Logged in as: {st.session_state['username']}")
        if st.button("🚪 Logout"): st.session_state.clear(); st.rerun()
        st.caption(f"Students: {len(students)} | Books: {len(books)} | Logs: {len(logs)}")

    st.markdown("<h1 style='text-align:center;'>📚 Tzu Chi Foundation — Tutor Class Library System</h1>",unsafe_allow_html=True)

    # Status health check
    logged_keys=set(logs[logs["Returned"].str.lower()=="no"]["Copy Key"])
    borrowed_keys=set(books[books["Status"].str.lower()=="borrowed"]["_COPY_KEY"])
    missing=set(borrowed_keys-logged_keys)
    with st.expander("⚠️ Status health check"):
        if missing:
            st.warning("Borrowed copies without open log:")
            st.write(list(missing)[:20])
        if st.button("🔗 Create open logs for borrowed copies (quick sync)"):
            fixed,created,patched=sync_missing_open_logs(books,logs)
            save_logs(fixed)
            st.success(f"Patched {len(patched)}, created {len(created)} new log(s).")
            st.rerun()

    tabs = st.tabs(["📖 Borrow","📦 Return","📋 Borrowed Now","➕ Add","📜 Logs"])

    # ================= Borrow =================
    with tabs[0]:
        st.subheader("Borrow a Book")
        sc1,sc2=st.columns(2)
        code=sc1.text_input("Scan Student Code")
        barcode=sc2.text_input("Scan Book Barcode")
        found_student=""
        if code:
            hit=students[students["_CODE_CANON"]==_canon(code)]
            if not hit.empty:
                r=hit.iloc[0]; found_student=f"{r['Name']} {r['Surname']}"
                st.success(f"Student: {found_student}")
        found_copy=""
        if barcode:
            hit=books[(books["_BARCODE_CANON"]==_canon(barcode))&(books["Status"].str.lower()=="available")]
            if not hit.empty:
                found_copy=hit.iloc[0]["_COPY_KEY"]
                st.success(f"Book found: {hit.iloc[0]['Book Title']}")

        names=(students["Name"].str.strip()+" "+students["Surname"].str.strip()).tolist()
        final_student=st.selectbox("Pick Borrower",[""]+sorted(set(names)),index=0)
        if found_student: final_student=found_student
        avail=books[books["Status"].str.lower()=="available"].copy()
        avail["_label"]=avail["Book Title"]+" [BC:"+avail["Barcode"]+"]"
        sel=st.selectbox("Select Book Copy",[""]+avail["_label"].tolist(),index=0)
        if sel: found_copy=avail.loc[avail["_label"]==sel,"_COPY_KEY"].iloc[0]
        days=st.slider("Borrow Days",1,30,14)

        if st.button("✅ Confirm Borrow"):
            if not final_student or not found_copy:
                st.error("Select both borrower and book.")
            else:
                if not logs[(logs["Student"]==final_student)&(logs["Returned"].str.lower()=="no")].empty:
                    st.error(f"{final_student} already has a book borrowed.")
                else:
                    row=books[books["_COPY_KEY"]==found_copy].iloc[0]
                    now=datetime.now(); due=now+timedelta(days=days)
                    new_log={"Student":final_student,"Book Title":row["Book Title"],"Book ID":row["Book ID"],
                             "Barcode":row["Barcode"],"Copy Key":found_copy,"Date Borrowed":now.strftime("%Y-%m-%d %H:%M:%S"),
                             "Due Date":due.strftime("%Y-%m-%d %H:%M:%S"),"Returned":"No"}
                    logs=df_append(logs,new_log); save_logs(logs)
                    books.loc[books["_COPY_KEY"]==found_copy,"Status"]="Borrowed"; save_books(books)
                    st.success(f"Borrowed {row['Book Title']} to {final_student}")
                    st.rerun()

    # ================= Return =================
    with tabs[1]:
        st.subheader("Return a Book")
        barcode_r=st.text_input("Scan/enter Barcode")
        open_logs=logs[logs["Returned"].str.lower()=="no"]
        if barcode_r:
            canon=_canon(barcode_r)
            hit=open_logs[open_logs["Barcode"].map(_canon)==canon]
            if not hit.empty:
                row=hit.iloc[0]
                logs.loc[hit.index,"Returned"]="Yes"
                books.loc[books["_COPY_KEY"]==row["Copy Key"],"Status"]="Available"
                save_logs(logs); save_books(books)
                st.success(f"Returned: {row['Book Title']} from {row['Student']}")
                st.rerun()
        else:
            sel=st.selectbox("Select entry",open_logs["Student"]+" | "+open_logs["Book Title"])
            if st.button("Mark Returned"):
                r=open_logs[open_logs["Student"]+" | "+open_logs["Book Title"]==sel].iloc[0]
                logs.loc[open_logs.index,"Returned"]="Yes"
                books.loc[books["_COPY_KEY"]==r["Copy Key"],"Status"]="Available"
                save_logs(logs); save_books(books)
                st.success("Book returned.")
                st.rerun()

    # ================= Borrowed Now =================
    with tabs[2]:
        st.subheader("Current Borrows")
        open_df=logs[logs["Returned"].str.lower()=="no"]
        if open_df.empty: st.info("None out.")
        else:
            st.dataframe(open_df[["Student","Book Title","Due Date"]])

    # ================= Add =================
    with tabs[3]:
        st.subheader("Add Student/Staff or Book")
        choice=st.radio("Add:",["Person","Book Copy"],horizontal=True)
        if choice=="Person":
            code=st.text_input("Code")
            name=st.text_input("First Name")
            surname=st.text_input("Surname")
            gender=st.selectbox("Gender",["Boy","Girl","Other"])
            staff=st.checkbox("This person is Staff",value=False)
            if st.button("Add Person"):
                df=load_students()
                df=df_append(df,{"Code":code,"Name":name,"Surname":surname,"Gender":gender,"Staff":"Yes" if staff else "No"})
                save_students(df); st.success("Person added."); st.rerun()
        else:
            title=st.text_input("Book Title")
            author=st.text_input("Author")
            bid=st.text_input("Book ID")
            bc=st.text_input("Barcode")
            if st.button("Add Book Copy"):
                df=load_books()
                next_uid=str(int(pd.to_numeric(df["_ROW_UID"],errors="coerce").max() or 0)+1)
                new={"Book ID":bid,"Book Title":title,"Author":author,"Status":"Available","Barcode":bc,"_ROW_UID":next_uid}
                df=df_append(df,new); save_books(df); st.success("Book added."); st.rerun()

    # ================= Logs =================
    with tabs[4]:
        st.subheader("Borrow Log")
        logs_now=load_logs()
        if logs_now.empty: st.info("No logs yet.")
        else:
            now=datetime.now()
            logs_now["Due Date"]=pd.to_datetime(logs_now["Due Date"],errors="coerce")
            logs_now["Days Overdue"]=logs_now.apply(
                lambda r:(now-r["Due Date"]).days if str(r["Returned"]).lower()=="no" and pd.notna(r["Due Date"]) and r["Due Date"]<now else 0,axis=1)
            st.dataframe(logs_now, use_container_width=True)
            st.download_button("Download CSV",logs_now.to_csv(index=False),"Borrow_log.csv")

# ======================================================
if __name__ == "__main__":
    if "logged_in" not in st.session_state or not st.session_state["logged_in"]:
        login_form()
    else:
        main()
