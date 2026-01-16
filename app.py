import streamlit as st
import pandas as pd
from io import BytesIO

from db import (
    get_engine,
    init_db,
    get_or_create_dataset_id,
    list_datasets,
    load_dataset,
    replace_dataset_with_df,
    add_record,
    delete_records,
)

# ----------------- Excel normalization + mapping (your working approach) -----------------
ALIASES = {
    "supplier": [
        "supplier", "supplier name", "vendor", "vendor name", "company", "company name",
        "supplier_id", "supplier id", "supplierid"
    ],
    "product": ["product", "product name", "item", "item name", "sku", "service"],
    "details": ["details", "detail", "description", "notes", "product details"],
    "website": ["website", "url", "web", "link"],
    "phone": ["phone", "telephone", "tel", "contact number", "mobile"],
    "login_info": ["login info", "login", "login details", "credentials", "account", "notes (login)", "login/notes"],
}

CANONICAL_ORDER = ["supplier", "product", "details", "website", "phone", "login_info"]
DISPLAY_NAMES = {
    "supplier": "Supplier",
    "product": "Product",
    "details": "Details",
    "website": "Website",
    "phone": "Phone",
    "login_info": "Login Info",
}

def norm(s: str) -> str:
    return (
        str(s)
        .strip()
        .lower()
        .replace("\n", " ")
        .replace("\t", " ")
    )

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [norm(c) for c in df.columns]
    return df

def infer_mapping(columns: list[str]) -> dict:
    colset = set(columns)
    mapping = {}
    for canonical, options in ALIASES.items():
        hit = None
        for opt in options:
            optn = norm(opt)
            if optn in colset:
                hit = optn
                break
        mapping[canonical] = hit
    return mapping

def build_details_from_remaining_columns(df_raw: pd.DataFrame, used_cols: set[str]) -> pd.Series:
    remaining = [c for c in df_raw.columns if c not in used_cols]
    if not remaining:
        return pd.Series([""] * len(df_raw), index=df_raw.index)

    def row_details(row):
        parts = []
        for c in remaining:
            v = row.get(c, "")
            if pd.isna(v):
                continue
            s = str(v).strip()
            if not s or s.lower() == "nan":
                continue
            parts.append(f"{c}: {s}")
        return " | ".join(parts)

    return df_raw.apply(row_details, axis=1)

def build_standard_df(df_raw: pd.DataFrame, mapping: dict) -> pd.DataFrame:
    df = df_raw.copy()
    used_cols = set(v for v in mapping.values() if v)

    out = {}
    out["Supplier"] = df[mapping["supplier"]]
    out["Product"] = df[mapping["product"]]

    out["Website"] = df[mapping["website"]] if mapping.get("website") else ""
    out["Phone"] = df[mapping["phone"]] if mapping.get("phone") else ""
    out["Login Info"] = df[mapping["login_info"]] if mapping.get("login_info") else ""

    if mapping.get("details"):
        out["Details"] = df[mapping["details"]]
    else:
        out["Details"] = build_details_from_remaining_columns(df, used_cols)

    std = pd.DataFrame(out)

    for c in std.columns:
        std[c] = std[c].where(~pd.isna(std[c]), "")
        std[c] = std[c].astype(str).str.strip()

    std = std[(std["Supplier"] != "") & (std["Product"] != "")]
    return std.reset_index(drop=True)

def to_canonical(df_std: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame(
        {
            "supplier": df_std["Supplier"].astype(str),
            "product": df_std["Product"].astype(str),
            "details": df_std.get("Details", "").astype(str),
            "website": df_std.get("Website", "").astype(str),
            "phone": df_std.get("Phone", "").astype(str),
            "login_info": df_std.get("Login Info", "").astype(str),
        }
    ).fillna("").astype(str)

    df["supplier"] = df["supplier"].str.strip()
    df["product"] = df["product"].str.strip()
    df = df[(df["supplier"] != "") & (df["product"] != "")]
    return df.reset_index(drop=True)

def to_excel_bytes(df: pd.DataFrame, sheet_name: str = "Master_List") -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    return output.getvalue()

def apply_search(df: pd.DataFrame, term: str) -> pd.DataFrame:
    term = (term or "").strip().lower()
    if not term:
        return df
    mask = False
    for c in ["supplier", "product", "details", "website", "phone", "login_info"]:
        mask = mask | df[c].astype(str).str.lower().str.contains(term, na=False)
    return df[mask]

# ----------------- App -----------------
st.set_page_config(page_title="Sourcing Manager Pro", layout="wide")
st.title("📦 Product & Supplier Database (Persistent Server Files)")

engine = get_engine(st.secrets["DATABASE_URL"])
init_db(engine)

default_name = st.secrets.get("DEFAULT_DATASET_NAME", "Main")
default_id = get_or_create_dataset_id(engine, default_name)

# Sidebar: choose server file (dataset)
st.sidebar.header("Server files")
datasets_df = list_datasets(engine)
name_by_id = dict(zip(datasets_df["id"], datasets_df["name"]))

selected_id = st.sidebar.selectbox(
    "Open server file",
    options=list(datasets_df["id"]),
    format_func=lambda i: name_by_id.get(i, f"Dataset {i}"),
    index=(list(datasets_df["id"]).index(default_id) if default_id in list(datasets_df["id"]) else 0),
)

st.sidebar.divider()
st.sidebar.subheader("Create new server file (optional)")
new_ds_name = st.sidebar.text_input("New server file name")
if st.sidebar.button("Create"):
    if not new_ds_name.strip():
        st.sidebar.error("Enter a name.")
    else:
        get_or_create_dataset_id(engine, new_ds_name.strip())
        st.sidebar.success("Created.")
        st.rerun()

tab1, tab2, tab3, tab4 = st.tabs(["🔍 Search", "➕ Add / 🗑️ Delete", "📂 Import Excel", "💾 Export"])

# Load current dataset
df_db = load_dataset(engine, selected_id)
df_can = df_db.copy()
for c in ["supplier", "product", "details", "website", "phone", "login_info"]:
    if c not in df_can.columns:
        df_can[c] = ""
df_can = df_can.fillna("").astype(str)

with tab1:
    st.caption(f"Open server file: {name_by_id.get(selected_id, str(selected_id))} | Rows: {len(df_can)}")
    term = st.text_input("Search products, suppliers, or keywords:")
    filtered = apply_search(df_can, term)

    show = filtered.rename(
        columns={
            "id": "Record ID",
            "supplier": "Supplier",
            "product": "Product",
            "details": "Details",
            "website": "Website",
            "phone": "Phone",
            "login_info": "Login Info",
        }
    )
    st.dataframe(show, use_container_width=True)

with tab2:
    st.subheader("Add record (saved server-side)")
    with st.form("add_form"):
        c1, c2 = st.columns(2)
        with c1:
            supplier = st.text_input("Supplier*")
            website = st.text_input("Website")
            phone = st.text_input("Phone")
            login_info = st.text_input("Login/Notes")
        with c2:
            product = st.text_input("Product*")
            details = st.text_area("Details")
        if st.form_submit_button("Save"):
            if not supplier.strip() or not product.strip():
                st.error("Supplier and Product are required.")
            else:
                add_record(
                    engine,
                    selected_id,
                    supplier.strip(),
                    product.strip(),
                    details=(details or "").strip(),
                    website=(website or "").strip(),
                    phone=(phone or "").strip(),
                    login_info=(login_info or "").strip(),
                )
                st.success("Saved.")
                st.rerun()

    st.divider()
    st.subheader("Delete record(s) (saved server-side)")
    if df_db.empty:
        st.info("No records to delete.")
    else:
        ids = df_db["id"].tolist()
        to_delete = st.multiselect("Select Record IDs to delete", options=ids)
        confirm_del = st.checkbox("I understand this permanently deletes the selected records.")
        if st.button("Delete selected"):
            if not to_delete:
                st.error("Select at least one Record ID.")
            elif not confirm_del:
                st.error("Tick the confirmation checkbox first.")
            else:
                n = delete_records(engine, selected_id, [int(x) for x in to_delete])
                st.success(f"Deleted {n} record(s).")
                st.rerun()

with tab3:
    st.subheader("Import Excel (saved server-side)")
    st.write("Upload an Excel file, map columns, then either overwrite the open server file or save as a new server file.")

    uploaded = st.file_uploader("Upload Excel (.xlsx)", type=["xlsx"])
    if uploaded:
        df_raw = pd.read_excel(uploaded)
        df_raw = normalize_columns(df_raw)

        st.write("Detected columns:")
        st.code(", ".join(df_raw.columns))

        auto_map = infer_mapping(list(df_raw.columns))
        mapping = {}
        options = ["(not mapped)"] + list(df_raw.columns)

        st.write("Column mapping (auto-detected; you can override):")
        for canonical in CANONICAL_ORDER:
            default = auto_map.get(canonical)
            default_idx = options.index(default) if default in options else 0
            choice = st.selectbox(
                f"{DISPLAY_NAMES[canonical]} ←",
                options,
                index=default_idx,
                key=f"map_{canonical}",
            )
            mapping[canonical] = None if choice == "(not mapped)" else choice

        missing_required = [k for k in ["supplier", "product"] if not mapping.get(k)]
        if missing_required:
            st.error("You must map Supplier and Product.")
        else:
            df_std = build_standard_df(df_raw, mapping)
            st.subheader("Preview (standardized)")
            st.dataframe(df_std.head(50), use_container_width=True)

            df_new = to_canonical(df_std)

            st.divider()
            mode = st.radio(
                "Import mode",
                options=["Overwrite currently open server file", "Save as a NEW server file"],
                horizontal=True,
            )

            if mode == "Overwrite currently open server file":
                st.warning("This will permanently replace the open server file contents.")
                confirm = st.checkbox("I understand this overwrite is permanent.")
                if st.button("Apply overwrite"):
                    if not confirm:
                        st.error("Tick the confirmation checkbox first.")
                    else:
                        replace_dataset_with_df(engine, selected_id, df_new)
                        st.success("Overwritten.")
                        st.rerun()
            else:
                new_name = st.text_input("New server file name for this import")
                if st.button("Create new server file from this Excel"):
                    if not new_name.strip():
                        st.error("Enter a name.")
                    else:
                        new_id = get_or_create_dataset_id(engine, new_name.strip())
                        replace_dataset_with_df(engine, new_id, df_new)
                        st.success("Created new server file.")
                        st.rerun()

with tab4:
    st.subheader("Export current server file")
    export_df = df_can.rename(
        columns={
            "supplier": "Supplier",
            "product": "Product",
            "details": "Details",
            "website": "Website",
            "phone": "Phone",
            "login_info": "Login Info",
        }
    )
    filename_base = name_by_id.get(selected_id, "server_file")

    st.download_button(
        "📥 Download as Excel",
        data=to_excel_bytes(export_df, sheet_name=filename_base[:31]),
        file_name=f"{filename_base}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    st.download_button(
        "📥 Download as CSV",
        data=export_df.to_csv(index=False).encode("utf-8"),
        file_name=f"{filename_base}.csv",
        mime="text/csv",
    )
