import streamlit as st
import pandas as pd
from io import BytesIO

st.set_page_config(page_title="Sourcing Manager Pro", layout="wide")
st.title("📦 Product & Supplier Database (Excel-in, Web Query Tool)")

# ---------------- Column normalization + flexible mapping ----------------
ALIASES = {
    # Updated to match your file: Supplier_id
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

def to_excel_bytes(df: pd.DataFrame, sheet_name: str = "Master_List") -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    return output.getvalue()

def apply_search(df: pd.DataFrame, term: str) -> pd.DataFrame:
    term = (term or "").strip()
    if not term:
        return df
    t = term.lower()
    mask = False
    for col in df.columns:
        mask = mask | df[col].astype(str).str.lower().str.contains(t, na=False)
    return df[mask]

def build_details_from_remaining_columns(df_raw: pd.DataFrame, used_cols: set[str]) -> pd.Series:
    """
    If Details isn't mapped, build it by concatenating any non-empty values
    from columns not used for Supplier/Product/etc (including 'unnamed:*').
    """
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
            # include column name for clarity, especially for Unnamed cols
            parts.append(f"{c}: {s}")
        return " | ".join(parts)

    return df_raw.apply(row_details, axis=1)

def build_standard_df(df_raw: pd.DataFrame, mapping: dict) -> pd.DataFrame:
    """
    Standardized columns:
    Supplier, Product, Details, Website, Phone, Login Info

    Required: supplier + product must be mapped.
    If Details not mapped: auto-build from leftover columns.
    """
    df = df_raw.copy()

    used_cols = set(v for v in mapping.values() if v)
    out = {}

    # Supplier/Product always from mapped columns
    out["Supplier"] = df[mapping["supplier"]]
    out["Product"] = df[mapping["product"]]

    # Optional columns
    if mapping.get("website"):
        out["Website"] = df[mapping["website"]]
    else:
        out["Website"] = ""

    if mapping.get("phone"):
        out["Phone"] = df[mapping["phone"]]
    else:
        out["Phone"] = ""

    if mapping.get("login_info"):
        out["Login Info"] = df[mapping["login_info"]]
    else:
        out["Login Info"] = ""

    # Details: mapped if possible, otherwise built from leftover columns (Unnamed etc.)
    if mapping.get("details"):
        out["Details"] = df[mapping["details"]]
    else:
        out["Details"] = build_details_from_remaining_columns(df, used_cols)

    std = pd.DataFrame(out)

    # Clean values to strings
    for c in std.columns:
        std[c] = std[c].where(~pd.isna(std[c]), "")
        std[c] = std[c].astype(str).str.strip()

    # Drop empty required rows
    std = std[(std["Supplier"] != "") & (std["Product"] != "")]
    return std.reset_index(drop=True)

# ---------------- Session state ----------------
if "master_df" not in st.session_state:
    st.session_state.master_df = None
if "mapping" not in st.session_state:
    st.session_state.mapping = None
if "source_filename" not in st.session_state:
    st.session_state.source_filename = None

tab1, tab2, tab3, tab4 = st.tabs(["🔍 Search", "➕ Add Manual", "📂 Import", "💾 Export"])

# ---------------- TAB 3: IMPORT ----------------
with tab3:
    st.write("Upload an Excel file. The dataset is kept in-memory for searching/exporting (no database file).")
    uploaded_file = st.file_uploader("Upload Excel file", type=["xlsx"])

    if uploaded_file:
        df_raw = pd.read_excel(uploaded_file)
        df_raw = normalize_columns(df_raw)

        st.subheader("Detected columns")
        st.code(", ".join(list(df_raw.columns)) if len(df_raw.columns) else "(none)")

        auto_map = infer_mapping(list(df_raw.columns))

        st.subheader("Column mapping (auto-detected; you can override)")
        mapping = {}
        col_options = ["(not mapped)"] + list(df_raw.columns)

        for canonical in CANONICAL_ORDER:
            default = auto_map.get(canonical)
            default_idx = col_options.index(default) if default in col_options else 0
            choice = st.selectbox(
                f"{DISPLAY_NAMES[canonical]} ←",
                col_options,
                index=default_idx,
                key=f"map_{canonical}",
            )
            mapping[canonical] = None if choice == "(not mapped)" else choice

        missing_required = [k for k in ["supplier", "product"] if not mapping.get(k)]
        if missing_required:
            st.error(f"Required fields not mapped: {', '.join(DISPLAY_NAMES[k] for k in missing_required)}")
            st.stop()

        std_preview = build_standard_df(df_raw, mapping)

        st.subheader("Preview (standardized)")
        st.dataframe(std_preview.head(50), use_container_width=True)

        if st.session_state.master_df is not None:
            st.warning("A dataset is already loaded in this session. Importing will overwrite it.")
        confirm = st.checkbox("I understand importing will overwrite the currently loaded dataset (if any).")

        if st.button("Confirm Import"):
            if st.session_state.master_df is not None and not confirm:
                st.error("Confirmation required. Tick the checkbox first.")
            else:
                st.session_state.master_df = std_preview
                st.session_state.mapping = mapping
                st.session_state.source_filename = uploaded_file.name
                st.success("Imported successfully. Dataset loaded for Search/Add/Export.")

# ---------------- TAB 1: SEARCH ----------------
with tab1:
    df = st.session_state.master_df
    if df is None or df.empty:
        st.info("No dataset loaded yet. Go to the Import tab and upload an Excel file.")
    else:
        st.caption(f"Loaded: {st.session_state.source_filename} | Rows: {len(df)}")
        search_term = st.text_input("Search products, suppliers, or keywords:")

        suppliers = sorted([s for s in df["Supplier"].unique() if s.strip() != ""])
        sel_suppliers = st.multiselect("Filter by Supplier (optional)", suppliers)

        filtered = df
        if sel_suppliers:
            filtered = filtered[filtered["Supplier"].isin(sel_suppliers)]
        filtered = apply_search(filtered, search_term)

        st.dataframe(filtered, use_container_width=True)

        st.download_button(
            label="📥 Download current results (Excel)",
            data=to_excel_bytes(filtered, sheet_name="Search_Results"),
            file_name="search_results.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

# ---------------- TAB 2: ADD MANUAL ----------------
with tab2:
    df = st.session_state.master_df
    if df is None:
        st.info("No dataset loaded yet. Import an Excel file first.")
    else:
        with st.form("manual_form"):
            col1, col2 = st.columns(2)
            with col1:
                sup_name = st.text_input("Supplier Name*")
                sup_web = st.text_input("Website")
                sup_phone = st.text_input("Phone")
                sup_login = st.text_input("Login/Notes")
            with col2:
                prod_name = st.text_input("Product Name*")
                prod_details = st.text_area("Product Details")

            if st.form_submit_button("Save Entry"):
                if not sup_name.strip() or not prod_name.strip():
                    st.error("Supplier Name and Product Name are required.")
                else:
                    new_row = {
                        "Supplier": sup_name.strip(),
                        "Product": prod_name.strip(),
                        "Details": (prod_details or "").strip(),
                        "Website": (sup_web or "").strip(),
                        "Phone": (sup_phone or "").strip(),
                        "Login Info": (sup_login or "").strip(),
                    }
                    st.session_state.master_df = pd.concat(
                        [st.session_state.master_df, pd.DataFrame([new_row])],
                        ignore_index=True,
                    )
                    st.success("Saved (in this session). Export to download the updated Excel.")

        st.subheader("Current dataset (last 50 rows)")
        st.dataframe(st.session_state.master_df.tail(50), use_container_width=True)

# ---------------- TAB 4: EXPORT ----------------
with tab4:
    df = st.session_state.master_df
    if df is None or df.empty:
        st.info("No dataset loaded yet.")
    else:
        st.header("Download Data")

        st.download_button(
            label="📥 Download Master Dataset as Excel",
            data=to_excel_bytes(df, sheet_name="Master_List"),
            file_name="supplier_database_export.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

        st.download_button(
            label="📥 Download Master Dataset as CSV",
            data=df.to_csv(index=False).encode("utf-8"),
            file_name="supplier_database_export.csv",
            mime="text/csv",
        )

