import streamlit as st
import os
import shutil
from pathlib import Path
from rwc_nv_invoice_extractor import rwc_run_pipeline_batch_write # Make sure to expose `run_pipeline` in a separate file
from novolex_nv_invoice_extractor import novolex_run_pipeline_batch_write
import pandas as pd
import openpyxl
from io import BytesIO
import os
from rwc_validation import rwc_validate_invoices
from novolex_validation import novolex_validate_invoices
import time
from rwc_novolex_kpi import novolex_rwc_run_pipeline_batch_write
from rwc_novolex_validation import rwc_novolex_validate_invoices

if "pipeline_run" not in st.session_state:
    st.session_state.pipeline_run = False
if "selected_option" not in st.session_state:
    st.session_state.selected_option = "Select"


os.environ["STREAMLIT_WATCH_MODE"] = "false"

# Define folders
BASE_DIR = Path(__file__).resolve().parent

RWC_EXCEL_OUTPUT = (BASE_DIR / ".." / "filled_invoice_rwc.xlsx").resolve()
NOVOLEX_EXCEL_OUTPUT = (BASE_DIR / ".." / "filled_invoice_novolex.xlsx").resolve()
BOTH_EXCEL_OUTPUT = (BASE_DIR / ".." / "combined_output.xlsx").resolve()


def style_dataframe(df):
    header_color = "#1A5276"  # dark blue
    row_colors = ['#E8F6F3', "#EEDC9C"]  # light blue, creamy

    return df.style\
        .set_table_styles(
            {
                col: [{'selector': '', 'props': [('background-color', header_color), ('color', 'white'), ('text-align', 'center'), ('font-weight', 'bold')]}]
                for col in df.columns
            },
            axis=1
        )\
        .apply(
            lambda x: [
                f'background-color: {row_colors[i % 2]}; color: black;'
                for i in range(len(x))
            ],
            axis=1
        )\
        .set_properties(**{
            'border': '1px solid #D5DBDB',
            'font-size': '14px',
            'text-align': 'left'
        })


st.set_page_config(
    page_title="Utility Bill Analysis",
    page_icon=":person_in_tuxedo:",
    layout="wide",
)
st.markdown("""
        <style>
            .avatar img {
                    visibility: hidden;
                }
                            .block-container {
                    padding-top: 1rem;
                    padding-bottom: 0rem;
                }
            .st-emotion-cache-1wbqy5l.e17vllj40 {
                    visibility: hidden;
                }
            .st-emotion-cache-18ni7ap {
            height:0;
            }
        </style>
        """, unsafe_allow_html=True)

page_bg_img  = """
<style>
[data-testid="stAppViewContainer"]{
background-size : cover;
}
[data-testid="stHeader"]{
background-color : rgba(0,0,0,0)
}
[data-testid="stToolbar"]{
right : 2rem;
}
[data-testid="stSidebarContent"]{
background-color: lightblue;
}
[data-testid="stImage"]{
    max-width: 100%;
    margin-bottom: -50px;
}
</style>
"""
# App Title
st.subheader("📄 Utility Bill Analysis ")
st.markdown(page_bg_img, unsafe_allow_html=True)


# File Uploader
# uploaded_files = st.sidebar.file_uploader(
#     "Upload multiple invoice PDFs", type="pdf", accept_multiple_files=True
# )

# Clear temp_in before every run to avoid leftovers
# if uploaded_files:
#     for f in TEMP_IN.glob("*.pdf"):
#         f.unlink()

#     for file in uploaded_files:
#         file_path = TEMP_IN / file.name
#         with open(file_path, "wb") as f:
#             f.write(file.read())
#     st.sidebar.success(f"{len(uploaded_files)} files uploaded to temp_in!")

# Process Button

with st.sidebar:
    options = ["RWC", "Novolex Milton", "Both"]
    options_with_default = ["Select"] + options

    selected = st.selectbox("Choose an option:", options_with_default)
    st.session_state.selected_option = selected

    if selected == "Select":
        st.sidebar.warning("Please select an option from the dropdown.")
# Process Invoices
if st.sidebar.button("🚀 Process Invoices"):
    with st.spinner("Processing all invoices..."):
        # time.sleep(10)
        selected_option = st.session_state.selected_option
        if selected_option == "RWC":
            rwc_run_pipeline_batch_write()
            summary, styled, total, relevant = rwc_validate_invoices()
            output = RWC_EXCEL_OUTPUT
            VENDOR = "RWC"
        elif selected_option == "Novolex Milton":
            novolex_run_pipeline_batch_write()
            summary, styled, total, relevant = novolex_validate_invoices()
            output = NOVOLEX_EXCEL_OUTPUT
            VENDOR = "Novolex Milton"
        elif selected_option == "Both":
            novolex_rwc_run_pipeline_batch_write()
            summary, styled, total, relevant = rwc_novolex_validate_invoices()
            output = BOTH_EXCEL_OUTPUT
            VENDOR = "Both"

            

        # Save to session state
        st.session_state.pipeline_run = True
        st.session_state.excel_output = output
        st.session_state.summary = summary
        st.session_state.styled = styled
        st.session_state.total = total
        st.session_state.relevant = relevant
        st.session_state.vendor = VENDOR

# Display results if pipeline has been run
if st.session_state.pipeline_run and st.session_state.selected_option == st.session_state.vendor:
    st.success("✅ Invoices processed successfully! Download the report below.")

    st.download_button(
        label="Download Excel Report",
        data=st.session_state.excel_output.read_bytes(),
        file_name=f"utility_bill_{st.session_state.vendor}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

    col1, col2 = st.columns(2)
    with col1:
        st.metric("Total Invoices", st.session_state.total)
    with col2:
        st.metric("Total Relevant Entities", st.session_state.relevant)

    st.subheader("Validation Report Summary")
    st.dataframe(st.session_state.summary, use_container_width=True, height=100)

    st.subheader("Detailed Validation Table")
    st.dataframe(st.session_state.styled, use_container_width=True, height=500, hide_index=True)
        # Display the styled validation table
# # ---------------------------------------------------------
# if st.sidebar.button("🚀 Process Invoices"):
#     with st.spinner("Processing all invoices... This may take a moment."):
#         if selected_option != "Select" and selected_option == "RWC":
#             st.session_state["results"] = rwc_run_pipeline_batch_write()
#             st.session_state["validation_df"] = rwc_validate_invoices()
#             st.session_state["excel_output"] = RWC_EXCEL_OUTPUT
#         elif selected_option != "Select" and selected_option == "Novolex Milton":
#             st.session_state["results"] = novolex_run_pipeline_batch_write()
#             st.session_state["validation_df"] = novolex_validate_invoices()
#             st.session_state["excel_output"] = NOVOLEX_EXCEL_OUTPUT

# # Show results if they exist
# if "excel_output" in st.session_state and Path(st.session_state["excel_output"]).exists():
#     df = pd.read_excel(st.session_state["excel_output"], header=[0, 1])
#     validation_df = st.session_state["validation_df"]

#     st.success("✅ Invoices processed successfully! Download the report below.")
#     st.download_button(
#         label="Download Excel Report",
#         data=Path(st.session_state["excel_output"]).read_bytes(),
#         file_name="filled_invoice.xlsx",
#         mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
#     )

#     st.subheader("Billing Summary Report")
#     st.dataframe(df, use_container_width=True, height=500, hide_index=True)
    
#     st.subheader("Validation Report")
#     st.dataframe(validation_df, use_container_width=True, height=500, hide_index=True)      
