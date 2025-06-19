import os
import pandas as pd
from difflib import SequenceMatcher
from pathlib import Path


# Helper Function to apply font stylings for Correct/Incorrect Matchings
# This function will now receive the row of the *display_df* and the *comparison_df*
# to look up similarity scores.
def style_validation_table(row, comparison_df, columns_to_compare):
    styles = [''] * len(row)
    # The 'Sr. No' column is at index 0, so we start from index 1 for predicted columns
    # and map them back to their original column names for comparison_df lookup.
    for i, col_name_display in enumerate(row.index):
        if col_name_display == 'Sr. No':
            continue # Skip styling for Sr. No

        # In this version, col_name_display is already the original column name
        original_col_name = col_name_display

        if original_col_name in columns_to_compare:
            # Get the row index from the display_df (which is the same as actual_df/llm_df index)
            row_idx = row.name # pandas Series has a .name attribute for its index

            # Look up the similarity score from the comparison_df
            # Ensure row_idx is valid for comparison_df before accessing
            if original_col_name in comparison_df.columns and row_idx in comparison_df.index:
                similarity_value = comparison_df.loc[row_idx, original_col_name]

                if pd.notna(similarity_value):
                    if similarity_value == 100:
                        styles[i] = 'color: green;'
                    elif similarity_value < 100: # Any mismatch
                        styles[i] = 'color: red;'
    return styles

# --------------------------
def novolex_validate_invoices():

    # pred = pd.read_excel('../filled_invoice_novolex.xlsx', header=1)
    # Construct an absolute path from the script
    base_dir = os.path.dirname(os.path.dirname(__file__))  # goes up from app/
    file_path = os.path.join(base_dir, 'filled_invoice_novolex.xlsx')
    pred = pd.read_excel(file_path, header=1)

    base_dir = os.path.dirname(os.path.dirname(__file__))  # goes up from app/
    file_path = os.path.join(base_dir, 'novolex_validation_data.xlsx')
    actual = pd.read_excel(file_path, header=1)
    # actual = pd.read_excel('../novolex_validation_data.xlsx', header=1)

    actual['Billing Date '] = pd.to_datetime(actual['Billing Date '], format='%d/%m/%Y')
    actual['Billing Date '] = actual['Billing Date '].dt.strftime('%m/%d/%y')

    actual['Month'] = actual['Month'].dt.strftime('%b-%y')

    actual['From'] = pd.to_datetime(actual['From'], format='%d/%m/%Y')
    actual['From'] = actual['From'].dt.strftime('%d/%m/%y')

    actual['To'] = pd.to_datetime(actual['To'], format='%d/%m/%Y')
    actual['To'] = actual['To'].dt.strftime('%d/%m/%y')

    pred['Blended rate\n$/kWh\n(With VAT)'] = pred['Blended rate\n$/kWh\n(With VAT)'].round(2)
    pred['Total kWh'] = pred['Total kWh'].round(2)
    pred[' kWh per day'] = pred[' kWh per day'].round(2)
    pred['Total $ amount\n(Without VAT)'] = pred['Total $ amount\n(Without VAT)'].round(2)

    actual['Blended rate\n$/kWh\n(With VAT)'] = actual['Blended rate\n$/kWh\n(With VAT)'].round(2)
    actual['Blended rate\n$/kWh\n(Without VAT)'] = actual['Blended rate\n$/kWh\n(Without VAT)'].round(2)
    actual['Total kWh'] = actual['Total kWh'].round(2)
    actual[' kWh per day'] = actual[' kWh per day'].round(1)
    actual['Total $ amount\n(Without VAT)'] = actual['Total $ amount\n(Without VAT)'].round(2)

    pred.columns = pred.columns.str.strip()
    actual.columns = actual.columns.str.strip()
   
    
    # Step 3: Sort the DataFrame by 'Billing Date'
    pred_sorted = pred.sort_values(by='Billing Date')
    actual_sorted = actual.sort_values(by='Billing Date')


    # Optional: Reset index
    pred_sorted = pred_sorted.reset_index(drop=True)
    actual_sorted = actual_sorted.reset_index(drop=True)

    print("Pred Kwh per day ---->>>>", pred['Total $ amount\n(Without VAT)'].tolist())
    print("Actual Kwh per day ---->>>>", actual['Total $ amount\n(Without VAT)'].tolist())
    # # Columns you want to compare
    # columns_to_compare = ['Billing Date', 'Month', 'From', 'To',
    #     'No of Days', 'Day \nkWh', 'Night\nkWh', 'Total kWh', 'kWh per day','DUOS Capacity Charge',
    #     'Excess Capacity Charge', 
    #     'VAT\n$', 'Total\n $ amount\n(With VAT)',
    #     'Total $ amount\n(Without VAT)',
    #     'Blended rate\n$/kWh\n(With VAT)',
    #     'Blended rate\n$/kWh\n(Without VAT)']
    
     # Define columns to be compared
    columns_to_compare = [
        'Billing Date', 'Month', 'From', 'To', 'No of Days', 'Day \nkWh', 'Night\nkWh',
        'Total kWh', 'kWh per day', 'DUOS Capacity Charge', 'Excess Capacity Charge',
        'VAT\n$', 'Total\n $ amount\n(With VAT)', 'Total $ amount\n(Without VAT)',
        'Blended rate\n$/kWh\n(With VAT)', 'Blended rate\n$/kWh\n(Without VAT)'
    ]
    
    columns_to_compare = [col for col in columns_to_compare if col in pred_sorted.columns and col in actual_sorted.columns]

    # print(result)
    match_percentage = {}

    # print(result)
    for col in columns_to_compare:
        pred_col = pred_sorted[col]
        actual_col = actual_sorted[col]

        # Compare elements, treating NaNs in same position as a match
        match = (pred_col == actual_col) | (pd.isna(pred_col) & pd.isna(actual_col))

        print(f"Column: {col}", match)

        # Calculate match percentage
        match_percentage[col] = match.sum() / len(pred_col) * 100
        print(f"Match Percentage for {col}: {match_percentage[col]}%")
        # print(f"Match Percentage for {col}: {match_percentage[col]}%")

    # Convert to DataFrame
    result = pd.DataFrame(list(match_percentage.items()), columns=['ColumnName', 'Match Percentage'])
   
    result['Match Percentage'] = result['Match Percentage'].round(2)
    shape = pred.shape
    result['Total Count'] = shape[0]
    actual_df = actual_sorted[columns_to_compare].astype(str)
    llm_df = pred_sorted[columns_to_compare].astype(str)

    # Similarity function
    def similarity(a, b):
        return SequenceMatcher(None, a, b).ratio() * 100

    
    # Initialize summary dictionary for different match categories
    summary = {col: {'100%': 0, '90-100%': 0, '<90': 0} for col in columns_to_compare}

    # DataFrame to store similarity scores for detailed validation table
    comparison_df = pd.DataFrame(index=actual_df.index, columns=columns_to_compare)

    # Populate comparison_df with similarity scores and update summary counts
    for col in columns_to_compare:
        for i in range(len(actual_df)):
            actual_val = actual_df.iloc[i][col]
            llm_val = llm_df.iloc[i][col]
            score = similarity(actual_val, llm_val)
            comparison_df.loc[i, col] = score # Store the similarity score directly

            if score == 100:
                summary[col]['100%'] += 1
            elif score >= 90:
                summary[col]['90-100%'] += 1
            else:
                summary[col]['<90'] += 1

    # Convert summary dictionary to DataFrame
    summary_df = pd.DataFrame.from_dict(summary, orient='index').reset_index()
    summary_df.columns = ['ColumnName', '100%', '90-100%', '<90']
    final_result = pd.merge(result, summary_df, on='ColumnName')

    ## Pivot Table for Summary Evals
    # --- Calculate Overall Summary Metrics for Pivot Table ---
    total_100_matches = 0
    total_90_99_matches = 0
    total_less_89_matches = 0

    # Aggregate counts from the per-column summary
    for col in columns_to_compare:
        total_100_matches += summary[col]['100%']
        total_90_99_matches += summary[col]['90-100%']
        total_less_89_matches += summary[col]['<90']

    # Calculate the total number of individual cell comparisons made across all columns and rows
    total_relevant_entities = len(actual_df) * len(columns_to_compare)

    # Calculate percentages, handling division by zero if no entities to compare
    if total_relevant_entities > 0:
        percent_100 = (total_100_matches / total_relevant_entities) * 100
        percent_90_99 = (total_90_99_matches / total_relevant_entities) * 100
        percent_less_89 = (total_less_89_matches / total_relevant_entities) * 100
    else:
        percent_100 = 0.0
        percent_90_99 = 0.0
        percent_less_89 = 0.0

    # Create the pivot-like summary DataFrame as requested
    summary_pivot_data = {
        "100% Match Entities": [total_100_matches, f"{percent_100:.2f}%"],
        "90-99% Match Entities": [total_90_99_matches, f"{percent_90_99:.2f}%"],
        "<89% Match Entities": [total_less_89_matches, f"{percent_less_89:.2f}%"]
    }
    summary_pivot_df = pd.DataFrame(summary_pivot_data, index=["Count", "% of Total Relevant"])

    # Bold column names and index for the pivot table
    summary_pivot_df_styled = summary_pivot_df.style.set_table_styles([
        # Style for column headers
        {'selector': 'th.col_heading', 'props': [('font-weight', 'bold')]},
        {'selector': 'th.col_heading.level0', 'props': [('font-weight', 'bold')]},

        # Style for index headers
        {'selector': 'th.row_heading', 'props': [('font-weight', 'bold')]},
        {'selector': 'th.row_heading.level0', 'props': [('font-weight', 'bold')]}
    ])

    ## Now, let's create a combined DataFrame for display with styling
    # This display_df will ONLY contain 'Sr. No' and the original column names
    display_df = pd.DataFrame(index=actual_df.index)
    display_df.insert(0, 'Sr. No', range(1, 1 + len(display_df)))

    for col in columns_to_compare:
        # Assign directly to the original column name
        display_df[col] = llm_df[col]
    
    

    # Apply the row-wise styling (green/red) based on (Similarity) values from comparison_df
    # We use a lambda to pass additional arguments to style_validation_table
    styled_display_df_filtered = display_df.style.apply(
        lambda row: style_validation_table(row, comparison_df, columns_to_compare), axis=1
    ).set_table_styles([
        {'selector': 'th', 'props': [('font-weight', 'bold')]}
    ])
    
    total_entities = len(actual_df) # Total number of invoices/rows processed

    # Return the filtered styled DataFrame for the UI
    return summary_pivot_df_styled, styled_display_df_filtered, total_entities, total_relevant_entities













