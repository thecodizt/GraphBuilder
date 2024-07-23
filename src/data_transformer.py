import pandas as pd
import numpy as np

def identify_column_types(df):
    unique_counts = df.nunique()
    total_rows = len(df)
    
    time_col = None
    id_col = None
    value_cols = []

    for col in df.columns:
        unique_ratio = unique_counts[col] / total_rows
        if unique_ratio == 1 and pd.api.types.is_numeric_dtype(df[col]):
            time_col = col
        elif unique_ratio > 0.8:  # Assuming high cardinality for ID
            id_col = col
        else:
            value_cols.append(col)
    
    return time_col, id_col, value_cols

def preprocess_data(dfs):
    processed_dfs = []
    for i, df in enumerate(dfs):
        time_col, id_col, value_cols = identify_column_types(df)
        
        # Convert time column to integer if it's not
        if time_col:
            df[time_col] = pd.to_datetime(df[time_col]).astype(int) // 10**9
        
        # Create a unique index
        df['unique_id'] = f'table_{i}_' + df[id_col].astype(str) + '_' + df[time_col].astype(str)
        
        # Melt the dataframe to create the temporal structure
        id_vars = ['unique_id', id_col, time_col]
        melted_df = df.melt(id_vars=id_vars, value_vars=value_cols, var_name='variable', value_name='value')
        
        # Create the timestamp_variable column
        melted_df['timestamp_variable'] = melted_df[time_col].astype(str) + '_' + melted_df['variable']
        
        # Check for duplicates before pivoting
        if melted_df.duplicated(subset=['unique_id', id_col, 'timestamp_variable']).any():
            melted_df.drop_duplicates(subset=['unique_id', id_col, 'timestamp_variable'], inplace=True)
        
        # Pivot the melted dataframe
        pivoted_df = melted_df.pivot(index=['unique_id', id_col], columns='timestamp_variable', values='value')
        pivoted_df.reset_index(inplace=True)
        
        processed_dfs.append(pivoted_df)
    
    return processed_dfs