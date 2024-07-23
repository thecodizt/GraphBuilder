import pandas as pd

def create_cross_sectional_table(dfs):
    # Combine all dataframes
    cross_sectional = pd.concat(dfs, axis=0, ignore_index=True)
    
    # Identify the ID column (assuming it's the second column, after 'unique_id')
    id_col = cross_sectional.columns[1]
    
    # Remove duplicate rows based on the ID column
    cross_sectional = cross_sectional.drop_duplicates(subset=[id_col])
    
    return cross_sectional

def create_temporal_table(dfs):
    temporal_data = []
    
    for df in dfs:
        id_col = df.columns[1]  # Assuming the ID column is the second column
        
        for col in df.columns[2:]:  # Skip 'unique_id' and ID column
            if '_' in col:
                variable, timestamp = col.rsplit('_', 1)
                temp_df = df[['unique_id', id_col, col]].copy()
                temp_df.columns = ['unique_id', id_col, 'value']
                temp_df['timestamp'] = pd.to_numeric(timestamp, errors='coerce')
                temp_df['variable'] = variable
                temporal_data.append(temp_df)
    
    temporal_table = pd.concat(temporal_data, ignore_index=True)
    return temporal_table