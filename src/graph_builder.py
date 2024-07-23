import networkx as nx
import numpy as np
from collections import defaultdict

def create_graph(cross_sectional_table, temporal_table, timestamp):
    G = nx.Graph()
    
    # Identify the ID column (assuming it's the second column, after 'unique_id')
    id_col = cross_sectional_table.columns[1]
    
    # Add nodes
    for _, row in cross_sectional_table.iterrows():
        G.add_node(row[id_col], **row.to_dict())
    
    # Filter temporal data for the given timestamp
    temporal_slice = temporal_table[temporal_table['timestamp'] == timestamp]
    
    # Group by ID and variable
    grouped_data = defaultdict(lambda: defaultdict(list))
    for _, row in temporal_slice.iterrows():
        grouped_data[row[id_col]][row['variable']].append(row['value'])
    
    # Calculate correlations without pivoting
    variables = list(set(temporal_slice['variable']))
    correlations = defaultdict(dict)
    
    for i, var1 in enumerate(variables):
        for j, var2 in enumerate(variables[i+1:], i+1):
            values1 = []
            values2 = []
            for id_values in grouped_data.values():
                if var1 in id_values and var2 in id_values:
                    values1.extend(id_values[var1])
                    values2.extend(id_values[var2])
            if values1 and values2:
                # Convert to numeric and handle non-numeric values
                values1_numeric = np.array(values1, dtype=float)
                values2_numeric = np.array(values2, dtype=float)
                mask = ~(np.isnan(values1_numeric) | np.isnan(values2_numeric))
                if np.sum(mask) > 1:  # Need at least 2 valid pairs to compute correlation
                    corr = np.corrcoef(values1_numeric[mask], values2_numeric[mask])[0, 1]
                    correlations[var1][var2] = corr
                    correlations[var2][var1] = corr
    
    # Add edges based on correlations
    threshold = 0.7  # Correlation threshold for creating an edge
    for id1 in grouped_data:
        for id2 in grouped_data:
            if id1 != id2:
                edge_weight = 0
                for var1 in grouped_data[id1]:
                    for var2 in grouped_data[id2]:
                        if var1 in correlations and var2 in correlations[var1]:
                            if abs(correlations[var1][var2]) > threshold:
                                edge_weight += abs(correlations[var1][var2])
                if edge_weight > 0:
                    G.add_edge(id1, id2, weight=edge_weight)
    
    return G

def build_graphs(cross_sectional_table, temporal_table):
    timestamps = temporal_table['timestamp'].unique()
    return {t: create_graph(cross_sectional_table, temporal_table, t) for t in timestamps}