import logging
from src import data_loader, data_transformer, table_manager, graph_builder, utils

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    
    try:
        logging.info("Loading data...")
        config = utils.load_config('config.yaml')
        dfs = data_loader.load_csv_files(config['input_files'])
        logging.info(f"Loaded {len(dfs)} dataframes")
        
        # Preprocess data
        logging.info("Preprocessing data...")
        processed_dfs = data_transformer.preprocess_data(dfs)
        
        # Create tables
        logging.info("Creating tables...")
        cross_sectional_table = table_manager.create_cross_sectional_table(processed_dfs)
        temporal_table = table_manager.create_temporal_table(processed_dfs)
        
        # Build graphs
        logging.info("Building graphs...")
        graphs = graph_builder.build_graphs(cross_sectional_table, temporal_table)
        
        # Further processing or saving of graphs
        for timestamp, graph in graphs.items():
            logging.info(f"Graph for timestamp {timestamp}: Nodes: {graph.number_of_nodes()}, Edges: {graph.number_of_edges()}")
        
        logging.info("Processing completed successfully.")
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        raise

if __name__ == "__main__":
    main()