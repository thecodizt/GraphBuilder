import pandas as pd

def load_csv_files(file_paths):
    return [pd.read_csv(file) for file in file_paths]