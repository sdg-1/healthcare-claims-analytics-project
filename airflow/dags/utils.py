import requests
import zipfile
import io
import pandas as pd

def download_zip(url):
    """Download a csv file from a URL."""
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an HTTPError for bad responses
        return io.BytesIO(response.content)
    except requests.exceptions.RequestException as e:
        raise Exception(f"Failed to download the file from {url}. Error: {e}")

# def extract_csv_from_zip(zip_content, delimiter=None):
#     """Extract CSV data from a zip file and load it into a list of DataFrames."""
#     dataframes = []
#     with zipfile.ZipFile(zip_content) as zip_file:
#         csv_files = [file_name for file_name in zip_file.namelist() if file_name.endswith(".csv")]
        
#         if not csv_files:
#             raise Exception("No CSV file found in the zip archive.")
        
#         for file_name in csv_files:
#             with zip_file.open(file_name) as csv_file:
#                 try:
#                     df = pd.read_csv(csv_file, delimiter=delimiter or '|')
#                     dataframes.append(df)
#                 except pd.errors.ParserError as e:
#                     raise Exception(f"Failed to parse {file_name}. Error: {e}")

#     return dataframes

def extract_csv_from_zip(zip_content, file_extensions=(".csv", ".txt"), encoding="utf-8", on_bad_lines='warn'):
    """
    Extract data from a zip file and load it into a list of DataFrames.
    
    Args:
        zip_content: The zip file content
        file_extensions: Tuple of allowed file extensions
        encoding: File encoding to use
        on_bad_lines: How to handle bad lines ('error', 'warn', 'skip')
    """
    dataframes = []
    with zipfile.ZipFile(zip_content) as zip_file:
        # Filter files by the specified extensions
        data_files = [file_name for file_name in zip_file.namelist() if file_name.endswith(file_extensions)]
        
        if not data_files:
            raise Exception(f"No files with extensions {file_extensions} found in the zip archive.")
        
        for file_name in data_files:
            with zip_file.open(file_name) as data_file:
                try:
                    # Set delimiter based on file extension
                    delimiter = '\t' if file_name.endswith('.txt') else ','
                    df = pd.read_csv(
                        data_file, 
                        delimiter=delimiter, 
                        encoding=encoding,
                        on_bad_lines=on_bad_lines
                    )
                    dataframes.append(df)
                except pd.errors.ParserError as e:
                    raise Exception(f"Failed to parse {file_name}. Error: {e}")

    return dataframes

