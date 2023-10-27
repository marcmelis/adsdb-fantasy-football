# packages
import pandas as pd
import camelot
import PyPDF2
import glob
import re
import os
import duckdb
import numpy as np
from . import dtype_dictionaries 
from . import duck_db_helper

def __transform_month__(match):
    month_abbreviations = {
        "January": "01",
        "February": "02",
        "March": "03",
        "April": "04",
        "May": "05",
        "June": "06",
        "July": "07",
        "August": "08",
        "September": "09",
        "October": "10",
        "November": "11",
        "December": "12"
    }
    # Get the month abbreviation from the match
    month_abbrev = match.group(3)
    # Use the abbreviation to look up the full month name
    full_month = month_abbreviations.get(month_abbrev, month_abbrev)
    return f"{match.group(5)}-{full_month}-{match.group(1)}"

def __parse_date__(page_text):
    # The top of the page is always:  "Daily Weather Summary for Sunday 01 January 2023 \n".
    pre_index = page_text.find('day') + 3 # we cant be sure that the pdf if correctly read 100% so this should be quite generic
    post_index = page_text.find('Selected')
    # Get the date between the two token variables and add to the df
    date = re.sub(r'[^a-zA-Z0-9]', '', page_text[pre_index:post_index])
    formatted_date = re.sub(r'(\d{1,2})(\s*)([A-Za-z]+)(\s*)(\d{3,4})', __transform_month__, date) # make sure to format the date correctly for
    midnight = formatted_date + "T00:00"
    noon = formatted_date + "T12:00"
    print(midnight)
    return np.datetime64(midnight), np.datetime64(noon)

def __process_met_pdf__(file_path):
    dtype_dict, _ = dtype_dictionaries.create_dtype_dict(file_path)

    # This is the text at the top of the page where the weather tables are. We use it to know which pages we want to read
    search_string = 'Selected UK readings at (L) 0000 and (R) 1200 UTC'
    pattern = re.compile(r'\s*'.join(re.escape(word) for word in search_string.split())) # the whitespace can be read incorrectly so we allow for optional whitespace
    # This is the columns without the date. When we need the date we append it with ['Date'] + columns
    columns = ['Date', 'Station_no', 'Station_name', 'PRESS', 'WDIR', 'WSPD', 'CLOUD', 'TEMP', 'TDEW']
    df_dtypes = {'Date': "datetime64[s]", 'Station_no': int, 'Station_name': str, 'PRESS': float, 'WDIR': str, 'WSPD': float, 'CLOUD': float, 'TEMP': float, 'TDEW': float}
    #         '1200_PRESS', '1200_WDIR', '1200_WSPD', '1200_CLOUD', '1200_TEMP', '1200_TDEW']

    with open(file_path, 'rb') as pdf_raw:
        pdf = PyPDF2.PdfReader(pdf_raw)
        print(len(pdf.pages))

        df = pd.DataFrame(dtype_dict)
        i = 0
        while i < len(pdf.pages):
            page_text = pdf.pages[i].extract_text()
            if re.search(pattern, page_text):
                table = camelot.read_pdf(file_path, pages=str(i + 1))

                table_df = table[0].df
                table_df = table_df.iloc[2:] # cut of the header of the table

                midnight_df = table_df.iloc[:,0:8]
                noon_columns = table_df.columns[:2].union(table_df.columns[8:])
                noon_df = table_df[noon_columns]

                midnight_date, noon_date = __parse_date__(page_text)

                midnight_df.insert(0, "Date", midnight_date)
                midnight_df = midnight_df.replace("-", np.nan)
                midnight_df.columns = columns
                midnight_df = midnight_df.astype(df_dtypes)

                noon_df.insert(0, "Date", noon_date)
                noon_df = noon_df.replace("-", np.nan)
                noon_df.columns = columns
                noon_df = noon_df.astype(df_dtypes)

                df = pd.concat([df, midnight_df, noon_df], ignore_index=True)
                i += 6
            i += 1
    return df

def move_to_formatted(data_dir):

    data_path= os.path.join(data_dir, "landing", "persistent", "*", "*")
    # connect to the formatted zone database
    conn = duckdb.connect(os.path.join(data_dir, 'formatted_zone', 'formatted_zone.db'))

    for file in glob.glob(data_path):
        if os.path.isdir(file):
            continue
        base_name = os.path.basename(file) # Get last part of the filepath
        table_name = os.path.splitext(base_name)[0] # Remove the extension if there is one
        if duck_db_helper.table_exists(table_name, conn):
            continue
        print(f"Processing: {table_name}")
        # only move .csv and .pdf files to a table
        if file.split(".")[-1] == "csv":
            dtype_dict, date_columns = dtype_dictionaries.create_dtype_dict(table_name)
            df = pd.read_csv(file, dtype=dtype_dict, parse_dates=date_columns)

            conn.sql(f"CREATE TABLE \"{table_name}\" AS SELECT * FROM df")

        if file.split(".")[-1] == "pdf":
            # this will create a new table per pdf file
            df = __process_met_pdf__(file)

            conn.sql(f"CREATE TABLE \"{table_name}\" AS SELECT * FROM df")


    # close the connection
    conn.close()

def list_formatted_tables(data_dir):
    # CHECK THE TABLES ON THE DB
    conn = duckdb.connect(os.path.join(data_dir,'formatted_zone', 'formatted_zone.db'))
    tables = duck_db_helper.get_tables(conn)
    for table_name in tables:
        print(table_name)
        # df = conn.sql(f"SELECT * FROM \"{table_name}\";").df()
        # print(df.describe())
    conn.close()

