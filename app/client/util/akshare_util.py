import re
import pandas as pd

def convert_gdp_dataframe(df):
    df = _convert_gdp_dataframe_columns_to_english(df)
    df['DATE'] = df['QUARTER'].apply(_convert_gdp_dataframe_quarter_to_date)
    return df

def _convert_gdp_dataframe_columns_to_english(df):
    # Translation dictionary for column names
    col_translations = {
        '国内生产总值-绝对值': 'GDP',
        '国内生产总值-同比增长': 'GDP_YOY',
        '第一产业-绝对值': 'FIRST_INDUSTRY_GDP',
        '第一产业-同比增长': 'FIRST_INDUSTRY_GDP_YOY',
        '第二产业-绝对值': 'SECOND_INDUSTRY_GDP',
        '第二产业-同比增长': 'SECOND_INDUSTRY_GDP_YOY',
        '第三产业-绝对值': 'THIRD_INDUSTRY_GDP',
        '第三产业-同比增长': 'THIRD_INDUSTRY_GDP_YOY',
        '季度': 'QUARTER'
    }
    # Make a copy to avoid modifying the original dataframe
    df_english = df.copy()
    # Rename columns
    df_english.columns = [col_translations.get(col, col) for col in df.columns]
    return df_english

def _convert_gdp_dataframe_quarter_to_date(text):
    # Clean the input text
    text = text.strip()
    
    # Extract year and quarter information
    year_match = re.search(r'(\d{4})年', text)
    quarter_match = re.search(r'第(\d)-?(\d)?季度', text)
    
    if not year_match or not quarter_match:
        return None
    
    year = year_match.group(1)
    start_quarter = int(quarter_match.group(1))
    end_quarter = int(quarter_match.group(2)) if quarter_match.group(2) else start_quarter
    
    # Map quarter to month
    month = end_quarter * 3
    
    date_text = f"{year}-{month:02d}"
    return pd.to_datetime(date_text)