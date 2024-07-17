import streamlit as st
from snowflake.snowpark import Session
import pandas as pd
from snowflake.snowpark.context import get_active_session
import os
import configparser

st.set_page_config(layout="wide")
st.title("Email Conversion")

# Define a function to get a Snowflake session
@st.cache_resource
def get_session():
    try:
        return get_active_session()
    except:
        parser = configparser.ConfigParser()
        parser.read(os.path.join(os.path.expanduser('~'), ".snowsql/config"))
        section = "connections.demo_conn"
        pars = {
            "account": parser.get(section, "account"),
            "user": parser.get(section, "username"),
            "password": parser.get(section, "password"),
            "warehouse": parser.get(section, "warehousename"),
            "role": parser.get(section, "role")
        }
        return Session.builder.configs(pars).create()

# Define a function to execute a query and return a DataFrame
@st.cache_data
def get_dataframe(query):
    session = get_session()
    if session is None:
        st.error("Session is not initialized.")
        return None
    try:
        # Execute query and fetch results
        snow_df = session.sql(query).to_pandas()
        return snow_df
    except Exception as e:
        st.error(f"Failed to execute query or process data: {str(e)}")
        return None

# SQL query
query = """
    SELECT 
        *
    FROM 
        SALES_ANALYTICS.PUBLIC.FAIRMONT_EMAIL_CONVERSION_RESULTS
    """

# Use the function to retrieve data
df = get_dataframe(query)

# Rename columns
df.rename(columns={
    'year_month': 'Year Month',
    'count_id_notification_not_equal': 'Email/60',
    'count_id_fellowship_not_equal': 'Converted/60',
    'count_transid_transbook_not_equal': 'Booked/60',
    'sum_guests_transbook_not_equal': 'Guests/60',
    'sum_subtotalagree_transbook_not_equal': 'Value/60',
    'count_id_notification_equal': 'Email/7',
    'count_id_fellowship_equal': 'Converted/7',
    'count_transid_transbook_equal': 'Booked/7',
    'sum_guests_transbook_equal': 'Guests/7',
    'sum_subtotalagree_transbook_equal': 'Value/7',
    'conversion_percentage_not_equal': 'Conversion/60',
    'conversion_percentage_equal': 'Conversion/7'
}, inplace=True)

# Ensure 'Conversion/60' and 'Conversion/7' have 2 decimal places and include a percentage sign
df['Conversion/60'] = df['Conversion/60'].apply(lambda x: f'{x:.2f}%')
df['Conversion/7'] = df['Conversion/7'].apply(lambda x: f'{x:.2f}%')

# Order by 'Year Month' in descending order
df.sort_values(by='Year Month', ascending=False, inplace=True)

# Display the search input
st.markdown("## 🔍 Search the Table")
search_input = st.text_input("Type to search the table", "")

# Filter the dataframe based on the search input
if search_input:
    df = df[df.apply(lambda row: row.astype(str).str.contains(search_input, case=False).any(), axis=1)]

# Display the table result
if df is not None:
    st.markdown("## 📊 Table Result")
    st.dataframe(df, height=600, width=None)
