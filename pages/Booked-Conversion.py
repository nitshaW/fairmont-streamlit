import streamlit as st
from snowflake.snowpark import Session
import plotly.express as px
import pandas as pd
from snowflake.snowpark.context import get_active_session
import os
import configparser

st.set_page_config(layout="wide")
st.title("Booked-Conversion Analysis")

# Define a function to get a Snowflake session
@st.cache_resource
def get_session():
    try:
        return get_active_session()
    except:
        pars = {
            "account": st.secrets["snowflake"]["account"],
            "user": st.secrets["snowflake"]["user"],
            "password": st.secrets["snowflake"]["password"],
            "warehouse": st.secrets["snowflake"]["warehouse"],
            "role": st.secrets["snowflake"]["role"],
            "database": st.secrets["snowflake"]["database"]
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

        # Perform preprocessing on Snowflake using Snowpark DataFrame operations
        snow_df = snow_df.drop_duplicates()

        # Replace null ITEM_NAME and DEPARTMENT with 'Unknown'
        snow_df['ITEM_NAME'] = snow_df['ITEM_NAME'].fillna('Unknown')
        snow_df['PRODUCT_CATEGORY'] = snow_df['PRODUCT_CATEGORY'].fillna('Unknown')

        # Convert 'Booked Year Month' from YYYYMM to datetime
        snow_df['BOOKED_MONTH'] = pd.to_datetime(snow_df['BOOKED_MONTH'].astype(str) + '01', format='%Y%m%d')

        # Rename columns
        snow_df.rename(columns={
            'BOOKED_MONTH': 'Booked Year Month',
            'ITEM_NAME': 'Item Name',
            'PRODUCT_CATEGORY': 'Department',
            'VIEWED': 'View',
            'CONVERSION': 'Conversion',
            'TRANSACTIONS': 'Gross Booked',
            'BOOKED': 'Net Booked',
            'ATTENDANCE': 'Attendance',
            'VALUE': 'Value',
            'CANCELLED': 'Cancelled',
            'OTHER_STATUS': 'Other Status'
        }, inplace=True)

        # Format 'Booked Year Month' to show only year and month
        snow_df['Booked Year Month'] = snow_df['Booked Year Month'].dt.strftime('%Y-%m')

        return snow_df
    except Exception as e:
        st.error(f"Failed to execute query or process data: {str(e)}")
        return None

# Function to convert DataFrame to CSV
def convert_df_to_csv(df):
    return df.to_csv(index=False).encode('utf-8')

# SQL query with fully qualified table name
query = """
    SELECT 
        *
    FROM 
        SALES_ANALYTICS.PUBLIC.FAIRMONT_REPORT_ITEMS
    """

# Use the function to retrieve data
df = get_dataframe(query)

# Check if df is not None before applying filters
if df is not None:
    # Interactive filters
    st.sidebar.header("Filters")
    selected_month = st.sidebar.multiselect("Select Booked Year Month", df['Booked Year Month'].unique())

    if selected_month:
        df = df[df['Booked Year Month'].isin(selected_month)]
    
    selected_department = st.sidebar.multiselect("Select Department", df['Department'].unique())
    
    if selected_department:
        df = df[df['Department'].isin(selected_department)]
    
    selected_item = st.sidebar.multiselect("Select Item Name", df['Item Name'].unique())

    if selected_item:
        df = df[df['Item Name'].isin(selected_item)]

    # Order data by 'Booked Year Month' in descending order
    df = df.sort_values(by='Booked Year Month', ascending=False)

    # Create plot
    value_chart_tab, value_dataframe_tab = st.tabs(["Chart", "Tabular Data"])
    
    with value_chart_tab:
        fig = px.line(df, x='Booked Year Month', y='Conversion', color='Item Name', title='Conversion Over Time', 
                      markers=True, hover_data=['Department'])
        st.plotly_chart(fig, use_container_width=True)
        
    with value_dataframe_tab:
        st.write("Booked-Conversion Data")
        renamed_columns = [
            'Booked Year Month', 'Item Name', 'Department', 'View', 'Conversion',
            'Gross Booked', 'Net Booked', 'Attendance', 'Value', 'Cancelled', 'Other Status'
        ]
        filtered_df = df[renamed_columns]

        # Calculate grand total row dynamically
        grand_total = filtered_df.select_dtypes(include=['number']).sum().to_frame().T
        grand_total.index = ['Grand Total']
        
        # Calculate average conversion ignoring inf values
        average_conversion = filtered_df['Conversion'].replace([float('inf'), float('-inf')], pd.NA).mean()
        grand_total['Conversion'] = average_conversion
        
        # Remove non-numeric columns from the grand total row
        grand_total = grand_total.reindex(columns=['View', 'Conversion', 'Gross Booked', 'Net Booked', 'Attendance', 'Value', 'Cancelled', 'Other Status'])
        
        # Rename 'Conversion' to 'Average Conversion' in grand total row
        grand_total.rename(columns={'Conversion': 'Average Conversion'}, inplace=True)
        
        # Round values to 2 decimal places
        grand_total = grand_total.round(2)

        # Format values to two decimal places as strings
        grand_total = grand_total.applymap(lambda x: f'{x:.2f}')
        
        # Display data without grand total row in full height
        st.dataframe(filtered_df, height=600, width=None)  

        # Display grand total row separately with fixed column widths
        st.write("Grand Total")
        grand_total_style = grand_total.style.set_properties(
            **{'text-align': 'left', 'white-space': 'nowrap', 'overflow': 'hidden', 'text-overflow': 'ellipsis'}
        )
        st.write(grand_total_style.to_html(), unsafe_allow_html=True)

        st.markdown("<br>", unsafe_allow_html=True)  # Add space above the download button
        
        # Allow CSV download
        filtered_df_with_total = pd.concat([filtered_df, grand_total])
        csv_data = convert_df_to_csv(filtered_df_with_total)
        st.download_button(label="Download Booked-Conversion as CSV", data=csv_data, file_name='booked_conversion_data.csv', mime='text/csv')

else:
    st.error("Failed to retrieve data.")
