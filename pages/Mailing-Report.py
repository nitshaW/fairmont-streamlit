import streamlit as st
from snowflake.snowpark import Session
import pandas as pd
import os
import configparser
from datetime import datetime, timedelta
import json
import plotly.express as px

st.set_page_config(layout="wide")
st.title("📊 Mailing Report")

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
        return snow_df
    except Exception as e:
        st.error(f"Failed to execute query or process data: {str(e)}")
        return None

# SQL queries
query_mandrill = """
    SELECT 
        *
    FROM 
        SALES_ANALYTICS.PUBLIC.FAIRMONT_MANDRILL_NOTIFICATIONS
    """
    
query_conversion = """
    SELECT 
        *
    FROM 
        SALES_ANALYTICS.PUBLIC.FAIRMONT_EMAIL_CONVERSION
    """

# Use the function to retrieve data
mandrill_df = get_dataframe(query_mandrill)
conversion_df = get_dataframe(query_conversion)

if mandrill_df is not None and conversion_df is not None:
    # Convert timestamps to naive datetime
    mandrill_df['DATA_TS_DATE'] = pd.to_datetime(mandrill_df['DATA_TS_DATE']).dt.tz_localize(None)
    conversion_df['createtstamp_notification'] = pd.to_datetime(conversion_df['createtstamp_notification']).dt.tz_localize(None)

    # Date range filter for both dataframes
    start_date, end_date = st.sidebar.date_input("Select date range", [
        min(mandrill_df['DATA_TS_DATE'].min().date(), conversion_df['createtstamp_notification'].min().date()),
        max(mandrill_df['DATA_TS_DATE'].max().date(), conversion_df['createtstamp_notification'].max().date())
    ])

    # Ensure the end date includes the entire day
    end_date = end_date + timedelta(days=1)

    # Convert start_date and end_date to datetime
    start_date = pd.Timestamp(start_date)
    end_date = pd.Timestamp(end_date)

    st.write(f"Start Date: {start_date}")
    st.write(f"End Date: {end_date - timedelta(seconds=1)}")

    mandrill_df = mandrill_df[(mandrill_df['DATA_TS_DATE'] >= start_date) & (mandrill_df['DATA_TS_DATE'] < end_date)]
    conversion_df = conversion_df[(conversion_df['createtstamp_notification'] >= start_date) & (conversion_df['createtstamp_notification'] < end_date)]

    # Calculate metrics for "Automatic Emails 7 days"
    mandrill_7days = mandrill_df[(mandrill_df['NOTIFICATION_TAG'] == 'days:7') & (mandrill_df['DATA_SUBJECT'] == 'Get the most out of your time at Fairmont Banff Springs')]
    emails_sent_7 = mandrill_7days['DATA_ID'].nunique()
    emails_delivered_7 = mandrill_7days['SENT'].sum()
    emails_opened_7 = mandrill_7days['OPEN'].sum()
    avg_delivery_rate_7 = emails_delivered_7 / emails_sent_7 if emails_sent_7 else 0
    total_clicks_7 = mandrill_7days['DATA_CLICKS'].sum()
    emails_with_click_7 = mandrill_7days['CLICKS'].sum()
    ctr_7 = emails_with_click_7 / emails_delivered_7 if emails_delivered_7 else 0
    avg_open_rate_7 = emails_opened_7 / emails_delivered_7 if emails_delivered_7 else 0

    # Calculate conversion rate for "Automatic Emails 7 days"
    conversion_7days = conversion_df[(conversion_df['extra_notification'] == 'days:7') & (conversion_df['subject_notification'] == 'Get the most out of your time at Fairmont Banff Springs')]
    conversion_rate_7 = (conversion_7days['id_fellowship'].dropna().nunique() / conversion_7days['id_notification'].dropna().nunique()) if conversion_7days['id_notification'].dropna().nunique() else 0

    # Calculate attendance and quantity for "Automatic Emails 7 days"
    attendance_7 = conversion_7days['guests_transbook'].sum()
    quantity_7 = conversion_7days['qty_transbook'].sum()

    # Display metrics for "Automatic Emails 7 days"
    st.markdown("## 📅 Automatic Emails 7 days")
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Emails Sent", emails_sent_7)
    col2.metric("Emails Delivered", emails_delivered_7)
    col3.metric("Emails Opened", emails_opened_7)
    col4.metric("AVG delivery rate", f"{avg_delivery_rate_7:.2%}")

    col5, col6, col7, col8 = st.columns(4)
    col5.metric("Total Clicks", total_clicks_7)
    col6.metric("Emails With at Least 1 Click", emails_with_click_7)
    col7.metric("Click Rate (CTR)", f"{ctr_7:.2%}")
    col8.metric("AVG Open Rate", f"{avg_open_rate_7:.2%}")

    col9, col10, col11, col12 = st.columns(4)
    col9.metric("Conversion Rate", f"{conversion_rate_7:.2%}")
    col10.metric("Attendance", attendance_7)
    col11.metric("Quantity", quantity_7)
    col12.metric("", "")

    # Calculate metrics for "Automatic Emails 60 days"
    mandrill_60days = mandrill_df[((mandrill_df['NOTIFICATION_TAG'] != 'days:7') | (mandrill_df['NOTIFICATION_TAG'].isna()) | (mandrill_df['NOTIFICATION_TAG'] == '')) & (mandrill_df['DATA_SUBJECT'] == 'Get the most out of your time at Fairmont Banff Springs')]
    emails_sent_60 = mandrill_60days['DATA_ID'].nunique()
    emails_delivered_60 = mandrill_60days['SENT'].sum()
    emails_opened_60 = mandrill_60days['OPEN'].sum()
    avg_delivery_rate_60 = emails_delivered_60 / emails_sent_60 if emails_sent_60 else 0
    total_clicks_60 = mandrill_60days['DATA_CLICKS'].sum()
    emails_with_click_60 = mandrill_60days['CLICKS'].sum()
    ctr_60 = emails_with_click_60 / emails_delivered_60 if emails_delivered_60 else 0
    avg_open_rate_60 = emails_opened_60 / emails_delivered_60 if emails_delivered_60 else 0

    # Calculate conversion rate for "Automatic Emails 60 days"
    conversion_60days = conversion_df[((conversion_df['extra_notification'] != 'days:7') | (conversion_df['extra_notification'].isna()) | (conversion_df['extra_notification'] == '')) & (conversion_df['subject_notification'] == 'Get the most out of your time at Fairmont Banff Springs')]
    conversion_rate_60 = (conversion_60days['id_fellowship'].dropna().nunique() / conversion_60days['id_notification'].dropna().nunique()) if conversion_60days['id_notification'].dropna().nunique() else 0

    # Calculate attendance and quantity for "Automatic Emails 60 days"
    attendance_60 = conversion_60days['guests_transbook'].sum()
    quantity_60 = conversion_60days['qty_transbook'].sum()

    # Display metrics for "Automatic Emails 60 days"
    st.markdown("## 📅 Automatic Emails 60 days")
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Emails Sent", emails_sent_60)
    col2.metric("Emails Delivered", emails_delivered_60)
    col3.metric("Emails Opened", emails_opened_60)
    col4.metric("AVG delivery rate", f"{avg_delivery_rate_60:.2%}")

    col5, col6, col7, col8 = st.columns(4)
    col5.metric("Total Clicks", total_clicks_60)
    col6.metric("Emails With at Least 1 Click", emails_with_click_60)
    col7.metric("Click Rate (CTR)", f"{ctr_60:.2%}")
    col8.metric("AVG Open Rate", f"{avg_open_rate_60:.2%}")

    col9, col10, col11, col12 = st.columns(4)
    col9.metric("Conversion Rate", f"{conversion_rate_60:.2%}")
    col10.metric("Attendance", attendance_60)
    col11.metric("Quantity", quantity_60)
    col12.metric("", "")

    # Calculate metrics for "Guest Services Emails"
    guest_services = mandrill_df[(mandrill_df['DATA_SUBJECT'] == 'Personalize My Guest Experience at Fairmont Banff Springs') | (mandrill_df['NOTIFICATION_TAG'] == 'Personalize My Guest Experience at Fairmont Banff Springs')]
    emails_sent_gs = guest_services['DATA_ID'].nunique()
    emails_delivered_gs = guest_services['SENT'].sum()
    emails_opened_gs = guest_services['OPEN'].sum()
    avg_delivery_rate_gs = emails_delivered_gs / emails_sent_gs if emails_sent_gs else 0
    total_clicks_gs = guest_services['DATA_CLICKS'].sum()
    emails_with_click_gs = guest_services['CLICKS'].sum()
    ctr_gs = emails_with_click_gs / emails_delivered_gs if emails_delivered_gs else 0
    avg_open_rate_gs = emails_opened_gs / emails_delivered_gs if emails_delivered_gs else 0

    # Display metrics for "Guest Services Emails"
    st.markdown("## 💼 Guest Services Emails")
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Emails Sent", emails_sent_gs)
    col2.metric("Emails Delivered", emails_delivered_gs)
    col3.metric("Emails Opened", emails_opened_gs)
    col4.metric("AVG delivery rate", f"{avg_delivery_rate_gs:.2%}")

    col5, col6, col7, col8 = st.columns(4)
    col5.metric("Total Clicks", total_clicks_gs)
    col6.metric("Emails With at Least 1 Click", emails_with_click_gs)
    col7.metric("Click Rate (CTR)", f"{ctr_gs:.2%}")
    col8.metric("AVG Open Rate", f"{avg_open_rate_gs:.2%}")

    # Calculate metrics for "General Data"
    st.markdown("## 📊 General Data")
    # Emails Sent Metrics
    emails_sent_state = mandrill_df.groupby('DATA_STATE').size().reset_index(name='Total Emails Sent')
    # Open Frequency Metrics
    open_frequency = mandrill_df.groupby('DATA_OPENS').size().reset_index(name='Total Opens')

    # Display General Data
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("### 📧 Emails Sent")
        st.dataframe(emails_sent_state)
    with col2:
        st.markdown("### 🔄 Open Frequency")
        st.dataframe(open_frequency)

    # Calculate device comparisons for opens
    def parse_json(detail):
        try:
            return json.loads(detail)
        except json.JSONDecodeError:
            return []

    mandrill_df['DATA_OPENS_DETAIL'] = mandrill_df['DATA_OPENS_DETAIL'].fillna('[]').apply(parse_json)
    mandrill_df['device_type'] = mandrill_df['DATA_OPENS_DETAIL'].apply(lambda details: [
        'mobile' if d and d.get('ua') and 'Mobile' in d['ua'] else 'desktop' if d and d.get('ua') and ('Windows' in d['ua'] or 'Linux' in d['ua'] or 'OS X' in d['ua']) else 'unknown' for d in details
    ])
    
    mandrill_df['mobile_opens'] = mandrill_df.apply(lambda row: row['device_type'].count('mobile') if row['OPEN'] == 1 else 0, axis=1)
    mandrill_df['desktop_opens'] = mandrill_df.apply(lambda row: row['device_type'].count('desktop') if row['OPEN'] == 1 else 0, axis=1)
    mandrill_df['unknown_opens'] = mandrill_df.apply(lambda row: row['device_type'].count('unknown') if row['OPEN'] == 1 else 0, axis=1)

    opens_by_date = mandrill_df.groupby(mandrill_df['DATA_TS_DATE'].dt.date).agg({
        'mobile_opens': 'sum',
        'desktop_opens': 'sum',
        'unknown_opens': 'sum'
    }).reset_index().rename(columns={'DATA_TS_DATE': 'date'})

    # Calculate device comparisons for clicks
    mandrill_df['DATA_CLICKS_DETAIL'] = mandrill_df['DATA_CLICKS_DETAIL'].fillna('[]').apply(parse_json)
    mandrill_df['device_type_clicks'] = mandrill_df['DATA_CLICKS_DETAIL'].apply(lambda details: [
        'mobile' if d and d.get('ua') and 'Mobile' in d['ua'] else 'desktop' if d and d.get('ua') and ('Windows' in d['ua'] or 'Linux' in d['ua'] or 'OS X' in d['ua']) else 'unknown' for d in details
    ])

    mandrill_df['mobile_clicks'] = mandrill_df.apply(lambda row: row['device_type_clicks'].count('mobile') if row['CLICKS'] == 1 else 0, axis=1)
    mandrill_df['desktop_clicks'] = mandrill_df.apply(lambda row: row['device_type_clicks'].count('desktop') if row['CLICKS'] == 1 else 0, axis=1)
    mandrill_df['unknown_clicks'] = mandrill_df.apply(lambda row: row['device_type_clicks'].count('unknown') if row['CLICKS'] == 1 else 0, axis=1)

    clicks_by_date = mandrill_df.groupby(mandrill_df['DATA_TS_DATE'].dt.date).agg({
        'mobile_clicks': 'sum',
        'desktop_clicks': 'sum',
        'unknown_clicks': 'sum'
    }).reset_index().rename(columns={'DATA_TS_DATE': 'date'})

    # Merge opens and clicks data
    device_comparisons = pd.merge(opens_by_date, clicks_by_date, on='date', how='outer').fillna(0)

    # Plot the data using Plotly Express
    device_comparisons_melted = device_comparisons.melt(id_vars='date', value_vars=[
        'mobile_opens', 'desktop_opens', 'unknown_opens', 'mobile_clicks', 'desktop_clicks', 'unknown_clicks'
    ], var_name='device_metric', value_name='count')

    device_comparisons_melted['type'] = device_comparisons_melted['device_metric'].apply(lambda x: 'Opens' if 'opens' in x else 'Clicks')
    device_comparisons_melted['device'] = device_comparisons_melted['device_metric'].apply(lambda x: 'Mobile' if 'mobile' in x else 'Desktop' if 'desktop' in x else 'Unknown')

    fig = px.line(device_comparisons_melted, x='date', y='count', color='device_metric', line_dash='type',
                  title='Device Comparisons: Opens / Clicks',
                  labels={'count': 'Count', 'date': 'Date', 'device_metric': 'Device / Metric'},
                  category_orders={'device_metric': ['mobile_opens', 'desktop_opens', 'unknown_opens', 'mobile_clicks', 'desktop_clicks', 'unknown_clicks']})

    fig.update_layout(
        legend_title_text='Device / Metric',
        xaxis_title='Date',
        yaxis_title='Count',
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )

    st.plotly_chart(fig)

else:
    st.error("Failed to retrieve data.")
