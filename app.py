import streamlit as st
import pandas as pd
import numpy as np
import folium
from streamlit.components.v1 import html
from geopy.distance import distance
import plotly.express as px
import requests
import os
import re

# *************  Import related to S3 **************************
from s3_scripts import *
# **************************************************************

# ==========================
# Load CSV from a specific directory
# ==========================
# @st.cache_data
# def load_data(directory=r"D:\deployment code\bhutan_app\csv_files"):
#     csv_files = [f for f in os.listdir(directory) if f.endswith(".csv")]
#     if not csv_files:
#         st.error(f"No CSV files found in {directory}")
#         return pd.DataFrame()
    
#     # Pick the oldest CSV by modified time
#     csv_files.sort(key=lambda x: os.path.getmtime(os.path.join(directory, x)))
#     file_path = os.path.join(directory, csv_files[0])
    
#     df = pd.read_csv(file_path)
#     return df

# df = load_data()

# *************************************** Changes - 2  ****************************************************
@st.cache_data
def get_available_ecmwf_csv_data():
    unsorted_ecmwf_obj_dict = {}
    sorted_ecmwf_obj_dict = {}
    s3c, bucket_name = get_s3_client()
    lst_grib_csv_objs = []
    s3_file_prefix = "ecmwfdata/"
    lst_grib_csv_objs = list_bucket_objects(bucket=bucket_name, s3_client=s3c, object_prefix=s3_file_prefix)
    if lst_grib_csv_objs:
        # create a unsorted dict first
        unsorted_ecmwf_obj_dict = {
            int(re.search(r'_fc_(\d+)\.csv', filename).group(1)): filename 
            for filename in lst_grib_csv_objs
        }

        # print(unsorted_ecmwf_obj_dict)
        # sort the above dict
        sorted_ecmwf_obj_dict = dict(sorted(unsorted_ecmwf_obj_dict.items()))
        print(sorted_ecmwf_obj_dict)
    s3c.close()

    return sorted_ecmwf_obj_dict

@st.cache_data
def load_ecmwf_csv_data_for_by_day(day_key=1, dict_s3_obj={}):
    df = None
    # get the appropriate object name for the day_key
    fname_by_day = dict_s3_obj[day_key]
    # get the data for that object from S3
    s3c, bucket_name = get_s3_client()
    df = load_csv_from_s3_to_dataframe(s3_file_key=fname_by_day, bucket=bucket_name, s3_client=s3c)
    s3c.close()
    # return the dataframe
    return df

# Need to store the available objects in S3 - i.e. return of get_available_ecmwf_csv_data function
#  - put in session 
df_data = load_ecmwf_csv_data_for_by_day(day_key=2, dict_s3_obj=get_available_ecmwf_csv_data())
# ************************************************************************************************************


# ==========================
# Geocode location
# ==========================
def geocode_location(locality, gewog_thromde, dzongkhag):
    url = "https://nominatim.openstreetmap.org/search"
    query = f"{locality}, {gewog_thromde}, {dzongkhag}, Bhutan"
    params = {"q": query, "format": "json"}
    try:
        response = requests.get(url, params=params, headers={'User-Agent': 'forecast-app'})
        if response.status_code == 200 and response.json():
            data = response.json()[0]
            return float(data['lat']), float(data['lon'])
    except Exception as e:
        st.error(f"Geocoding error: {e}")
    return None, None

# ==========================
# Bilinear interpolation
# ==========================
def find_surrounding_points(df, lat, lon, param, time_col):
    df_param = df[df['param'] == param]
    latitudes = np.sort(df_param['latitude'].unique())
    longitudes = np.sort(df_param['longitude'].unique())

    lat_below = latitudes[latitudes <= lat].max() if np.any(latitudes <= lat) else None
    lat_above = latitudes[latitudes >= lat].min() if np.any(latitudes >= lat) else None
    lon_left = longitudes[longitudes <= lon].max() if np.any(longitudes <= lon) else None
    lon_right = longitudes[longitudes >= lon].min() if np.any(longitudes >= lon) else None

    if lat_below is None or lat_above is None or lon_left is None or lon_right is None:
        return None

    Q11 = df_param[(df_param['latitude'] == lat_below) & (df_param['longitude'] == lon_left)][time_col].values
    Q21 = df_param[(df_param['latitude'] == lat_below) & (df_param['longitude'] == lon_right)][time_col].values
    Q12 = df_param[(df_param['latitude'] == lat_above) & (df_param['longitude'] == lon_left)][time_col].values
    Q22 = df_param[(df_param['latitude'] == lat_above) & (df_param['longitude'] == lon_right)][time_col].values

    if len(Q11) == 0 or len(Q21) == 0 or len(Q12) == 0 or len(Q22) == 0:
        return None

    return {
        'lat_below': lat_below,
        'lat_above': lat_above,
        'lon_left': lon_left,
        'lon_right': lon_right,
        'Q11': Q11[0],
        'Q21': Q21[0],
        'Q12': Q12[0],
        'Q22': Q22[0]
    }

def bilinear_interpolation(data, lat, lon):
    x1, x2 = data['lon_left'], data['lon_right']
    y1, y2 = data['lat_below'], data['lat_above']
    x, y = lon, lat

    Q11 = data['Q11']
    Q21 = data['Q21']
    Q12 = data['Q12']
    Q22 = data['Q22']

    denom = (x2 - x1) * (y2 - y1)
    if denom == 0:
        return None

    term1 = Q11 * (x2 - x) * (y2 - y)
    term2 = Q21 * (x - x1) * (y2 - y)
    term3 = Q12 * (x2 - x) * (y - y1)
    term4 = Q22 * (x - x1) * (y - y1)

    return (term1 + term2 + term3 + term4) / denom

# ==========================
# Initialize session_state
# ==========================
if 'forecast_clicked' not in st.session_state:
    st.session_state.forecast_clicked = False
if 'lat' not in st.session_state:
    st.session_state.lat = None
if 'lon' not in st.session_state:
    st.session_state.lon = None
if 'selected_param' not in st.session_state:
    st.session_state.selected_param = None

# ==========================
# Streamlit UI
# ==========================
st.set_page_config(layout="wide")

st.markdown("""
<div style="background-color:white; padding: 10px 5px 5px 5px; border-bottom: 2px solid #e6e6e6;">
    <h1 style="text-align: left; font-size: 60px; color: black; font-style: Calibri;">
        འབྲུག་ཆུ་རུད་ཀྱི་རྐྱེན་ངན་ཉེན་བརྡའི་དྲ་ངོས།<br>
        <span style="font-size: 30px;">Bhutan Weather Portal</span>
    </h1>
</div>
""", unsafe_allow_html=True)

st.sidebar.title(" ")  # Empty sidebar

# ======= Main Page Inputs =======
tab_weather_forecast, = st.tabs(["Weather Forecast"])

with tab_weather_forecast:

    col1, col2, col3 = st.columns(3)
    with col1:
        locality = st.text_input("Locality", value="Changzamtog")
    with col2:
        gewog_thromde = st.text_input("Gewog or Thromde", value="Thimphu Thromde")
    with col3:
        dzongkhag = st.text_input("Dzongkhag", value="Thimphu")

    # Button click to get forecast
    if st.button("Get Forecast"):
        lat, lon = geocode_location(locality, gewog_thromde, dzongkhag)
        if lat is None or lon is None:
            st.error("Location not found.")
            st.session_state.forecast_clicked = False
        else:
            st.session_state.lat = lat
            st.session_state.lon = lon
            st.session_state.forecast_clicked = True
            st.success(f"Location: Latitude {lat:.4f}, Longitude {lon:.4f}")

    # Only show map/chart if forecast has been clicked
    if st.session_state.forecast_clicked:
        # Update parameter options to include the new variable
        expected_params = ["temperature_celcius", "precipitation", "surface_area"]
        params = [p for p in expected_params if p in df_data['param'].unique()]

        time_cols = [c for c in df_data.columns if 'h' in c]

        # Set default parameter
        if st.session_state.selected_param is None:
            st.session_state.selected_param = "temperature_celcius" if "temperature_celcius" in params else params[0]

        # Layout: Map left, chart right
        col_map, col_chart = st.columns([1, 1])

        with col_map:
            m = folium.Map(location=[st.session_state.lat, st.session_state.lon], zoom_start=10)
            folium.Circle(
                location=[st.session_state.lat, st.session_state.lon],
                radius=10000,
                color="blue",
                fill=True,
                fill_color="blue",
                fill_opacity=0.2,
                popup="10 km radius"
            ).add_to(m)
            html(m._repr_html_(), height=500)

        with col_chart:
            # Updated dropdown
            selected_param = st.selectbox(
                "Select Parameter",
                options=params,
                key='selected_param'
            )

            results = []
            for time in time_cols:
                surrounding = find_surrounding_points(df_data, st.session_state.lat, st.session_state.lon, selected_param, time)
                if surrounding:
                    value = bilinear_interpolation(surrounding, st.session_state.lat, st.session_state.lon)
                    results.append((time, round(value, 2)))
                else:
                    results.append((time, "Insufficient data"))

            result_df = pd.DataFrame(results, columns=['Forecast Time', 'Interpolated Value'])

            # Filter numeric values for plotting
            plot_df = result_df[result_df['Interpolated Value'].apply(lambda x: isinstance(x, (int, float)))]
            if not plot_df.empty:
                fig = px.line(plot_df, x='Forecast Time', y='Interpolated Value',
                              title=f"{selected_param} over Time", markers=True)
                st.plotly_chart(fig, use_container_width=True)
