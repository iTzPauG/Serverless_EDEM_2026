"""
Script: App & BI

Favicon personalizado: spotify.png

Description:
This code reads data from BigQuery to create a Streamlit dashboard showing:  
- The total number of users in the database
- Number of users per location
- List of all users

EDEM. Master Big Data & Cloud 2025/2026
Professor: Javi Briones & Adriana Campos
"""
import streamlit as st
from google.cloud import bigquery
import pandas as pd
import altair as alt
import time

client = bigquery.Client()
st.set_page_config(
    page_title="Dashboard Clients",
    layout="wide",
    page_icon="Spotify.png"
)
st.title("KPIs Clients Dashboard")
# Fondo degradado: negro arriba, verde Spotify abajo
spotify_green = "#1DB954"
st.markdown(f"""
	<style>
	body {{
		background: linear-gradient(to bottom, #000000 0%, {spotify_green} 100%) !important;
	}}
	.stApp {{
		background: linear-gradient(to bottom, #000000 0%, {spotify_green} 100%) !important;
	}}
	</style>
""", unsafe_allow_html=True)

# Query to count distinct users
query_users = """
SELECT COUNT(DISTINCT User_ID) AS total_users
FROM `inspiring-bonus-481514-j4.edem_data.clients`
"""

# Query to count users per location
query_location = """
SELECT Location, COUNT(User_ID) AS users_count
FROM `inspiring-bonus-481514-j4.edem_data.clients`
GROUP BY Location
ORDER BY users_count DESC
"""

# Query to get all users
query_all_users = """
SELECT User_ID, First_Name, Last_Name, Location
FROM `inspiring-bonus-481514-j4.edem_data.clients`
ORDER BY User_ID
"""

# Execute queries
users_df = client.query(query_users).to_dataframe()
location_df = client.query(query_location).to_dataframe()
all_users_df = client.query(query_all_users).to_dataframe()


# Counter llamativo con animación y estilo personalizado
st.markdown(f"""
	<div style="
		display: flex;
		flex-direction: column;
		align-items: center;
		justify-content: center;
		height: 180px;
		background: rgba(0,0,0,0.15);
		border-radius: 20px;
		box-shadow: 0 4px 24px 0 rgba(0,0,0,0.25);
		margin-bottom: 32px;
	">
		<span style="font-size: 2.2rem; font-weight: 600; color: #fff; letter-spacing: 2px; margin-bottom: 0.5rem;">Total Users</span>
		<span style="font-size: 4.5rem; font-weight: 900; color: #fff; -webkit-text-stroke: 2px #1DB954; text-shadow: 0 2px 8px #1DB954; animation: pop 1s cubic-bezier(.17,.67,.83,.67);">{users_df['total_users'][0]}</span>
	</div>
	<style>
	@keyframes pop {{
		0% {{transform: scale(0.7);}}
		60% {{transform: scale(1.6);}}
		80% {{transform: scale(1.2);}}
		100% {{transform: scale(1);}}
	}}
	</style>
""", unsafe_allow_html=True)


# Display users per location con barras verdes Spotify usando Altair
st.subheader("Users per Location")
st.markdown("""
    <style>
    .element-container:has(.vega-embed) {margin-top: 40px !important;}
    </style>
""", unsafe_allow_html=True)

# Animación progresiva de las barras
location_df_top25 = location_df.sort_values('users_count', ascending=False).head(25)
bar_placeholder = st.empty()
for i in range(1, len(location_df_top25)+1):
	partial_df = location_df_top25.iloc[:i]
	bar_chart = alt.Chart(partial_df).mark_bar(
		color='#1DB954',
		cornerRadiusTopLeft=6,
		cornerRadiusTopRight=6
	).encode(
		x=alt.X('Location', sort='-y'),
		y=alt.Y('users_count', scale=alt.Scale(domain=[0, 10])),
		tooltip=['Location', 'users_count']
	).properties(
		width='container',
		height=400,
		padding={'top': 40, 'bottom': 0, 'left': 5, 'right': 5}
	)
	bar_placeholder.altair_chart(bar_chart, use_container_width=True)
	time.sleep(0.1)

# Añadir margen inferior al gráfico de barras
st.markdown("""
	<style>
	.element-container:has(.vega-embed) {margin-bottom: 40px;}
	</style>
""", unsafe_allow_html=True)

# Display full table of users
st.subheader("All Users")
st.dataframe(all_users_df)
