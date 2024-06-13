import streamlit as st
import snowflake.connector
import pandas as pd
import plotly.express as px

st.set_page_config(
    page_title="Streaming Service Analytics",
    layout="wide",
    initial_sidebar_state="expanded")

# Snowflake connection parameters
snowflake_config = {
    "user": st.secrets["user"],
    "password": st.secrets["password"],
    "account": st.secrets["account"],
    "database": st.secrets["database"],
    "schema": st.secrets["schema-level"]
}

# Establish Snowflake connection
conn = snowflake.connector.connect(**snowflake_config)
cur = conn.cursor()

# Fetch data
@st.cache_data(ttl=43200)
def fetch_data():
    cur.execute("""
        SELECT d.title_id, d.title_text, d.imdb_score, d.type, d.release_year, d.tv_rating,
               sh.streaming_service, g.genre_name,sh.added_date,
                CASE 
                    WHEN d.type = 'Movie' and RIGHT(d.runtime, 3) = 'min' THEN REPLACE(d.runtime,' min')::INT
                    WHEN d.type = 'Movie' and RIGHT(d.runtime, 4) = 'mins' THEN REPLACE(d.runtime,' mins')::INT
                    ELSE NULL 
                END AS movie_runtime_mins,
                CASE 
                    WHEN d.type = 'TV Show' and RIGHT(d.runtime, 6) = 'Season' THEN REPLACE(d.runtime,' Season')::INT
                    WHEN d.type = 'TV Show' and RIGHT(d.runtime, 7) = 'Seasons' THEN REPLACE(d.runtime,' Seasons')::INT
                    ELSE NULL 
                END AS tv_show_seasons
        FROM Details d
        JOIN Stream_history sh ON d.title_id = sh.title_id
        JOIN title_genre tg ON d.title_id = tg.title_id
        JOIN Genres g ON tg.genre_id = g.genre_id
        WHERE sh.current_status = 'true'
    """)
    data = cur.fetchall()
    columns = [desc[0] for desc in cur.description]
    return pd.DataFrame(data, columns=columns)

df = fetch_data()
df = df[pd.notnull(df['ADDED_DATE'])]
# Streamlit app
st.title("Streaming Service Analytics")
st.subheader('Content Analysis of Netflix, Hulu, Dinsey Plus, Hbo Max')

# Sidebar - Streaming service filter
services = df['STREAMING_SERVICE'].unique()
selected_services = st.sidebar.multiselect("Select Streaming Services", services, default=services)
content_types = df['TYPE'].unique()
selected_types = st.sidebar.multiselect("Select Content Type", content_types, default=content_types)
filtered_df = df[(df['STREAMING_SERVICE'].isin(selected_services)) & (df['TYPE'].isin(selected_types))]

# Charts
st.markdown("## Content Added Over Time")

with st.expander("Adjust Chart Settings"):
    # Convert ADDED_DATE to datetime
    df['ADDED_DATE'] = pd.to_datetime(df['ADDED_DATE'])

    # Date range selectors
    min_date, max_date = df['ADDED_DATE'].min(), df['ADDED_DATE'].max()
    start_date = pd.to_datetime(st.date_input("Start Date", value=2024-04-01, min_value=min_date, max_value=max_date, key="start_date"))
    end_date = pd.to_datetime(st.date_input("End Date", value=max_date, min_value=start_date, max_value=max_date, key="end_date"))
   
    # Streaming service selector (reusing the existing one)
    selected_services_time = st.multiselect(
        "Select Streaming Services",
        services,
        default=services,
        key="services_for_time_chart"
    )

    # Aggregation method
    agg_method = st.selectbox(
        "Select Aggregation Method",
        ["Daily","Weekly","Monthly"],
        index=0
    )

# Filter data based on date range and selected services
filtered_df_time = df[
    (df['ADDED_DATE'] >= start_date) &
    (df['ADDED_DATE'] <= end_date) &
    (df['STREAMING_SERVICE'].isin(selected_services_time))
]
gdf = filtered_df_time.copy()
filtered_df = filtered_df_time.drop(columns=['GENRE_NAME'])
filtered_df = filtered_df.drop_duplicates()

# Group by date and streaming service, then count titles
# Group by date and streaming service, then count titles
if agg_method == "Daily":
    time_unit = filtered_df['ADDED_DATE'].dt.date
elif agg_method == "Weekly":
    time_unit = filtered_df['ADDED_DATE'].dt.to_period('W').apply(lambda r: r.start_time)
else:  # Monthly
    time_unit = filtered_df['ADDED_DATE'].dt.to_period('M').apply(lambda r: r.start_time)
content_additions = filtered_df.groupby([time_unit, 'STREAMING_SERVICE']).size().unstack(fill_value=0)
content_additions.index = pd.to_datetime(content_additions.index)
content_additions = content_additions.loc[:, (content_additions != 0).any(axis=0)]
selected_services_time = content_additions.columns.unique().to_list()
# Create the line chart
fig_additions = px.line(content_additions, x=content_additions.index, y=selected_services_time,
                        title=f"Content Added Daily",
                        labels={'x': 'Date', 'y': 'Number of Titles Added', 'variable': 'Streaming Service'},
                        line_shape='spline',  # Makes lines smoother
                        markers=True)  # Adds markers at data points

fig_additions.update_layout(
    xaxis_title="Date",
    yaxis_title="Number of Titles Added",
    legend_title="Streaming Service",
    hovermode="x unified"
)
st.plotly_chart(fig_additions)
# TV Rating
tv_rating_counts = filtered_df['TV_RATING'].value_counts()
fig_tv_rating = px.bar(tv_rating_counts, x=tv_rating_counts.index, y=tv_rating_counts.values,
                        color=tv_rating_counts.index, title="Distribution of TV Ratings")
st.plotly_chart(fig_tv_rating)

tv_rating_df = filtered_df.groupby(['TV_RATING', 'STREAMING_SERVICE']).size().unstack(fill_value=0)
tv_rating_df_percent = tv_rating_df.apply(lambda x: x / x.sum() * 100, axis=1)
selected_services_rating = tv_rating_df.columns.unique().to_list()
fig_tv_rating = px.bar(tv_rating_df_percent, x=tv_rating_df_percent.index, y=selected_services_rating, 
                        title="TV Ratings Distribution by Streaming Service",
                        labels={'value': 'Percentage', 'variable': 'Streaming Service', 'index': 'TV Rating'},
                        color_discrete_sequence=px.colors.qualitative.Pastel)
fig_tv_rating.update_layout(barmode='stack', yaxis={'categoryorder':'total ascending'})
st.plotly_chart(fig_tv_rating)

# IMDB Score
fig_imdb = px.histogram(filtered_df, x="IMDB_SCORE", color="STREAMING_SERVICE",
                        marginal="box", title="IMDB Score Distribution")
st.plotly_chart(fig_imdb)

# Release Year
fig_release = px.histogram(filtered_df, x="RELEASE_YEAR", color="STREAMING_SERVICE",
                            marginal="box", title="Release Year Distribution")
st.plotly_chart(fig_release)

# Movie Runtime
movie_df = filtered_df[filtered_df['TYPE'] == 'Movie'].dropna(subset=['MOVIE_RUNTIME_MINS'])
if not movie_df.empty:
    fig_movie_runtime = px.box(movie_df, x="STREAMING_SERVICE", y="MOVIE_RUNTIME_MINS",
                                title="Movie Runtime Distribution (minutes)")
    st.plotly_chart(fig_movie_runtime)
else:
    st.write("No movie data available.")

tv_df = filtered_df[filtered_df['TYPE'] == 'TV Show'].dropna(subset=['TV_SHOW_SEASONS'])
if not tv_df.empty:
    fig_tv_seasons = px.box(tv_df, x="STREAMING_SERVICE", y="TV_SHOW_SEASONS",
                            title="TV Show Seasons Distribution")
    st.plotly_chart(fig_tv_seasons)
else:
    st.write("No TV show data available.")


# Genres
genre_counts = gdf['GENRE_NAME'].value_counts().nlargest(10)
fig_genres = px.pie(genre_counts, values=genre_counts.values, names=genre_counts.index,
                   title="Top 10 Genres", hole=0.3)
st.plotly_chart(fig_genres)

# Close Snowflake connection
cur.close()
conn.close()
