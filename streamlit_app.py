import streamlit as st
import json
import pandas as pd
import snowflake.connector
import matplotlib

"""
# Notehub to Snowflake Demo!

This demo pulls data from Snowflake that was routed from [this Notehub project](https://notehub.io/project/app:7580945e-58ae-424c-b254-5ec55ee2eeff/).

Each button press on a connected host is sent to the Notecard as a `note.add` with
the OS running on the Host MCU and a count of presses since last restart.

```json
{"req":"note.add","sync":true,"body":{"os":"zephyr","button_count":16}}
```

Raw JSON is routed to Snowflake using the Snowflake SQL API and transformed into
a structured data table using a view.

"""

"""
### Options
"""

with st.echo(code_location='below'):
    num_rows = st.slider('Rows to fetch?', 1, 50, 30)
    sort = st.selectbox('Sort?',('asc', 'desc'))

    # Initialize connection.
    @st.experimental_singleton
    def init_connection():
        return snowflake.connector.connect(**st.secrets["snowflake"])

    conn = init_connection()

    # Perform query.
    @st.experimental_memo(ttl=600)
    def run_query(query):
        with conn.cursor() as cur:
            cur.execute(query)
            return cur.fetchall()

    # Select rows from a snowflake view on top of JSON data
    rows = run_query(f'SELECT * from data_vw ORDER BY received {sort} limit {num_rows};')

    """
    ## Notecard `data.qo` Events
    """

    notecard_data = pd.DataFrame(rows, columns=("id", "Button Count", "OS", "Location", "Location Type", "Date"))
    table = notecard_data.style.background_gradient(cmap="Spectral")
    table

    """
    ### Summarized Data
    """

    group = notecard_data.groupby("OS")["Button Count"].count()
    group

    st.bar_chart(group)
