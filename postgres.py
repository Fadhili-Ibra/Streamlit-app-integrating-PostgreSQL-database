import streamlit as st
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
import plotly.express as px
import plotly.graph_objects as go

# Function to create the connection using psycopg2
def create_psycopg2_connection():
    conn = psycopg2.connect(
        host='write the name of the host',
        port='enter the port number',
        database='enter dbase name',
        username='Enter your user name for Postgres dbase',
        password='Enter your password for Postgres dbase'
    )
    return conn

# Function to load schema names from the server
def load_schema_names():
    conn = create_psycopg2_connection()
    connection_string = URL.create(
        drivername='postgresql+psycopg2',
        username='Enter your user name for Postgres dbase',
        password='Enter your password for Postgres dbase',
        host='write the name of the host',
        port='enter the port number',
        database='enter dbase name'
    )
    engine = create_engine(connection_string, creator=lambda: conn)

    query = """
        SELECT schema_name
        FROM information_schema.schemata
        WHERE schema_name NOT IN ('information_schema', 'pg_catalog')
    """
    schemas = pd.read_sql(query, engine)
    return schemas.iloc[:, 0].tolist(), engine

# Function to load table names from a specific schema
def load_table_names(schema_name, engine):
    query = f"""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = '{schema_name}'
    """
    tables = pd.read_sql(query, engine)
    return tables.iloc[:, 0].tolist()

# Function to load data from a specific table
def load_table_data(schema_name, table_name, engine):
    query = f'SELECT * FROM {schema_name}.{table_name}'
    table = pd.read_sql(query, engine)
    return table

# Fetch metrics
def fetch_metrics(conn, stage_filter=None, status_filter=None):
    cursor = conn.cursor()
    query = "SELECT COUNT(*) FROM claims.claims"
    filters = []
    params = []
    if stage_filter:
        filters.append("stage = %s")
        params.append(stage_filter)
    if status_filter:
        filters.append("status = %s")
        params.append(status_filter)
    if filters:
        query += " WHERE " + " AND ".join(filters)
    cursor.execute(query, tuple(params))
    total_claims = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM claims.prospective_pensioners")
    total_pensioners = cursor.fetchone()[0]

    cursor.execute("SELECT status, COUNT(*) FROM claims.claims GROUP BY status")
    claims_status = cursor.fetchall()

    claims_passed = sum(count for status, count in claims_status if status == 2)
    claims_queried = sum(count for status, count in claims_status if status == 0)

    return total_claims, total_pensioners, claims_passed, claims_queried

# Fetch claims monthly
def fetch_claims_monthly(conn, stage_filter=None, status_filter=None):
    cursor = conn.cursor()
    query = """
        SELECT TO_CHAR(created_date, 'Mon-YYYY') AS month, COUNT(*) AS count
        FROM claims.claims
    """
    filters = []
    params = []
    if stage_filter:
        filters.append("stage = %s")
        params.append(stage_filter)
    if status_filter:
        filters.append("status = %s")
        params.append(status_filter)
    if filters:
        query += " WHERE " + " AND ".join(filters)
    query += """
        GROUP BY TO_CHAR(created_date, 'Mon-YYYY')
        ORDER BY TO_DATE(TO_CHAR(created_date, 'Mon-YYYY'), 'Mon-YYYY')
    """
    cursor.execute(query, tuple(params))
    claims_monthly = cursor.fetchall()
    return pd.DataFrame(claims_monthly, columns=['month', 'count'])

# Fetch claims by status
def fetch_claims_by_status(conn, stage_filter=None):
    cursor = conn.cursor()
    query = """
        SELECT status, COUNT(*) AS count
        FROM claims.claims
    """
    filters = []
    params = []
    if stage_filter:
        filters.append("stage = %s")
        params.append(stage_filter)
    if filters:
        query += " WHERE " + " AND ".join(filters)
    query += " GROUP BY status"
    cursor.execute(query, tuple(params))
    claims_by_status = cursor.fetchall()
    return pd.DataFrame(claims_by_status, columns=['status', 'count'])

# Fetch claims by stage
def fetch_claims_by_stage(conn, stage_filter=None, status_filter=None):
    cursor = conn.cursor()
    query = """
        SELECT stage, COUNT(*) AS count
        FROM claims.claims
    """
    filters = []
    params = []
    if stage_filter:
        filters.append("stage = %s")
        params.append(stage_filter)
    if status_filter:
        filters.append("status = %s")
        params.append(status_filter)
    if filters:
        query += " WHERE " + " AND ".join(filters)
    query += " GROUP BY stage"
    cursor.execute(query, tuple(params))
    claims_by_stage = cursor.fetchall()
    return pd.DataFrame(claims_by_stage, columns=['stage', 'count'])

# Fetch claims verification register
def fetch_claims_verification_register(conn, stage_filter=None, status_filter=None):
    cursor = conn.cursor()
    query = """
        SELECT claim_id, stage, comments, created_by, created_date, approved_by
        FROM claims.claims
    """
    filters = []
    params = []
    if stage_filter:
        filters.append("stage = %s")
        params.append(stage_filter)
    if status_filter:
        filters.append("status = %s")
        params.append(status_filter)
    if filters:
        query += " WHERE " + " AND ".join(filters)
    cursor.execute(query, tuple(params))
    claims_verification = cursor.fetchall()
    return pd.DataFrame(claims_verification, columns=['claim_id', 'stage', 'comments', 'created_by', 'created_date', 'approved_by'])

# Modeling page
def modeling_page():
    st.title('Table Relationship & Filter Tool')

    # Load schema names and engine
    schema_names, engine = load_schema_names()

    # Step 1: Select a schema
    selected_schema_name = st.selectbox('Select Schema', schema_names)

    # Load table names based on the selected schema
    table_names = load_table_names(selected_schema_name, engine)

    # Step 2: Select a table (Table 1)
    selected_table_name_1 = st.selectbox('Select Table 1', table_names)
    selected_table_1 = load_table_data(selected_schema_name, selected_table_name_1, engine)
    
    st.write('Selected Table 1:')
    st.write(selected_table_1.head())

    # Step 3: Select the variable to link with another table
    link_variable_1 = st.selectbox('Select variable from Table 1 to link', selected_table_1.columns)

    # Step 4: Select another table to join (Table 2)
    selected_table_name_2 = st.selectbox('Select Table 2', table_names)
    selected_table_2 = load_table_data(selected_schema_name, selected_table_name_2, engine)

    st.write('Selected Table 2:')
    st.write(selected_table_2.head())

    # Step 5: Select the variable from the second table to link
    link_variable_2 = st.selectbox('Select variable from Table 2 to link', selected_table_2.columns)

    # Perform the join
    joined_table = selected_table_1.merge(selected_table_2, left_on=link_variable_1, right_on=link_variable_2, how='outer')

    st.write('Joined Table:')
    st.write(joined_table.head())

    # Step 6: View only the selected columns
    view_columns = st.multiselect('Select columns to view', list(joined_table.columns), default=list(joined_table.columns))

    # Display the final table with selected columns
    st.write('Selected Columns Table:')
    st.write(joined_table[view_columns].head())

# Report page
def report_page():
    st.title("Pension Analytics Dashboard")

    conn = create_psycopg2_connection()

    if conn:
        # Sidebar filters
        st.sidebar.header("Filters")
        stage_filter = st.sidebar.selectbox("Filter by Stage", options=[None] + fetch_claims_by_stage(conn)['stage'].tolist())
        status_filter = st.sidebar.selectbox("Filter by Status", options=[None, 0, 2])

        # Display metrics in tiles
        total_claims, total_pensioners, claims_passed, claims_queried = fetch_metrics(conn, stage_filter, status_filter)
        
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Claims", total_claims)
        col2.metric("Total Pensioners", total_pensioners)
        col3.metric("Claims Passed", claims_passed)
        col4.metric("Claims Queried", claims_queried)

        # Display charts
        st.subheader("Claims Incoming Register (Monthly)")
        claims_monthly_df = fetch_claims_monthly(conn, stage_filter, status_filter)
        fig = px.bar(claims_monthly_df, x='month', y='count', title="Claims Received (Monthly)")
        st.plotly_chart(fig)
        
        st.subheader("Claims by Status")
        claims_by_status_df = fetch_claims_by_status(conn, stage_filter)
        fig = px.pie(claims_by_status_df, names='status', values='count', title="Claims by Status")
        st.plotly_chart(fig)

        st.subheader("Claims by Stage")
        claims_by_stage_df = fetch_claims_by_stage(conn, stage_filter, status_filter)
        fig = go.Figure(data=[go.Pie(labels=claims_by_stage_df['stage'], values=claims_by_stage_df['count'], hole=.3)])
        fig.update_layout(title_text="Claims by Stage")
        st.plotly_chart(fig)

        # Claims Verification Register with filters
        st.subheader("Claims Verification Register")
        claims_verification_df = fetch_claims_verification_register(conn, stage_filter, status_filter)
        st.dataframe(claims_verification_df)

        conn.close()
    else:
        st.error("Failed to connect to the database.")

# Main app
def main():
    st.sidebar.title("Navigation")
    page = st.sidebar.radio("Go to", ["Modeling", "Report"])

    if page == "Modeling":
        modeling_page()
    elif page == "Report":
        report_page()

if __name__ == '__main__':
    main()
