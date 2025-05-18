import streamlit as st
import pyodbc
import google.generativeai as genai
import pandas as pd
import re

# --- CONFIGURE GEMINI ---
genai.configure(api_key="AIzaSyAayWiTM2snxqs2P13daKGkf87LshVPSeQ")
model = genai.GenerativeModel("gemini-1.5-flash")

# --- DATABASE CONNECTION ---
server = 'q2jdkkazwpdufgw7l5isv2yegu-ulyatwdewx3ufjannhm2hew6yi.datawarehouse.fabric.microsoft.com'
database = 'bing_lake_db'
username = 'vanam.hemanth@isteer.com'

conn_str = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={server};"
    f"DATABASE={database};"
    f"Authentication=ActiveDirectoryInteractive;"
    f"UID={username};"
    f"Encrypt=yes;"
    f"TrustServerCertificate=no;"
)

# Initialize chat history list in session state
if 'chat_history' not in st.session_state:
    st.session_state.chat_history = []

# Initialize toggle state in session_state for chat history visibility
if 'show_history' not in st.session_state:
    st.session_state.show_history = False

# Function to check if prompt is data query
def is_data_query(prompt):
    keywords = ['show', 'list', 'news', 'sentiment', 'category', 'published', 'description', 'records', 'count']
    return any(word in prompt.lower() for word in keywords)

# Function to check if it's a DAX request
def is_dax_request(prompt):
    return 'dax' in prompt.lower() or 'write dax' in prompt.lower()

# Run SQL query directly (no threading)
def run_sql(query):
    try:
        with pyodbc.connect(conn_str) as conn:
            df = pd.read_sql(query, conn)
        return df
    except Exception as e:
        return f"SQL execution failed: {e}"

# Extract only the SQL query part from Gemini response
def extract_sql_only(generated_text):
    # Try to extract everything from the first SELECT to the first semicolon
    match = re.search(r"(SELECT[\s\S]+?;)", generated_text, re.IGNORECASE)
    if match:
        sql = match.group(1)
    else:
        # If no semicolon, try from SELECT till end of text
        select_match = re.search(r"(SELECT[\s\S]+)", generated_text, re.IGNORECASE)
        sql = select_match.group(1) if select_match else generated_text

    # Remove any backticks or unwanted characters
    sql = sql.replace("`", "")

    # Strip trailing and leading spaces
    return sql.strip()

# Generate DAX expression using Gemini
def generate_dax(prompt):
    schema = """
    Table: tbl_sentiment_analysis
    Columns:
    - title (Text)
    - description (Text)
    - category (Text)
    - url (URL)
    - image (Text)
    - provider (Text)
    - datePublished (Date)
    - sentiment (Text): values are "positive", "negative", or "neutral"
    """
    full_prompt = f"Based on this table schema, write a DAX expression: {prompt}\n{schema}"
    response = model.generate_content(full_prompt)
    return response.text

# Generate SQL from Gemini and run
def handle_data_query(prompt):
    schema = """
    Table: tbl_sentiment_analysis
    Columns:
    - title (Text)
    - description (Text)
    - category (Text)
    - url (URL)
    - image (Text)
    - provider (Text)
    - datePublished (Date)
    - sentiment (Text): values are "positive", "negative", or "neutral"
    """
    try:
        # Special case: today’s record count
        if "how many records" in prompt.lower() and "today" in prompt.lower():
            sql_query = """
                SELECT COUNT(*) AS RecordsIngestedToday
                FROM tbl_sentiment_analysis
                WHERE datePublished >= CAST(GETDATE() AS DATE)
                  AND datePublished < DATEADD(day, 1, CAST(GETDATE() AS DATE));
            """
            return run_sql(sql_query)

        # Ask Gemini to generate SQL with schema included
        sql_prompt = f"""
        You are an expert in SQL Server.
        Write a valid SQL Server query for the following request based on table 'tbl_sentiment_analysis'.
        Use SQL Server syntax only (e.g., FORMAT() for date formatting).
        Table schema:
        {schema}

        Request: {prompt}
        """
        response = model.generate_content(sql_prompt)
        sql_raw = response.text

        # Extract only the valid SQL part
        sql_query = extract_sql_only(sql_raw)

        # Fix datePublished conditions for SQL Server
        sql_query = re.sub(
            r"datePublished\s*=\s*CONVERT\(DATE,\s*GETDATE\(\)\)",
            "CAST(datePublished AS DATE) = CAST(GETDATE() AS DATE)",
            sql_query,
            flags=re.IGNORECASE
        )
        sql_query = re.sub(
            r"datePublished\s*=\s*CAST\(GETDATE\(\)\s+AS\s+DATE\)",
            "CAST(datePublished AS DATE) = CAST(GETDATE() AS DATE)",
            sql_query,
            flags=re.IGNORECASE
        )

        return run_sql(sql_query)

    except Exception as e:
        return f"Error: {e}"

# --- STREAMLIT UI ---
st.set_page_config(page_title="Smart News Chatbot", layout="centered")
st.title("📰 AI Chatbot for News & DAX")

prompt = st.text_input("Ask a question (data, general, or DAX)")

if prompt:
    # Save user prompt in chat history
    st.session_state.chat_history.append({"role": "user", "content": prompt})

    if is_dax_request(prompt):
        with st.spinner("Generating DAX..."):
            result = generate_dax(prompt)
            st.session_state.chat_history.append({"role": "bot", "content": result})
            st.code(result, language='DAX')

    elif is_data_query(prompt):
        with st.spinner("Querying Lakehouse..."):
            result = handle_data_query(prompt)
            if isinstance(result, pd.DataFrame):
                st.session_state.chat_history.append({"role": "bot", "content": result.to_string(index=False)})
                st.dataframe(result)
            else:
                st.session_state.chat_history.append({"role": "bot", "content": result})
                st.error(result)

    else:
        with st.spinner("Generating response..."):
            response = model.generate_content(prompt)
            st.session_state.chat_history.append({"role": "bot", "content": response.text})
            st.write(response.text)

# Toggle button for showing/hiding chat history
if st.button("Show/Hide Chat History"):
    st.session_state.show_history = not st.session_state.show_history

if st.session_state.show_history:
    st.markdown("---")
    st.subheader("Chat History")
    for chat in st.session_state.chat_history:
        if chat["role"] == "user":
            st.markdown(f"**You:** {chat['content']}")
        else:
            # Display bot's dataframes as tables if possible
            if '\n' in chat['content'] and '|' in chat['content']:
                st.text(chat['content'])
            else:
                st.markdown(f"**Bot:** {chat['content']}")
