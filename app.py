"""
Redshift Watchdog - AI-Powered Database Investigation Tool
A Streamlit web application for investigating Redshift issues using natural language queries
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from typing import Optional
import time

# Import our custom modules
from config import Config
from db_connector import RedshiftConnector
from llm_service import LLMService

# Page configuration
st.set_page_config(
    page_title="Redshift Watchdog",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Hide password reveal eye icon
st.markdown("""
    <style>
    input[type='password'] + div button {
        display: none !important;
    }
    </style>
""", unsafe_allow_html=True)

# Custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .connection-status {
        padding: 1rem;
        border-radius: 0.5rem;
        margin-bottom: 1rem;
    }
    .connected {
        background-color: #d4edda;
        border: 1px solid #c3e6cb;
        color: #155724;
    }
    .disconnected {
        background-color: #f8d7da;
        border: 1px solid #f5c6cb;
        color: #721c24;
    }
    .query-box {
        background-color: #f8f9fa;
        padding: 1rem;
        border-radius: 0.5rem;
        border: 1px solid #dee2e6;
    }
</style>
""", unsafe_allow_html=True)

# Initialize session state
if 'db_connector' not in st.session_state:
    st.session_state.db_connector = RedshiftConnector()
if 'llm_service' not in st.session_state:
    try:
        st.session_state.llm_service = LLMService()
    except Exception as e:
        st.error(f"Failed to initialize LLM service: {str(e)}")
        st.session_state.llm_service = None
if 'query_history' not in st.session_state:
    st.session_state.query_history = []
if 'ai_history' not in st.session_state:
    st.session_state.ai_history = []
if 'ai_conversation' not in st.session_state:
    st.session_state.ai_conversation = []  # For N-turn context (last 5)

def display_connection_status():
    """Display a small green or red dot with 'Connected' or 'Disconnected' in the top-right corner."""
    status = st.session_state.db_connector.is_connected()
    dot = "<span style='font-size:1.2rem;vertical-align:middle;'>{}</span>".format("🟢" if status else "🔴")
    label = "<span style='font-size:1rem;color:{};margin-left:0.3rem;'>{}</span>".format(
        "#28a745" if status else "#dc3545",
        "Connected" if status else "Disconnected"
    )
    st.markdown(
        f"""
        <div style='position: absolute; top: 1.2rem; right: 2.5rem; z-index: 9999; display: flex; align-items: center;'>
            {dot}{label}
        </div>
        """,
        unsafe_allow_html=True
    )

def connection_sidebar():
    """Sidebar for database connection configuration"""
    st.sidebar.header("🔗 Database Connection")
    # Check if we have default config
    default_config = Config.get_redshift_params()
    # Connection form
    with st.sidebar.form("connection_form"):
        host = st.text_input("Host", value=default_config.get('host', ''))
        database = st.text_input("Database", value=default_config.get('database', ''))
        user = st.text_input("User", value=default_config.get('user', ''))
        # password input removed
        port = st.number_input("Port", min_value=1, max_value=65535, value=default_config.get('port', 5439))
        col1, col2 = st.columns(2)
        with col1:
            connect_btn = st.form_submit_button("Connect", type="primary")
        with col2:
            disconnect_btn = st.form_submit_button("Disconnect")
    # Handle connection
    if connect_btn:
        if all([host, database, user]):  # password removed from check
            with st.spinner("Connecting to database..."):
                success, message = st.session_state.db_connector.connect(
                    host=host,
                    database=database,
                    user=user,
                    password=default_config.get('password', ''),  # use password from env/config only
                    port=port
                )
            if success:
                st.sidebar.success(message)
                st.rerun()
            else:
                st.sidebar.error(message)
        else:
            st.sidebar.error("Please fill in all connection fields")
    if disconnect_btn:
        success, message = st.session_state.db_connector.disconnect()
        if success:
            st.sidebar.success(message)
            st.rerun()
        else:
            st.sidebar.error(message)

def sql_query_section():
    """Manual SQL query section"""
    st.header("💻 SQL Query")
    
    if not st.session_state.db_connector.is_connected():
        st.info("Please connect to a database to run SQL queries.")
        return
    
    # SQL input
    sql_query = st.text_area(
        "Enter your SQL query (SELECT statements only):",
        height=100,
        placeholder="SELECT * FROM your_table LIMIT 10;"
    )
    
    col1, col2 = st.columns([1, 1])
    with col1:
        run_query = st.button("Run Query", type="primary")
    with col2:
        explain_query = st.button("Explain Query")
    
    if run_query and sql_query:
        with st.spinner("Executing query..."):
            success, result = st.session_state.db_connector.execute_query(sql_query)
        
        if success:
            st.success(f"Query executed successfully! Found {len(result)} rows.")
            if not result.empty:
                st.dataframe(result, use_container_width=True)
                
                # Add to history
                st.session_state.query_history.append({
                    'query': sql_query,
                    'timestamp': time.time(),
                    'rows': len(result)
                })
                st.session_state.query_history = st.session_state.query_history[-10:]
            else:
                st.info("Query returned no results.")
        else:
            st.error(f"Query failed: {result}")
    
    if explain_query and sql_query and st.session_state.llm_service:
        with st.spinner("Explaining query..."):
            explanation = st.session_state.llm_service.explain_sql_query(sql_query)
        st.info(explanation)

def ai_investigation_section():
    """AI-powered investigation section (LLM-first, always show Force Kill)"""
    st.header("🤖 AI Investigation: Ask Anything About Your Redshift Database")
    st.markdown("<div style='margin-bottom:1rem;'><em>You can ask about performance, user data, query details, business intelligence, and more. The AI will generate SQL for any scope.</em></div>", unsafe_allow_html=True)

    if 'is_running' not in st.session_state:
        st.session_state.is_running = False
    if 'force_kill' not in st.session_state:
        st.session_state.force_kill = False
    if 'kill_message' not in st.session_state:
        st.session_state.kill_message = ""

    if not st.session_state.db_connector.is_connected():
        st.info("Please connect to a database to use AI investigation.")
        return

    if not st.session_state.llm_service:
        st.error("LLM service is not available. Please check your Gemini API configuration.")
        return

    question = st.text_area(
        "Ask a question about your data or Redshift system:",
        height=100,
        placeholder="What are the top 10 customers by revenue this month? Which queries are taking the longest time? Show table sizes. List all users."
    )

    col1, col2 = st.columns([1, 1])
    with col1:
        investigate = st.button("🔍 Investigate", type="primary", disabled=st.session_state.is_running)
    with col2:
        force_kill = st.button("🛑 Kill Investigation", type="secondary")
        if force_kill:
            st.session_state.force_kill = True
            st.session_state.is_running = False
            st.session_state.kill_message = "Investigation stopped. You can start a new one."
            st.rerun()

    if st.session_state.kill_message:
        st.warning(st.session_state.kill_message)
        st.session_state.kill_message = ""

    ai_result = None
    ai_sql = None
    ai_rows = None
    ai_success = False
    ai_error_message = None
    ai_timestamp = None

    if investigate and question and not st.session_state.is_running:
        st.session_state.is_running = True
        st.session_state.force_kill = False
        try:
            with st.spinner("Analyzing your question..."):
                schema_info = ""  # No unnecessary backend queries
                max_retries = 4
                attempt = 0
                success_flag = False
                sql_query = None
                llm_response = None
                error_message = None
                result = None
                conversation_history = st.session_state.ai_conversation[-5:] if st.session_state.ai_conversation else []
                while attempt < max_retries:
                    if st.session_state.force_kill:
                        st.session_state.is_running = False
                        st.session_state.kill_message = "Investigation stopped. You can start a new one."
                        st.rerun()
                    if attempt == 0:
                        gen_success, sql_query, llm_response = st.session_state.llm_service.natural_language_to_sql(
                            question, schema_info, conversation_history
                        )
                    else:
                        sql_query = st.session_state.llm_service.fix_failed_sql(
                            question, sql_query, error_message, schema_info, conversation_history
                        )
                        if not sql_query:
                            break
                    if st.session_state.force_kill:
                        st.session_state.is_running = False
                        st.session_state.kill_message = "Investigation stopped. You can start a new one."
                        st.rerun()
                    if sql_query:
                        exec_success, result = st.session_state.db_connector.execute_query(sql_query)
                        if exec_success:
                            success_flag = True
                            break
                        else:
                            error_message = result
                    else:
                        error_message = llm_response or "Could not generate SQL."
                    attempt += 1
                if st.session_state.force_kill:
                    st.session_state.is_running = False
                    st.session_state.kill_message = "Investigation stopped. You can start a new one."
                    st.rerun()
            ai_timestamp = time.time()
            if success_flag:
                ai_success = True
                ai_sql = sql_query
                ai_result = result
                ai_rows = len(result) if result is not None and not result.empty else 0
                # Generate a summary for context
                with st.spinner("Summarizing for context..."):
                    result_summary = st.session_state.llm_service.summarize_results(
                        question, sql_query, result, max_rows=5
                    ) if result is not None else "No results."
                # Add to AI history (keep last 10)
                st.session_state.ai_history.append({
                    'question': question,
                    'sql': ai_sql,
                    'timestamp': ai_timestamp,
                    'rows': ai_rows
                })
                st.session_state.ai_history = st.session_state.ai_history[-10:]
                # Add to conversation history (keep last 5)
                st.session_state.ai_conversation.append({
                    'question': question,
                    'sql': ai_sql,
                    'result_summary': result_summary
                })
                st.session_state.ai_conversation = st.session_state.ai_conversation[-5:]
                st.subheader("Generated SQL Query")
                st.code(sql_query, language="sql")
                st.subheader("Query Results")
                if not result.empty:
                    st.dataframe(result, use_container_width=True)
                    with st.spinner("Generating insights..."):
                        summary = st.session_state.llm_service.summarize_results(
                            question, sql_query, result
                        )
                    st.subheader("AI Analysis")
                    st.markdown(summary)
                    if len(result.columns) >= 2 and len(result) > 1:
                        st.subheader("Quick Visualization")
                        viz_col1, viz_col2 = st.columns(2)
                        with viz_col1:
                            if st.button("📊 Bar Chart"):
                                if len(result.columns) >= 2:
                                    fig = px.bar(result.head(20), x=result.columns[0], y=result.columns[1])
                                    st.plotly_chart(fig, use_container_width=True)
                        with viz_col2:
                            if st.button("📈 Line Chart"):
                                if len(result.columns) >= 2:
                                    fig = px.line(result.head(20), x=result.columns[0], y=result.columns[1])
                                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("Query returned no results.")
            else:
                ai_success = False
                ai_error_message = error_message
                st.error(f"Failed to generate or execute SQL after {max_retries} attempts. Last error: {error_message}")
        finally:
            st.session_state.is_running = False
            st.session_state.force_kill = False

def query_history_section():
    """Query history section"""
    st.header("🕑 AI Investigation History")
    if st.session_state.ai_history:
        for i, entry in enumerate(reversed(st.session_state.ai_history)):
            with st.expander(f"Q{len(st.session_state.ai_history) - i}: {entry['question']}"):
                st.code(entry['sql'], language="sql")
                st.caption(f"Rows: {entry['rows']} | Time: {time.ctime(entry['timestamp'])}")
    st.header("📜 SQL Query History")
    if st.session_state.query_history:
        for i, query_info in enumerate(reversed(st.session_state.query_history)):
            with st.expander(f"Query {len(st.session_state.query_history) - i}"):
                st.code(query_info['query'], language="sql")
                st.caption(f"Rows: {query_info['rows']} | Time: {time.ctime(query_info['timestamp'])}")

def main():
    """Main application"""
    # Header
    st.markdown('<h1 class="main-header">🔍 Redshift Watchdog</h1>', unsafe_allow_html=True)
    st.markdown('<div style="text-align:center;"><em>AI-Powered Database Investigation Tool</em></div>', unsafe_allow_html=True)

    # Sidebar
    connection_sidebar()

    # Connection status in top-right corner
    display_connection_status()

    # Tabs for different sections (remove Schema Explorer)
    tab1, tab2, tab3 = st.tabs(["🔍 AI Investigation", "💻 SQL Query", "📜 History"])

    with tab1:
        ai_investigation_section()
    with tab2:
        sql_query_section()
    with tab3:
        query_history_section()

    # Footer
    st.markdown("---")
    st.markdown("*Built with Gemini*")

if __name__ == "__main__":
    main() 