# 🔍 Redshift Watchdog

**AI-Powered Database Investigation Tool**

A Streamlit web application that enables natural language investigation of Amazon Redshift databases using Google Gemini 2.5 Flash. Ask questions in plain English and get SQL queries, results, and insights automatically.

## 🌟 Features

- **🤖 AI Investigation**: Ask questions in natural language and get SQL queries + results
- **💻 Manual SQL Queries**: Run SELECT queries directly with syntax highlighting
- **📊 Schema Explorer**: Browse schemas, tables, and column structures
- **🔒 Security First**: Only allows SELECT queries, validates all generated SQL
- **📜 Query History**: Track your investigation history
- **📈 Quick Visualizations**: Generate charts from query results
- **🔗 Easy Connection**: Simple sidebar for database connection management

## 🚀 Quick Start

### Prerequisites

- Python 3.8+
- Amazon Redshift cluster access
- Google Gemini API key

### Installation

1. **Clone and navigate to the project:**
```bash
cd DB-mcp/redshift-watchdog
```

2. **Install dependencies:**
```bash
pip install -r requirements.txt
```

3. **Set up your environment:**
Create a `.env` file in the project root:
```env
# Gemini API Configuration
GOOGLE_API_KEY=your_gemini_api_key_here
GEMINI_MODEL=gemini-2.5-flash

# Optional: Default Redshift Configuration
REDSHIFT_HOST=your-cluster.region.redshift.amazonaws.com
REDSHIFT_DATABASE=your_database
REDSHIFT_USER=your_username
REDSHIFT_PASSWORD=your_password
REDSHIFT_PORT=5439

# Application Configuration
DB_MCP_MODE=readonly
STREAMLIT_PORT=8501
```

4. **Run the application:**
```bash
streamlit run app.py
```

5. **Open your browser:**
Navigate to `http://localhost:8501`

## 📖 Usage Guide

### 1. **Database Connection**
- Use the sidebar to enter your Redshift connection details
- Click "Connect" to establish connection
- Connection status is displayed at the top of the main page

### 2. **AI Investigation** 🤖
- Ask questions in natural language like:
  - "What are the top 10 customers by revenue?"
  - "Show me all orders from last month"
  - "Which products have the highest sales?"
- Select the schema for context
- Click "🔍 Investigate" to generate and execute SQL
- Get AI-powered insights and summaries

### 3. **Manual SQL Queries** 💻
- Write SELECT queries directly
- Get syntax highlighting and results
- Use "Explain Query" to understand what your SQL does

### 4. **Schema Explorer** 📊
- Browse available schemas and tables
- View table structures and column details
- Sample data from tables quickly

### 5. **Query History** 📜
- Track all your queries and results
- Review past investigations
- See execution timestamps and row counts

## 🔧 Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `GOOGLE_API_KEY` | Your Gemini API key | Required |
| `GEMINI_MODEL` | Gemini model to use | `gemini-2.5-flash` |
| `REDSHIFT_HOST` | Redshift cluster endpoint | None |
| `REDSHIFT_DATABASE` | Database name | None |
| `REDSHIFT_USER` | Username | None |
| `REDSHIFT_PASSWORD` | Password | None |
| `REDSHIFT_PORT` | Port number | `5439` |
| `DB_MCP_MODE` | Access mode | `readonly` |
| `STREAMLIT_PORT` | Web app port | `8501` |

### Security Modes

- **readonly**: Only SELECT statements allowed (recommended)
- **readwrite**: SELECT, INSERT, UPDATE allowed
- **admin**: All statements allowed (use with caution)

## 🛡️ Security Features

- **SQL Validation**: Uses `sqlparse` to validate only SELECT statements
- **Query Safety**: Blocks dangerous SQL operations (DROP, DELETE, etc.)
- **Read-only Mode**: Default mode prevents data modification
- **Input Sanitization**: Validates all user inputs

## 🎯 Example Questions

Try asking these questions to get started:

**Data Exploration:**
- "Show me the structure of the users table"
- "What are all the tables in the sales schema?"
- "How many records are in each table?"

**Business Intelligence:**
- "What are the top 10 products by sales volume?"
- "Show me monthly revenue trends for the last year"
- "Which customers haven't placed orders recently?"

**Performance Analysis:**
- "What are the slowest running queries?"
- "Show me table sizes and row counts"
- "Which tables are accessed most frequently?"

## 🔍 How It Works

1. **Natural Language Processing**: Gemini 2.5 Flash converts your questions to SQL
2. **Schema Context**: Provides table/column information to the AI for accurate queries
3. **SQL Generation**: Creates safe, optimized SELECT statements
4. **Query Execution**: Runs queries against your Redshift cluster
5. **Result Analysis**: AI summarizes findings and provides insights
6. **Visualization**: Generates charts and graphs from results

## 📚 API Reference

### Core Components

- **`RedshiftConnector`**: Handles database connections and query execution
- **`LLMService`**: Manages Gemini API integration and SQL generation
- **`Config`**: Manages environment variables and configuration

### Key Methods

```python
# Database operations
connector.connect(host, database, user, password, port)
connector.execute_query(sql_query)
connector.list_schemas()
connector.list_tables(schema)
connector.describe_table(table, schema)

# AI operations
llm_service.natural_language_to_sql(question, schema_info)
llm_service.summarize_results(question, sql_query, results_df)
llm_service.explain_sql_query(sql_query)
```

## 🚨 Troubleshooting

### Common Issues

**Connection Failed:**
- Check your Redshift cluster is running and accessible
- Verify security group settings allow connections
- Ensure credentials are correct

**LLM Service Error:**
- Verify your Gemini API key is valid
- Check internet connectivity
- Ensure you haven't exceeded API rate limits

**Query Execution Failed:**
- Check SQL syntax and table/column names
- Verify you have necessary permissions
- Review query complexity and timeout settings

### Debug Mode

Enable debug logging by setting:
```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🙏 Acknowledgments

- Built with [Streamlit](https://streamlit.io/)
- Powered by [Google Gemini 2.5 Flash](https://ai.google.dev/)
- Database connectivity via [redshift-connector](https://github.com/aws/amazon-redshift-python-driver)
- Adapted from [my-redshift-mcp](https://github.com/anshulpatre/my-redshift-mcp)

## 📞 Support

For questions or issues:
1. Check the troubleshooting section above
2. Review the GitHub issues
3. Create a new issue with detailed information

---

**Happy investigating!** 🔍✨
