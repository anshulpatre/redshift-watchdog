"""
LLM Service for Redshift Watchdog
Handles Gemini integration for natural language to SQL conversion
"""
import logging
import re
from typing import Tuple, Optional
import google.generativeai as genai
from config import Config
import sqlparse

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class LLMService:
    """Service for LLM operations using Gemini"""
    
    def __init__(self):
        self.model = None
        self.initialize_gemini()
    
    def initialize_gemini(self):
        """Initialize Gemini API client"""
        try:
            genai.configure(api_key=Config.GOOGLE_API_KEY)
            self.model = genai.GenerativeModel(Config.GEMINI_MODEL)
            logger.info(f"Initialized Gemini model: {Config.GEMINI_MODEL}")
        except Exception as e:
            logger.error(f"Failed to initialize Gemini: {str(e)}")
            raise
    
    def validate_sql_safety(self, sql: str) -> Tuple[bool, str]:
        """Validate that SQL is safe (only SELECT statements)"""
        try:
            # Parse SQL
            parsed = sqlparse.parse(sql)
            if not parsed:
                return False, "Invalid SQL statement"
            
            # Check each statement
            for statement in parsed:
                # Get the first token that's not whitespace/comment
                first_token = None
                for token in statement.flatten():
                    if token.ttype not in (sqlparse.tokens.Whitespace, 
                                         sqlparse.tokens.Comment.Single,
                                         sqlparse.tokens.Comment.Multiline):
                        first_token = token
                        break
                
                if not first_token:
                    continue
                
                # Check if it's a SELECT statement
                if first_token.ttype is sqlparse.tokens.Keyword.DML:
                    if first_token.value.upper() != 'SELECT':
                        return False, f"Only SELECT statements are allowed. Found: {first_token.value.upper()}"
                elif first_token.ttype is sqlparse.tokens.Keyword:
                    # Handle other keywords that might be dangerous
                    dangerous_keywords = ['INSERT', 'UPDATE', 'DELETE', 'DROP', 'CREATE', 
                                        'ALTER', 'TRUNCATE', 'GRANT', 'REVOKE']
                    if first_token.value.upper() in dangerous_keywords:
                        return False, f"Dangerous statement detected: {first_token.value.upper()}"
            
            return True, "SQL is safe"
            
        except Exception as e:
            logger.error(f"SQL validation error: {str(e)}")
            return False, f"SQL validation error: {str(e)}"
    
    def extract_sql_from_response(self, response: str) -> Optional[str]:
        """Extract SQL query from LLM response"""
        # Look for SQL in code blocks
        sql_pattern = r'```(?:sql)?\s*(.*?)\s*```'
        matches = re.findall(sql_pattern, response, re.DOTALL | re.IGNORECASE)
        
        if matches:
            return matches[0].strip()
        
        # If no code blocks, look for SELECT statements
        select_pattern = r'(SELECT\s+.*?)(?:\n\n|\Z)'
        matches = re.findall(select_pattern, response, re.DOTALL | re.IGNORECASE)
        
        if matches:
            return matches[0].strip()
        
        return None
    
    def natural_language_to_sql(self, question: str, schema_info: str, conversation_history=None) -> Tuple[bool, str, str]:
        logger.info(f"LLMService: Received question: {question}")
        try:
            # Build conversation context
            history_prompt = ""
            if conversation_history:
                for idx, turn in enumerate(conversation_history):
                    history_prompt += f"Previous Turn {idx+1}:\nQuestion: {turn['question']}\nSQL: {turn['sql']}\nResult Summary: {turn['result_summary']}\n\n"
            prompt = f"""
{history_prompt}
Current Question: {question}

Instructions:
- If the current question is a follow-up or refers to the previous result(s), use the previous context to generate the SQL.
- If the current question is unrelated, ignore the previous context and answer it independently.
- Always use Amazon Redshift official documentation for SQL syntax and best practices.

Database Schema Information:
{schema_info}

Rules:
1. Generate ONLY SELECT statements (no INSERT, UPDATE, DELETE, DROP, etc.)
2. Use proper Redshift SQL syntax
3. Include appropriate WHERE clauses, JOINs, and aggregations as needed
4. Return the SQL query wrapped in ```sql ``` code blocks
5. Be precise with column names and table references
6. Handle NULL values appropriately
7. Use LIMIT if the result set might be very large

Please provide a SQL query that answers this question:
"""
            
            # Generate response
            response = self.model.generate_content(prompt)
            
            if not response or not response.text:
                return False, "", "No response from LLM"
            
            # Extract SQL from response
            sql_query = self.extract_sql_from_response(response.text)
            
            if not sql_query:
                return False, "", "Could not extract SQL query from response"
            
            # Validate SQL safety
            is_safe, safety_message = self.validate_sql_safety(sql_query)
            if not is_safe:
                return False, "", f"Unsafe SQL detected: {safety_message}"
            
            return True, sql_query, response.text
            
        except Exception as e:
            logger.error(f"LLM query generation failed: {str(e)}")
            return False, "", f"LLM query generation failed: {str(e)}"
    
    def summarize_results(self, question: str, sql_query: str, results_df, max_rows: int = 100) -> str:
        logger.info(f"LLMService: Summarizing results for question: {question} | SQL: {sql_query}")
        try:
            # Prepare results summary
            if results_df.empty:
                results_summary = "No results found."
            else:
                row_count = len(results_df)
                col_count = len(results_df.columns)
                
                # Show first few rows as sample
                sample_rows = min(max_rows, row_count)
                sample_data = results_df.head(sample_rows).to_string(index=False)
                
                results_summary = f"""
Query returned {row_count} rows and {col_count} columns.

Sample data (first {sample_rows} rows):
{sample_data}

Columns: {', '.join(results_df.columns.tolist())}
"""
                
                if row_count > sample_rows:
                    results_summary += f"\n... and {row_count - sample_rows} more rows"
            prompt = f"""
You are a data analyst. Please provide a clear, concise summary of the query results in natural language.

When referencing SQL or database behavior, always use the official Amazon Redshift documentation as your source of truth.

Original Question: {question}

SQL Query Used:
{sql_query}

Query Results:
{results_summary}

Please provide:
1. A direct answer to the original question
2. Key insights from the data
3. Any notable patterns or findings
4. Keep the response conversational and easy to understand

Summary:
"""
            
            response = self.model.generate_content(prompt)
            
            if response and response.text:
                return response.text
            else:
                return "Could not generate summary of results."
                
        except Exception as e:
            logger.error(f"Result summarization failed: {str(e)}")
            return f"Result summarization failed: {str(e)}"
    
    def explain_sql_query(self, sql_query: str) -> str:
        logger.info(f"LLMService: Explaining SQL query: {sql_query}")
        try:
            prompt = f"""
Please explain this SQL query in plain English. Make it easy to understand for someone who may not be familiar with SQL.

When referencing SQL syntax or functions, always use the official Amazon Redshift documentation as your reference.

SQL Query:
{sql_query}

Explanation:
"""
            
            response = self.model.generate_content(prompt)
            
            if response and response.text:
                return response.text
            else:
                return "Could not generate explanation."
                
        except Exception as e:
            logger.error(f"SQL explanation failed: {str(e)}")
            return f"SQL explanation failed: {str(e)}"
    
    def suggest_related_queries(self, question: str, schema_info: str) -> str:
        """Suggest related questions that might be interesting"""
        try:
            prompt = f"""
Based on the database schema and the user's question, suggest 3-5 related questions that might provide additional insights.

Database Schema:
{schema_info}

User's Question: {question}

Please suggest related questions that would be interesting to explore:
"""
            
            response = self.model.generate_content(prompt)
            
            if response and response.text:
                return response.text
            else:
                return "Could not generate suggestions."
                
        except Exception as e:
            logger.error(f"Query suggestions failed: {str(e)}")
            return f"Query suggestions failed: {str(e)}" 

    def fix_failed_sql(self, question: str, failed_sql: str, error_message: str, schema_info: str, conversation_history=None) -> str:
        """Ask the LLM to fix a failed SQL query given the error message, with conversation context"""
        logger.info(f"LLMService: Attempting to fix failed SQL. Error: {error_message}")
        # Build conversation context
        history_prompt = ""
        if conversation_history:
            for idx, turn in enumerate(conversation_history):
                history_prompt += f"Previous Turn {idx+1}:\nQuestion: {turn['question']}\nSQL: {turn['sql']}\nResult Summary: {turn['result_summary']}\n\n"
        prompt = f"""
{history_prompt}
Current Question: {question}

You generated this SQL:
```sql
{failed_sql}
```

But it failed with this error:
{error_message}

Database Schema Information:
{schema_info}

Please correct the SQL so it works, and return only the corrected SQL in a code block. Only generate SELECT statements.

Always use the official Amazon Redshift documentation as your reference for SQL syntax, functions, and best practices. Do not guess; if unsure, follow Redshift's official docs.

Instructions:
- If the current question is a follow-up or refers to the previous result(s), use the previous context to generate the SQL.
- If the current question is unrelated, ignore the previous context and answer it independently.
"""
        try:
            response = self.model.generate_content(prompt)
            if response and response.text:
                sql_query = self.extract_sql_from_response(response.text)
                return sql_query
            else:
                return None
        except Exception as e:
            logger.error(f"LLM auto-fix failed: {str(e)}")
            return None 