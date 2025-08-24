import os
import duckdb
from letta_client import Letta, RequiredBeforeExitToolRule, ContinueToolRule
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

db_name = os.getenv('DATABASE_NAME')

instructions = f"""
You are a Credit Risk Assessment Agent with the following capabilities:

MISSION: Assess credit risk for clients with full transparency and regulatory compliance. You have been provided with a database containing client and trade data.

RISK FRAMEWORK: Use a 5-component risk model:
1. Credit Risk (35% weight): PD + Rating + Concentration
2. Market Risk (25% weight): Factor sensitivities
3. Stress Test Risk (25% weight): 36 stress scenarios
4. Operational Risk (10% weight): News sentiment
5. Country Risk (5% weight): Geographic concentration

Compute the overall risk provide by combining these weighted components. When complete, return a risk score and explanation with actionable recommendations.

You may explore the data by looking up the schema with the show_table_schemas tool and running queries with the execute_sql tool.

For analysis, use the run_code_with_queries tool to execute code that queries the database and analyzes the results. You can write code to query the database with the proper API key and analyze the results with the following: 
```
    import os 
    import duckdb
    motherduck_api_key = os.getenv('MOTHERDUCK_API_KEY')
    con = duckdb.connect('md:?motherduck_token=' + motherduck_api_key) 
    con.sql(f"USE {db_name}")
    
    # Execute the query and get results
    result = con.sql(query)
    rows = result.fetchall()
```
"""


def show_table_schemas():
    """
    Show the schemas of all tables in the current database.
    """
    import duckdb
    import os

    motherduck_api_key = os.getenv('MOTHERDUCK_API_KEY')
    con = duckdb.connect('md:?motherduck_token=' + motherduck_api_key) 


    # Run a query to verify the connection
    #con.sql("SHOW DATABASES").show()

    # show ths chemas
    database_name = os.getenv('DATABASE_NAME')
    con.sql(f"USE {database_name}")

    # show tables 
    #con.sql("SHOW TABLES").show()

    # show schemas for each table
    tables_result = con.sql("SHOW TABLES").fetchall()
    result_str = ""
    result_str += "\n=== TABLE SCHEMAS ===\n"
    
    for table in tables_result:
        table_name = table[0]
        result_str += f"\nTable: {table_name}\n"
        result_str += "-" * (len(table_name) + 7) + "\n"
        
        # Get column information
        schema_result = con.sql(f"DESCRIBE {table_name}").fetchall()
        
        for column in schema_result:
            column_name = column[0]
            column_type = column[1]
            is_nullable = "NULL" if column[2] else "NOT NULL"
            result_str += f"  • {column_name}: {column_type} ({is_nullable})\n"
        
        result_str += "\n"
    
    return result_str

def execute_sql(query: str):
    """
    Execute a SQL query and return results in LLM-friendly table format.

    Parameters:
        query (str): The SQL query to execute.

    Returns:
        str: The result of the query formatted as a readable table.
    """
    import os 
    import duckdb
    motherduck_api_key = os.getenv('MOTHERDUCK_API_KEY')
    con = duckdb.connect('md:?motherduck_token=' + motherduck_api_key) 
    database_name = os.getenv('DATABASE_NAME')
    con.sql(f"USE {database_name}")
    
    # Execute the query and get results
    result = con.sql(query)
    rows = result.fetchall()
    columns = [desc[0] for desc in result.description]
    
    if not rows:
        return "No results found."
    
    # Format as LLM-friendly table
    result_str = "\n=== QUERY RESULTS ===\n"
    result_str += f"Query: {query}\n"
    result_str += f"Rows returned: {len(rows)}\n\n"
    
    # Calculate column widths
    col_widths = [len(col) for col in columns]
    for row in rows:
        for i, cell in enumerate(row):
            col_widths[i] = max(col_widths[i], len(str(cell)))
    
    # Create header
    header = "| " + " | ".join(col.ljust(col_widths[i]) for i, col in enumerate(columns)) + " |"
    separator = "|" + "|".join("-" * (col_widths[i] + 2) for i in range(len(columns))) + "|"
    
    result_str += header + "\n"
    result_str += separator + "\n"
    
    # Add data rows
    for row in rows:
        row_str = "| " + " | ".join(str(cell).ljust(col_widths[i]) for i, cell in enumerate(row)) + " |"
        result_str += row_str + "\n"
    
    result_str += f"\nTotal rows: {len(rows)}\n"
    
    return result_str

def execute_sql_multiple(queries: list[str]):
    """
    Executes multiple SQL queries and returns results in LLM-friendly table format.

    Parameters:
        queries (list[str]): List of SQL queries to execute.

    Returns:
        str: Formatted results from all queries.
    """
    import os 
    import duckdb
    motherduck_api_key = os.getenv('MOTHERDUCK_API_KEY')
    con = duckdb.connect('md:?motherduck_token=' + motherduck_api_key) 
    database_name = os.getenv('DATABASE_NAME')
    con.sql(f"USE {database_name}")
    
    all_results = ""
    
    for i, query in enumerate(queries, 1):
        all_results += f"\n{'='*60}\n"
        all_results += f"QUERY {i} OF {len(queries)}\n"
        all_results += f"{'='*60}\n"
        
        # Execute the query and get results
        result = con.sql(query)
        rows = result.fetchall()
        columns = [desc[0] for desc in result.description]
        
        if not rows:
            all_results += f"Query: {query}\n"
            all_results += "No results found.\n"
            continue
        
        # Format as LLM-friendly table
        all_results += f"Query: {query}\n"
        all_results += f"Rows returned: {len(rows)}\n\n"
        
        # Calculate column widths
        col_widths = [len(col) for col in columns]
        for row in rows:
            for j, cell in enumerate(row):
                col_widths[j] = max(col_widths[j], len(str(cell)))
        
        # Create header
        header = "| " + " | ".join(col.ljust(col_widths[j]) for j, col in enumerate(columns)) + " |"
        separator = "|" + "|".join("-" * (col_widths[j] + 2) for j in range(len(columns))) + "|"
        
        all_results += header + "\n"
        all_results += separator + "\n"
        
        # Add data rows
        for row in rows:
            row_str = "| " + " | ".join(str(cell).ljust(col_widths[j]) for j, cell in enumerate(row)) + " |"
            all_results += row_str + "\n"
        
        all_results += f"\nTotal rows: {len(rows)}\n"
    
    return all_results

def generate_final_report(report: str): 
    """ Generate a final report from the risk analysis. 

    Parameters:
        report (str): The risk analysis report.
    """
    return 

import inspect
client = Letta(token=os.getenv('LETTA_API_KEY'))

execute_sql_tool = client.tools.upsert_from_function(
    func=execute_sql,
    pip_requirements=[{"name": "duckdb", "version": "1.3.1"}], 
    return_char_limit=50000
)
print(f"✅ Tool created successfully!", execute_sql_tool.id)

execute_sql_multiple_tool = client.tools.upsert_from_function(
    func=execute_sql_multiple,
    pip_requirements=[{"name": "duckdb", "version": "1.3.1"}], 
    return_char_limit=50000
)
print(f"✅ Tool created successfully!", execute_sql_multiple_tool.id)

show_table_schemas_tool = client.tools.upsert_from_function(
    func=show_table_schemas,
    pip_requirements=[{"name": "duckdb", "version": "1.3.1"}]
)
print(f"✅ Tool created successfully!", show_table_schemas_tool.id)

generate_final_report_tool = client.tools.upsert_from_function(
    func=generate_final_report,
    pip_requirements=[{"name": "duckdb", "version": "1.3.1"}]
)
print(f"✅ Tool created successfully!", generate_final_report_tool.id)

run_code_with_queries_tool = client.tools.upsert_from_function(
    func=run_code_with_queries,
    pip_requirements=[{"name": "duckdb", "version": "1.3.1"}]
)
print(f"✅ Tool created successfully!", run_code_with_queries_tool.id)

# persona 
persona = """
You are a Credit Risk Assessment Agent with the following capabilities:

MISSION: Assess credit risk for clients with full transparency and regulatory compliance. You have been provided with a database containing client and trade data.

RISK FRAMEWORK: Use a 5-component risk model:
1. Credit Risk (35% weight): PD + Rating + Concentration
2. Market Risk (25% weight): Factor sensitivities
3. Stress Test Risk (25% weight): 36 stress scenarios
4. Operational Risk (10% weight): News sentiment
5. Country Risk (5% weight): Geographic concentration

Compute the overall risk provide by combining these weightd components

WORKFLOW: Always follow this process:
- Understand the data available to you by querying the table schemas 
- Use SQL queries to analyze the data and calculate risk components
- Use the code interpreter to run calculations or run custom code for analysis 
- When completed, provide a risk score and explanation with actionable recommendations

QUERIES: Make sure you understand the schema and can run individual queries with execute_sql before trying to run many at a time. 
You also should make sure to use `Client` not `Parent Client`, which does not refer to the client.
"""

# create the agent
agent = client.agents.create(
    name="credit_risk_agent",
    memory_blocks=[
        {"label": "human", "value": "The user is Sarah, a credit risk analyst at Citi Bank."}, 
        {"label": "persona", "value": persona}
    ],
    tool_ids=[execute_sql_tool.id, show_table_schemas_tool.id, execute_sql_multiple_tool.id],
    tools=["run_code"],
    model="google_ai/gemini-2.5-pro",
    embedding="openai/text-embedding-3-small",
    tool_exec_environment_variables={
        "MOTHERDUCK_API_KEY": os.getenv("MOTHERDUCK_API_KEY"), 
        "DATABASE_NAME": os.getenv("DATABASE_NAME")
    },
    tool_rules=[
        #RequiredBeforeExitToolRule(tool_name="generate_final_report", type="required_before_exit"), 
        ContinueToolRule(tool_name="execute_sql", type="continue_loop"), 
        ContinueToolRule(tool_name="execute_sql_multiple", type="continue_loop"), 
        ContinueToolRule(tool_name="show_table_schemas", type="continue_loop"), 
    ]
)
print(f"✅ Agent created successfully!", agent.id)
