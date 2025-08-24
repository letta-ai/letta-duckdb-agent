# letta-duckdb-agent
Example text-to-sql agent using Motherduck (DuckDB)

## Setup

### Prerequisites
- Python 3.13 or higher
- [uv](https://docs.astral.sh/uv/) package manager

### Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd letta-duckdb-agent
```

2. Install dependencies using uv:
```bash
uv sync
```

3. Create a `.env` file in the project root with your configuration:
```env
# Add your environment variables here
# Example:
# LETTA_API_KEY=your_api_key_here
# DUCKDB_CONNECTION_STRING=your_connection_string
```

4. Run the agent:
```bash
uv run python create_agent.py
```

### Dependencies
- `duckdb` - DuckDB database engine
- `letta-client` - Letta AI agent framework client
- `python-dotenv` - Environment variable management

All dependencies and their exact versions are specified in `pyproject.toml` and locked in `uv.lock`.
