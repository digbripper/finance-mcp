FROM python:3.11-slim

WORKDIR /app

# Install dependencies first (layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy server code
COPY finance_mcp_server.py .

# Copy the BOE CSV
COPY nys_boe_data/parsed_contributions.csv nys_boe_data/

# Copy LDA registrants for in-memory cross-reference
COPY lda_registrants.csv .

# nyc_super_voters.csv and nyc_voters.db are downloaded from GitHub Releases
# at startup — see VOTER_CSV_RELEASE_URL and VOTER_DB_RELEASE_URL in server code

# Railway injects PORT at runtime
ENV PORT=8000

EXPOSE 8000

CMD ["python", "finance_mcp_server.py"]
