FROM python:3.11-slim

WORKDIR /app

# Install dependencies first (layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy server code
COPY finance_mcp_server.py .

# Copy the BOE CSV — only the CSV, not the giant zip files
COPY nys_boe_data/parsed_contributions.csv nys_boe_data/

# Copy LDA registrants for in-memory cross-reference
COPY lda_registrants.csv .

# Copy NYC super-voter file (416k active voters, 4+ GE votes)
COPY nyc_super_voters.csv .

# Railway injects PORT at runtime
ENV PORT=8000

EXPOSE 8000

CMD ["python", "finance_mcp_server.py"]
