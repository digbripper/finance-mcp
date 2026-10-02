FROM python:3.11-slim

WORKDIR /app

# Install dependencies first (layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy server code
COPY finance_mcp_server.py .
# Influence scoring v2.0 (imported by the server)
COPY influence_v2.py .

# NYS BOE contributions are queried from Neon (nys_boe_contributions);
# boe_common.py holds the shared name normalization and loader.
COPY boe_common.py .

# Copy LDA registrants for in-memory cross-reference
COPY lda_registrants.csv .

# Copy NYC super voters CSV (416k high-engagement voters, 49MB)
# Powers find_super_voters tool — loaded into memory at startup
COPY nyc_super_voters.csv .

# Full voter DB (nyc_voters.db, 1GB) is downloaded from GitHub Releases
# at startup if not already present — see VOTER_DB_RELEASE_URL in server code

# Railway injects PORT at runtime
ENV PORT=8000

EXPOSE 8000

CMD ["python", "finance_mcp_server.py"]
