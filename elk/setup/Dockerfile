# Build context is the repository root (see docker-compose.yml), so the image
# can include the shared schema/ folder.
FROM python:3.12-slim
WORKDIR /app
COPY schema/requirements.txt schema/requirements.txt
COPY elk/setup/requirements.txt elk/setup/requirements.txt
RUN pip install --no-cache-dir -r schema/requirements.txt -r elk/setup/requirements.txt
# Generate the protobuf code and mappings from the .proto files. Rebuilding
# the image (docker compose up -d --build) picks up schema changes.
COPY schema/ schema/
RUN python schema/generate.py
COPY elk/setup/setup.py elk/setup/setup.py
# -u: unbuffered output so prints show up in `docker compose logs` immediately
CMD ["python", "-u", "elk/setup/setup.py"]
