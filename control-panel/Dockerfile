# Build context is the repository root (see docker-compose.yml), so the image
# can include the shared common/ folder.
FROM python:3.12-slim
WORKDIR /app
COPY control-panel/requirements.txt control-panel/requirements.txt
RUN pip install --no-cache-dir -r control-panel/requirements.txt
COPY common/ common/
COPY control-panel/ control-panel/
# -u: unbuffered output so prints show up in `docker compose logs` immediately
CMD ["python", "-u", "control-panel/app.py"]
