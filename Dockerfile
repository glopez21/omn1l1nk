FROM python:3.11-slim AS base

WORKDIR /app
COPY pyproject.toml uv.lock ./

RUN pip install --no-cache-dir uv && \
    uv export --no-hashes --no-dev -o requirements.txt && \
    pip install --no-cache-dir -r requirements.txt

COPY app/ ./app/

RUN pip install --no-cache-dir --no-deps -e .

EXPOSE 9000

HEALTHCHECK --interval=10s --timeout=5s --retries=3 \
  CMD python -c "import urllib.request, json; r = urllib.request.urlopen('http://localhost:9000/health'); assert json.loads(r.read())['status'] == 'healthy'"

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "9000"]
