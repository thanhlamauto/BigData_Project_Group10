FROM python:3.11.15-slim
WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PYTHONPATH=/app:/app/members/TV5-Nguyen-Ngoc-Minh-rag-api-llm-tests
COPY services/indexer/requirements.txt /tmp/indexer.txt
COPY members/TV5-Nguyen-Ngoc-Minh-rag-api-llm-tests/requirements/runtime.txt /tmp/api.txt
RUN pip install --no-cache-dir --timeout 120 --retries 5 -r /tmp/indexer.txt -r /tmp/api.txt
COPY services /app/services
COPY data /app/data
COPY infra /app/infra
COPY scripts /app/scripts
COPY members/TV5-Nguyen-Ngoc-Minh-rag-api-llm-tests /app/members/TV5-Nguyen-Ngoc-Minh-rag-api-llm-tests
CMD ["python", "-m", "services.indexer.consumer"]
