FROM python:3.11-slim

WORKDIR /app
COPY api/requirements.txt api/requirements.txt
RUN pip install --no-cache-dir -r api/requirements.txt

COPY common/ common/
COPY api/ api/

ENV PYTHONUNBUFFERED=1
EXPOSE 8000
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000", "--no-access-log"]
