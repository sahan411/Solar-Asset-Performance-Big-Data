FROM python:3.11-slim

WORKDIR /app
COPY simulators/requirements.txt simulators/requirements.txt
RUN pip install --no-cache-dir -r simulators/requirements.txt

COPY common/ common/
COPY simulators/ simulators/

ENV PYTHONUNBUFFERED=1
# The command (which simulator to run) is set in docker-compose.yml.
