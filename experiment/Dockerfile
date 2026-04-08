FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app
COPY experiment/ /app/
COPY common/python/ /app/common/python/

RUN if [ -f requirements.txt ]; then pip install --no-cache-dir -r requirements.txt; fi

CMD ["sh", "-c", "echo 'experiment container started (starter Dockerfile)'; if [ -f main.py ]; then exec python main.py; fi; sleep infinity"]

