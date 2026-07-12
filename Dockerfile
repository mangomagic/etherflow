FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .
COPY templates/ templates/

# Bind all interfaces inside the container; keep annotations with the mounted
# data volume so they persist across container rebuilds.
ENV FLASK_RUN_HOST=0.0.0.0 \
    ETHERFLOW_ANNOTATIONS=data/annotations.json

EXPOSE 5000

CMD ["python", "app.py"]
