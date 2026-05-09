FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py ./

EXPOSE 4567

# Single worker — state lives in module globals.
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "4567", "--proxy-headers"]
