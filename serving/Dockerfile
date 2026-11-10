FROM python:3.11-slim
RUN pip install --no-cache-dir "numpy<2" pandas==2.0.3 scikit-learn==1.3.2 mlflow-skinny==2.17.2 fastapi uvicorn
WORKDIR /app
COPY app.py .
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]