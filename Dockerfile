FROM apache/spark:3.5.3
USER root
RUN apt-get update && apt-get install -y python3-pip && rm -rf /var/lib/apt/lists/*
RUN pip3 install --no-cache-dir "numpy<2" mlflow==2.17.2 boto3 "sqlalchemy>=2.0,<2.1" scikit-learn==1.3.2 pandas==2.0.3