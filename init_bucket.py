import os, time, boto3

s3 = boto3.client(
    "s3",
    endpoint_url=os.environ["MLFLOW_S3_ENDPOINT_URL"],
    aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
)

for _ in range(30):
    try:
        s3.create_bucket(Bucket="mlflow")
        print("Bucket mlflow créé")
        break
    except s3.exceptions.BucketAlreadyOwnedByYou:
        print("Bucket mlflow existe déjà")
        break
    except Exception as e:
        print("MinIO pas prêt, nouvel essai...", e)
        time.sleep(2)