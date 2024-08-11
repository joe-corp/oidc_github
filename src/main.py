from fastapi import FastAPI, File, UploadFile
from fastapi.responses import JSONResponse
from fastapi.requests import Request
from fastapi.exceptions import HTTPException
import boto3
import os
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()

sts_client = boto3.client('sts')

response = sts_client.assume_role(
    RoleArn='arn:aws:iam::635933673805:role/devops_role',
    RoleSessionName='MySessionName'
)

credentials = response['Credentials']

assumed_role_session = boto3.Session(
    aws_access_key_id=credentials['AccessKeyId'],
    aws_secret_access_key=credentials['SecretAccessKey'],
    aws_session_token=credentials['SessionToken']
)

s3 = assumed_role_session.client('s3')

@app.get('/')
def index():
    return 200, "Hello World"

@app.post("/upload_image")
async def upload_image(file: UploadFile = File(...)):
    bucket_name = os.environ['BUCKET_NAME']

    try:
        image_data = await file.read()
        image_key = f"images/{file.filename}"

        s3.put_object(Body=image_data, Bucket=bucket_name, Key=image_key, ContentType=file.content_type)

        return JSONResponse(status_code=201, content={"message": "Image uploaded successfully"})
    except Exception as e:
        print(f"Error uploading image: {e}")
        raise HTTPException(status_code=400, detail="Error uploading image")