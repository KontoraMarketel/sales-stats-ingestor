import json
import aioboto3


async def upload_to_minio(
        endpoint_url: str,
        access_key: str,
        secret_key: str,
        bucket: str,
        data: str,
        key: str,
):
    if not isinstance(data, str):
        data = json.dumps(data, ensure_ascii=False)

    session = aioboto3.Session()
    async with session.client(
            service_name='s3',
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key
    ) as s3:
        await s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=data.encode("utf-8"),
            ContentType="application/json"
        )


async def download_from_minio(
        endpoint_url: str,
        access_key: str,
        secret_key: str,
        bucket: str,
        key: str,
) -> dict:
    session = aioboto3.Session()
    async with session.client(
            service_name='s3',
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key
    ) as s3:
        try:
            response = await s3.get_object(Bucket=bucket, Key=key)
            content = await response['Body'].read()
            return json.loads(content.decode("utf-8"))
        except s3.exceptions.NoSuchKey:
            raise FileNotFoundError(f"No such key: {key}")
