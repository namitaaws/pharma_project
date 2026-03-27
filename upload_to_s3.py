import boto3
import os

def upload_file_to_s3(local_file_path, bucket_name, s3_key):
    """
    Upload a file to AWS S3.

    :param local_file_path: Path to the local file to upload
    :param bucket_name: Name of the S3 bucket
    :param s3_key: Key (path) for the file in S3
    """
    # Create an S3 client
    s3_client = boto3.client('s3',
    aws_access_key_id='AKIA43ZU5OV7FVUVSWWL',
    aws_secret_access_key='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',
    region_name='ap-south-1')

    try:
        # Upload the file
        s3_client.upload_file(local_file_path, bucket_name, s3_key)
        print(f"File '{local_file_path}' uploaded successfully to s3://{bucket_name}/{s3_key}")
    except Exception as e:
        print(f"Error uploading file: {e}")

if __name__ == "__main__":
    # Example usage
    local_file = r"C:\Users\punee\Desktop\Namita\Python_Project\customers.json"  # Path to your sample file
    bucket_name = "pharma-file-upload"  # Replace with your actual S3 bucket name
    s3_key = "Bronze_layer_file/customers/customers.json"  # Desired key in S3

    # Check if file exists
    if os.path.exists(local_file):
        upload_file_to_s3(local_file, bucket_name, s3_key)
    else:
        print(f"File '{local_file}' does not exist.")