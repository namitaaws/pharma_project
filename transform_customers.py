import boto3
import json
import pandas as pd
import re

def is_valid_email(email):
    """
    Validate email format.

    :param email: Email address to validate
    :return: True if valid, False otherwise
    """
    if not email:
        return False
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return re.match(pattern, email) is not None

def is_valid_phone(phone):
    """
    Validate phone number (must be 10 numeric digits only).

    :param phone: Phone number to validate
    :return: True if valid, False otherwise
    """
    if not phone:
        return False
    # Remove common phone formatting characters
    cleaned_phone = re.sub(r'[\s\-\(\)]', '', str(phone))
    # Check if all remaining characters are numeric and length is 10
    return cleaned_phone.isdigit() 
#and len(cleaned_phone) == 10

def is_valid_customer_id(customer_id):
    """
    Validate customer ID (must not be null).

    :param customer_id: Customer ID to validate
    :return: True if valid, False otherwise
    """
    return customer_id is not None

def read_from_s3(bucket_name, s3_key):
    """
    Read a JSON file from AWS S3.

    :param bucket_name: Name of the S3 bucket
    :param s3_key: Key (path) of the file in S3
    :return: Parsed JSON data
    """

  # Create an S3 client
    s3_client = boto3.client('s3',
    aws_access_key_id='AKIA43ZU5OV7FVUVSWWL',
    aws_secret_access_key='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',
    region_name='ap-south-1')
    try:
        obj = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
        data = json.loads(obj['Body'].read().decode('utf-8'))
        print(f"File '{s3_key}' read successfully from s3://{bucket_name}")
        return data
    except Exception as e:
        print(f"Error reading file from S3: {e}")
        return None

def transform_customers(customers):
    """
    Transform customer data by flattening nested structures and validating required fields.

    :param customers: List of customer dictionaries
    :return: List of transformed (flattened) customer dictionaries with valid records only
    """
    transformed = []
    invalid_records = []
    
    for idx, customer in enumerate(customers):
        customer_id = customer.get("customer_id")
        email = customer.get("email")
        phone = customer.get("phone")
        
        # Validate required fields
        validation_errors = []
        if not is_valid_customer_id(customer_id):
            validation_errors.append("customer_id is null")
        if not is_valid_email(email):
            validation_errors.append(f"email '{email}' is invalid")
        if not is_valid_phone(phone):
            validation_errors.append(f"phone '{phone}' is not valid")
        
        if validation_errors:
            invalid_records.append({
                "row_index": idx,
                "customer_id": customer_id,
                "errors": validation_errors
            })
            continue  # Skip this record
        
        flat_customer = {
            "customer_id": customer_id,
            "first_name": customer.get("first_name"),
            "last_name": customer.get("last_name"),
            "email": email,
            "phone": phone,
            "date_of_birth": customer.get("date_of_birth"),
            "street": customer.get("address", {}).get("street"),
            "city": customer.get("address", {}).get("city"),
            "state": customer.get("address", {}).get("state"),
            "country": customer.get("address", {}).get("country"),
            "zipcode": customer.get("address", {}).get("zipcode"),
            "account_number": customer.get("account", {}).get("account_number"),
            "account_type": customer.get("account", {}).get("account_type"),
            "balance": customer.get("account", {}).get("balance"),
            "currency": customer.get("account", {}).get("currency"),
            "created_at": customer.get("created_at")
        }
        transformed.append(flat_customer)
    
    # Print validation summary
    print(f"Total records: {len(customers)}")
    print(f"Valid records: {len(transformed)}")
    print(f"Invalid records: {len(invalid_records)}")
    
    if invalid_records:
        print("\nInvalid Records Summary:")
        for record in invalid_records[:10]:  # Show first 10 invalid records
            print(f"  Row {record['row_index']} (ID: {record['customer_id']}): {', '.join(record['errors'])}")
        if len(invalid_records) > 10:
            print(f"  ... and {len(invalid_records) - 10} more invalid records")
    
    return transformed

def write_json_to_s3(data, bucket_name, s3_key):
    """
    Write JSON data to AWS S3.

    :param data: Data to write (will be JSON serialized)
    :param bucket_name: Name of the S3 bucket
    :param s3_key: Key (path) for the file in S3
    """
    s3_client = boto3.client('s3',
    aws_access_key_id='AKIA43ZU5OV7FVUVSWWL',
    aws_secret_access_key='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',
    region_name='ap-south-1')
    try:
        json_str = json.dumps(data, indent=2)
        s3_client.put_object(Bucket=bucket_name, Key=s3_key, Body=json_str, ContentType='application/json')
        print(f"File '{s3_key}' written successfully to s3://{bucket_name}")
    except Exception as e:
        print(f"Error writing file to S3: {e}")


def write_parquet_to_s3(data, bucket_name, s3_key):
    """
    Write transformed data as Parquet to AWS S3.

    :param data: List of transformed dictionaries
    :param bucket_name: Name of the S3 bucket
    :param s3_key: Key (path) for the file in S3
    """
    import io

    s3_client = boto3.client('s3',
    aws_access_key_id='AKIA43ZU5OV7FVUVSWWL',
    aws_secret_access_key='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',
    region_name='ap-south-1')
    try:
        df = pd.DataFrame(data)
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False, engine='pyarrow')
        buffer.seek(0)
        s3_client.put_object(Bucket=bucket_name, Key=s3_key, Body=buffer.getvalue(), ContentType='application/octet-stream')
        print(f"Parquet file '{s3_key}' written successfully to s3://{bucket_name}")
    except Exception as e:
        print(f"Error writing Parquet to S3: {e}")
 
if __name__ == "__main__":
    # Configuration
    bucket_name = "pharma-file-upload"  # Replace with your actual S3 bucket name
    bronze_key = "Bronze_layer_file/customers/customers.json"  # Input file path in S3
    silver_key = "silver_layer_file/customers/customers_silver.parquet"  # Output file path in S3


    # Read data from Bronze layer
    customers_data = read_from_s3(bucket_name, bronze_key)
    if customers_data is None:
        print("Failed to read data from Bronze layer. Exiting.")
        exit(1)

    # Transform the data
    transformed_data = transform_customers(customers_data)

    # Optional: write JSON to silver layer
    #write_json_to_s3(transformed_data, bucket_name, silver_key.replace('.parquet', '.json'))

    # Write transformed data to Silver layer as Parquet
    write_parquet_to_s3(transformed_data, bucket_name, silver_key)
  
    print("Data transformation from Bronze to Silver layer completed successfully.")
