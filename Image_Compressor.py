import pandas as pd
import requests
from PIL import Image
from io import BytesIO
import os
from uuid import uuid4
import boto3

process_status_map={}
output_dir = 'output_images'
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

async def upload_to_aws(local_file, bucket, s3_file):
    ACCESS_KEY = os.getenv('AMAZON_ACCESS_KEY')
    SECRET_KEY = os.getenv('AMAZON_SECRET_KEY')
    s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
    try:
        s3.upload_file(local_file, bucket, s3_file)
        return True
    except FileNotFoundError:
        return False

def validate_csv(df):
    required_columns = ['Serial Number', 'Product Name', 'Input Image Urls']
    for col in required_columns:
        if col not in df.columns:
            raise ValueError(f"Missing required column: {col}")   
    for index, row in df.iterrows():
        if pd.isna(row['Serial Number']) or pd.isna(row['Product Name']) or pd.isna(row['Input Image Urls']):
            raise ValueError(f"Missing data at row {index + 1}")

def process_status (image_url):
    if image_url not in process_status_map:
        return "Unprocessed"
    return process_status_map[image_url]
              
def process_image(image_url, output_quality=50):
    try:
        process_status_map[image_url]="Processing"
        response = requests.get(image_url)
        img = Image.open(BytesIO(response.content))
        
        output_image_name = f"{uuid4()}.jpg"
        output_image_path = os.path.join(output_dir, output_image_name)
        
        img.save(output_image_path, "JPEG", quality=output_quality)

        # await upload_to_aws(output_image_path,"First_bucket", output_image_path)
        # file_name = "https://First_bucket.s3.amazonaws.com/"+output_image_path
        # return file_name

        process_status_map[image_url]="Processed"
        return output_image_path
    except Exception as e:
        print(f"Failed to process image {image_url}: {e}")
        return "Processing Failed"

def process_images_from_csv(df):
    output_data = []

    for index, row in df.iterrows():
        input_urls = row['Input Image Urls'].split(',')
        output_urls = []

        for url in input_urls:
            output_path = process_image(url.strip())
            output_urls.append(f'file://{output_path}' if output_path != "Processing Failed" else "Processing Failed")

        output_data.append({
            'Serial Number': row['Serial Number'],
            'Product Name': row['Product Name'],
            'Input Image Urls': row['Input Image Urls'],
            'Output Image Urls': ', '.join(output_urls)
        })
    return pd.DataFrame(output_data)

def save_output_csv(df, output_file_name='output_images.csv'):
    df.to_csv(output_file_name, index=False)
    print(f"Output CSV saved as {output_file_name}")

if __name__ == "__main__":
    try:
        df = pd.read_csv('input.csv')
        validate_csv(df)
    except Exception as e:
        print(f"Error validating CSV: {e}")
        exit(1)  

    processed_df = process_images_from_csv(df)
    save_output_csv(processed_df)
