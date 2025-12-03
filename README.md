# csv_s3_to_rds_lambda

# CSV Migration from S3 bucket to PostgreSQL database with AWS Lambda

## Order of Operations
1. Create Lambda function and upload .zip file.
2. Create exceution role in IAM and create permissions: S3 access, VPC access, SNS access.
3. Add VPC to Lambda, using security groups from RDS database.
4. Connect Lambda to RDS and vice versa.
5. Create VPC endpoint for S3.
6. Create VPC endpoint for SNS.
7. Test Lambda function.
8. Create trigger.

### S3 Bucket
* Create a VPC endpoint for the bucket that contains the CSV files.

* VPC Console > Endpoints > Create Endpoint (Gateway) > Service Category=AWS Services > `com.amazonaws.eu-west-1.s3` > Select your VPC > Choose route tables associated with the subnets where your Lambda function runs.
---

### RDS PostgreSQL Database.

* In the RDS security group’s inbound rules, add a rule to allow traffic on port 5432 (PostgreSQL default port).

* Set up a Lambda connection in Connected Compute Resources.

* Set the source of this inbound rule to be the security group(s) assigned to your Lambda function (by specifying the Lambda’s security group ID).
---

### Lambda
* Create Python Lambda file:

```python
import boto3
import psycopg2
import os
import pandas as pd
import io
import logging


logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')
sns = boto3.client('sns')

# Load expected columns from a plain text file in the Lambda package
def load_expected_columns(filename='expected_columns.txt'):
    expected = []
    with open(os.path.join(os.path.dirname(__file__), filename), 'r') as f:
        for line in f:
            col = line.strip()
            if col:
                expected.append(col)
    logger.info(f"Loaded expected columns: {expected}")
    return expected

EXPECTED_COLUMNS = load_expected_columns()

def lambda_handler(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']

    logger.info(f"Processing file {key} from bucket {bucket}")
    try:
        # Download CSV file from S3
        obj = s3.get_object(Bucket=bucket, Key=key)
        csv_content = obj['Body'].read()
        logger.info(f"Downloaded file {key}, size: {len(csv_content)} bytes")

        # Read CSV into pandas DataFrame
        df = pd.read_csv(io.BytesIO(csv_content))
        df['id'] = df['id'].astype(str).str.replace('.0', '', regex=False).astype(int)
        logger.info(f"CSV loaded into DataFrame with shape {df.shape}")
        # Validate columns
        if list(df.columns) != EXPECTED_COLUMNS:
            error_msg = f"CSV columns {list(df.columns)} do not match expected {EXPECTED_COLUMNS}"
            send_notification(error_msg, subject="CSV Import Failure")
            raise ValueError(error_msg)

        # Connect to PostgreSQL
        logger.info("Connecting to PostgreSQL")
        conn = psycopg2.connect(
            host=os.environ['PG_HOST'],
            dbname=os.environ['PG_DB'],
            user=os.environ['PG_USER'],
            password=os.environ['PG_PASSWORD'],
            port=os.environ['PG_PORT']
        )
        cur = conn.cursor()
        logger.info("PostgreSQL connection established")

        # Prepare in-memory CSV for COPY
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False, header=True)
        csv_buffer.seek(0)

        # Load data to staging table
        cur.execute("SET datestyle TO 'DMY';")
        logger.info("Copying data into staging table")
        cur.copy_expert(
            f"COPY psa_data.staging_selections ({', '.join(EXPECTED_COLUMNS)}) FROM STDIN WITH CSV HEADER",
            csv_buffer
        )

        # Insert unique rows into main table
        logger.info("Inserting unique rows into main table")
        cur.execute(f"""
            INSERT INTO psa_data.selections ({', '.join(EXPECTED_COLUMNS)})
            SELECT DISTINCT {', '.join(EXPECTED_COLUMNS)} FROM psa_data.staging_selections
            ON CONFLICT ON CONSTRAINT unique_all_columns DO NOTHING;
        """)

        # Truncate staging table
        logger.info("Truncating staging table")
        cur.execute("TRUNCATE TABLE psa_data.staging_selections;")

        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"Successfully processed and merged file {key}")

        send_notification(f"Successfully processed and merged file {key}", subject="CSV Import Success")

        return {'statusCode': 200, 'body': f'Processed file {key}'}

    except Exception as e:
        logger.error(f"Error processing file {key}: {str(e)}", exc_info=True)
        send_notification(f"Error processing file {key}: {str(e)}", subject="CSV Import Failure")
        raise


def send_notification(message, subject="Notification"):
    sns.publish(
        TopicArn=os.environ['SNS_TOPIC_ARN'],
        Message=message,
        Subject=subject
    )
```

* Set the Lambda runtime to Python 3.11
* Put your `psra_lambda.py` file into a directory with the expected columns file.
* Run this command to install a library (psycopg2-binary)  into that directory:

`pip install <package> --platform manylinux2014_x86_64 --only-binary=:all: --python-version 311 --implementation cp --abi cp311 -t .`

* Put all that into a zip folder and upload it to Lambda.

* Also, add this layer:
`arn:aws:lambda:eu-west-1:336392948345:layer:AWSSDKPandas-Python311:24`

Found on this website: https://aws-sdk-pandas.readthedocs.io/en/stable/layers.html

Allows you to use pandas and all its dependencies without downloading them.

Set Environment Variables:
|Key|Value|
|---|----|
|PG_HOST|Database endpoint|
|PG_DB|Database Name|
|PG_USER|User Name|
|PG_PASSWORD|Database Password|
|PG_PORT|Port (5432)|
|SNS_TOPIC_ARN| ARN of SNS topic|

Create test case for Lambda function:

```json
{
  "Records": [
    {
      "s3": {
        "bucket": {
          "name": "tailevaluation"
        },
        "object": {
          "key": "path/to/your-file.csv"
        }
      }
    }
  ]
}
```

Create an execution role on the IAM console: AWS Service > Lambda

Add inline policy for S3 bucket access:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::your-bucket-name",
        "arn:aws:s3:::your-bucket-name/*"
      ]
    }
  ]
}

```

When a Lambda function is configured to run inside a VPC, it needs to create ENIs to connect to the specified subnets, and this requires specific IAM permissions. add the policy: AWSLambdaVPCAccessExecutionRole.

Put your Lambda function into a VPC with the RDS instance, with the right subnets, then create an RDS connection.
The VPC and subnets will be available to copy from the RDS console of the relevant database.

Finally, set a trigger to call Lambda fucntion when a PUT/POST/COPY operation is done with the suffix '.csv.

---

### SNS

Create topic > Standard > make a name > Create.
Create Subscription > protocol=Email > endpoint=<email-address>

Create VPC endpoint.
Service Name: com.amazonaws.[region].sns
Grant a new inline policy for execution role:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "sns:Publish",
      "Resource": "arn:aws:sns:<TopicARN>"
    }
  ]
}

```
```json
{
  "Records": [
    {
      "s3": {
        "bucket": {
          "name": "data-storage-tailevaluation"
        },
        "object": {
          "key": "prsa/psra-comm-lease-01062025-060016.csv"
        }
      }
    }
  ]
}
```

