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
          "name": "your-bucket-name"
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





```csv
01/11/2025	12:55	Ascot	Thistle Be The One	13/2	Loss
01/11/2025	13:15	Wetherby	Strong Foundations	9/2	Loss
01/11/2025	14:05	Ascot	General Medrano	11/2	Loss
01/11/2025	14:22	Wetherby	Take No Chances	9/2	Loss
01/11/2025	15:10	Ascot	Move It Like Minnie	11/1	Place
01/11/2025	15:45	Ascot	The Changing Man	7/2	Loss
02/11/2025	12:32	Carlisle	Mr Hope Street	10/1	Loss
02/11/2025	14:37	Carlisle	Kalooki Kid	11/2	Loss
02/11/2025	15:12	Carlisle	Inis Oir	9/2	Loss
03/11/2025	14:30	Kempton	Back In Black	10/3	Loss
03/11/2025	15:38	Plumpton	Jorebel	4/5	Loss
03/11/2025	19:00	Wolverhampton	Zoulu Chief	6/4	Win
03/11/2025	19:30	Wolverhampton	True Colours	5/1	Win
03/11/2025	20:00	Wolverhampton	Gustav Graves	3/1	Win
04/11/2025	14:02	Warwick	Go West	8/13	Win
04/11/2025	14:10	Lingfield	The Thames Boatman	11/4	Loss
04/11/2025	16:02	Warwick	Datsalrightcharlie	3/1	Loss
04/11/2025	14:32	Warwick	Uhtred Ragnarson	11/2	Win
04/11/2025	15:22	Wolverhampton	Simply Blue	5/1	Loss
04/11/2025	15:52	Wolverhampton	Filly Foden	13/8	Loss
05/11/2025	15:00	Chepstow	Ben Solo	5/1	Loss
05/11/2025	12:30	Chepstow	Rocking Man	5/6	Win
05/11/2025	14:30	Chepstow	Kadastral	16/5	Loss
05/11/2025	15:35	Chepstow	Kelijoe	7/2	Loss
05/11/2025	17:30	Kempton	Completely Random	11/8	Loss
06/11/2025	13:20	Sedgefield	Prized Jet	7/2	Loss
06/11/2025	13:40	Newbury	Storming George	6/1	Place
06/11/2025	14:15	Newbury	Pottersville	5/1	Loss
06/11/2025	14:30	Sedgefield	Born In The West	9/4	Loss
06/11/2025	14:50	Newbury	Tranquil Sea	85/40	Loss
07/11/2025	14:25	Exeter	JPR One	10/3	Loss
07/11/2025	15:35	Exeter	Jupiter Allen	3/1	Loss
08/11/2025	13:30	Aintree	Andy Amo	7/2	Loss
08/11/2025	14:05	Aintree	Javert Allen	10/3	Loss
08/11/2025	14:40	Aintree	Johnnywho	17/2	Loss
08/11/2025	16:00	Wolverhampton	Clearpoint	3/1	Win
08/11/2025	18:00	Wolverhampton	Jungle Dance	20/1	Loss
10/11/2025	13:45	Carlisle	Dearkeithandkaty	4/1	Loss
10/11/2025	14:15	Carlisle	Unexpected Party	9/4	Loss
10/11/2025	15:05	Kempton	Georgi Girl	5/1	Loss
10/11/2025	14:00	Kempton	Jefe Triunfo	8/1	Loss
10/11/2025	14:30	Kempton	Idy Wood	7/2	Win
11/11/2025	13:17	Lingfield	King of Thieves	4/1	Loss
11/11/2025	15:02	Lingfield	Dangerous Touch	9/4	Loss
11/11/2025	15:37	Lingfield	Princess Keri	6/4	Win
11/11/2025	14:27	Lingfield	Harry Junior	11/4	Win
11/11/2025	15:55	Hereford	Renoir	9/4	Loss
12/11/2025	13:00	Bangor	New Order	5/1	Loss
12/11/2025	13:15	Ayr	President Scottie	5/2	Loss
12/11/2025	13:30	Bangor	Diva Luna	7/4	Win
12/11/2025	14:50	Ayr	Sixmilebridge	6/4	Win
13/11/2025	13:59	Sedgefield	Formel Park	3/1	Win
13/11/2025	14:23	Market Rasen	Heart Over Head	15/2	Place
13/11/2025	15:30	Market Rasen	La Marquise	6/1	Loss
13/11/2025	12:10	Market Rasen	Lock Stock	2/1	Loss
13/11/2025	14:58	Market Rasen	Sandscape	13/8	Loss
14/11/2025	13:10	Cheltenham	Marlacoo	3/1	Loss
14/11/2025	13:10	Cheltenham	Kap Vert	15/2	Loss
14/11/2025	13:45	Cheltenham	Fugitif	7/2	Loss
14/11/2025	14:20	Cheltenham	Double Powerful	7/2	Loss
14/11/2025	15:30	Cheltenham	Great Fleet	11/2	Loss
15/11/2025	13:10	Cheltenham	Herakles Westwood	13/2	Place
15/11/2025	14:20	Cheltenham	Panic Attack	6/1	Win
15/11/2025	14:55	Cheltenham	Kikijo	11/2	Win
15/11/2025	15:30	Cheltenham	Royal Infantry	7/2	Loss
16/11/2025	13:45	Cheltenham	Jog's Forge	3/1	Loss
16/11/2025	14:20	Cheltenham	Dr T J Eckleburg	16/1	Loss
16/11/2025	14:55	Cheltenham	Tanganyika	7/2	Loss
16/11/2025	15:30	Cheltenham	Mirabad	7/2	Loss
17/11/2025	14:00	Exeter	Fingle Bridge	7/1	Place
17/11/2025	14:20	Plumpton	Gold for Alec	18/5	Loss
17/11/2025	15:00	Exeter	Pony Soprano	16/5	Loss
17/11/2025	15:30	Exeter	Special John	1/1	Win
18/11/2025	13:45	Lingfield	Diamond Days	4/1	Loss
18/11/2025	14:45	Lingfield	Star of Diamonds	5/1	Loss
19/11/2025	14:00	Warwick	Dig Deep	5/6	Loss
19/11/2025	14:10	Ffos Las	Gee Force Flyer	4/7	Loss
19/11/2025	15:40	Warwick	Dromlac Jury	13/8	Loss
19/11/2025	12:55	Warwick	Excelero	7/4	Win
19/11/2025	14:35	Warwick	Dunskay	7/1	Loss
20/11/2025	13:45	Warwick	Torn and Frayed	7/4	Loss
20/11/2025	15:40	Lingfield	McLoven	5/4	Loss
21/11/2025	14:00	Ascot	Neon Moon	4/1	Win
21/11/2025	14:35	Ascot	Fortune De Mer	5/2	Loss
21/11/2025	15:10	Ascot	Vision De Maine	7/2	Loss
21/11/2025	14:09	Chepstow	High Tea	9/2	Win
22/11/2025	15:35	Haydock	Major Fortune	5/1	Loss
22/11/2025	13:58	Huntingdon	Kelijoe	9/4	Loss
22/11/2025	14:25	Haydock	Phantomofthepoints	18/1	Place
22/11/2025	15:15	Ascot	Martator	11/1	Place
22/11/2025	16:25	Wolverhampton	Far Too Fizzy	7/2	Loss
24/11/2025	15:05	Kempton	First Confession	1/1	Loss
24/11/2025	13:25	Kempton	Jeriko Du Reponet	1/1	Loss
24/11/2025	14:08	Ludlow	Wyenot	13/8	Win
24/11/2025	20:30	Wolverhampton	Sams Xpress	11/8	Loss
25/11/2025	13:00	Lingfield	El Bufalo	17/2	Place
25/11/2025	14:00	Hereford	Wistmans Prince	5/1	Loss
25/11/2025	14:35	Hereford	Mahler Moon	10/3	Loss
26/11/2025	13:08	Wetherby	Quantock Hills	7/4	Loss
26/11/2025	13:55	Market Rasen	Lihyan	4/1	Loss
26/11/2025	14:55	Market Rasen	Seasmoke	10/3	Win
27/11/2025	14:25	Lingfield	Vanderpoel	10/3	Loss
27/11/2025	12:25	Uttoxeter	River Run Free	11/2	Loss
27/11/2025	13:40	Taunton	Wicked Thoughts	6/1	Win
27/11/2025	15:52	Taunton	No Risk Today	5/1	Loss
27/11/2025	17:45	Newcastle	Pockley	6/1	Loss
28/11/2025	12:45	Newbury	Koapey	7/1	Loss
28/11/2025	13:27	Doncaster	Moon Rocket	15/8	Win
28/11/2025	12:15	Newbury	Sinnatra	15/8	Loss
28/11/2025	13:50	Newbury	Kingston Pride	13/2	Loss
28/11/2025	15:15	Southwell	Lou Lou's Gift	12/1	Loss
28/11/2025	15:35	Newbury	Diamond Ri	4/1	Loss
29/11/2025	12:37	Bangor	Newton Tornado	7/4	Win
29/11/2025	13:25	Newcastle	Wal Bucks	13/2	Loss
29/11/2025	14:55	Newbury	Myretown		
29/11/2025	14:55	Newbury	The Changing Man		
29/11/2025	15:35	Newbury	General Medrano		
30/11/2025	13:45	Carlisle	Pierrot Jaguen		
30/11/2025	13:55	Leicester	Home Made Hero		
30/11/2025	15:32	Leicester	Smile Back		
```
