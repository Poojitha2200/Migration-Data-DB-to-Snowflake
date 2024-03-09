import psycopg2
import snowflake.connector
import boto3
import pandas as pd
from io import StringIO
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import smtplib
import os
import datetime

print('Trying to connect postgres...')

# PostgreSQL connection parameters
pg_host = 'localhost'
pg_port = '******' # Mention the port number
pg_user = '******' # Provide the postgres username
pg_password = '******' # Provide the postgres password
pg_database = 'postgres' # Mention the required database
pg_table = 'agents1'
print('Connected to PostgreSQL successfully')

# pg_host = input('enter the host details of postgres:')
# pg_port = input('enter the port no of postgres:')
# pg_user = input('enter the username of postgres:')
# pg_password = input('enter the password to connect to postgres:')
# pg_database = input('enter the database present in postgres:')
# pg_table = input('enter the table name to connect to postgres:')

# AWS S3 connection parameters
aws_access_key_id = '******'
aws_secret_access_key = '*********'
region_name = '*******'
bucket_name = '******'
print('Connected to AWS successfully')

# Snowflake connection parameters
snowflake_account = '*******'
snowflake_user = '*****'
snowflake_password = '******'
snowflake_database = '***'
snowflake_warehouse = '****'
snowflake_role = '****'
snowflake_schema = '***'
snowflake_table ='***'
print('Connected to Snowflake successfully')

# snowflake_account = input('enter the Snowflake account name:')
# snowflake_user = input('enter the Snowflake username:')
# snowflake_password = input('enter the Snowflake password:')
# snowflake_database = input('enter the Snowflake database:')
# snowflake_warehouse = input('enter the Snowflake warehouse:')
# snowflake_role = input('enter the Snowflake role:')
# snowflake_schema = input('enter the Snowflake schema:')
# snowflake_table = input('enter the Snowflake table name:')

def send_mail_notification(file_path, body, row_count_csv_path):
    msg = MIMEMultipart()
    msg['From'] = '******' # Mention the mail from which notification needs to be sent
    msg['To'] = '******' # Mention the mail to which notification needs to be received
    msg['Subject'] = "Data Migration Status Notification"
    sender_email_id_password = '*******'  # Replace with the generated App Password

    # Include row counts in the email body
    body += f"\n\nTable Name and Row Counts:\n{row_count_df.to_string(index=False)}"

    msg.attach(MIMEText(body, 'plain'))

    # Attach CSV file with row counts
    with open(row_count_csv_path, "rb") as row_count_attachment:
        row_count_mime = MIMEBase('application', 'octet-stream')
        row_count_mime.set_payload(row_count_attachment.read())
        encoders.encode_base64(row_count_mime)
        row_count_mime.add_header('Content-Disposition', 'attachment', filename=os.path.basename(row_count_csv_path))
        msg.attach(row_count_mime)

    # Attach CSV file with data
    with open(file_path, "rb") as data_attachment:
        data_mime = MIMEBase('application', 'octet-stream')
        data_mime.set_payload(data_attachment.read())
        encoders.encode_base64(data_mime)
        data_mime.add_header('Content-Disposition', 'attachment', filename=os.path.basename(file_path))
        msg.attach(data_mime)

    # Establish SMTP connection and send email
    with smtplib.SMTP('smtp.gmail.com', 587) as server:
        server.starttls()
        server.login('t.vamshi406@gmail.com', sender_email_id_password)
        server.sendmail('t.vamshi406@gmail.com', 'v.poojitha2291@gmail.com', msg.as_string())

    print(f"Sent an Email with required details along with attachments to end user: {file_path}, {row_count_csv_path}")

# Create PostgreSQL connection
pg_connection = psycopg2.connect(host=pg_host, port=pg_port, user=pg_user, password=pg_password, database=pg_database)

# SQL query to select data from PostgreSQL
query = f"SELECT COUNT(*) FROM {pg_table}"

# Execute PostgreSQL query
pg_cursor = pg_connection.cursor()
pg_cursor.execute(query)

# Fetch the PostgreSQL row count
pg_row_count = pg_cursor.fetchone()[0]

print(f"Row count in PostgreSQL table '{pg_table}': {pg_row_count}")

# Load data from PostgreSQL into a Pandas DataFrame
df = pd.read_sql(f"SELECT * FROM {pg_table}", pg_connection)

# Specify the S3 key (file name in S3)
s3_key = f"{pg_table}/{pg_table}.csv"

# Local path where the CSV file will be saved
local_csv_path = f"{pg_table}_local.csv"

# Save the CSV data locally
df.to_csv(local_csv_path, index=False)

# Upload CSV data to S3
s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name=region_name)
s3.put_object(Body=df.to_csv(index=False), Bucket=bucket_name, Key=s3_key)

# Create Snowflake connection
snowflake_connection = snowflake.connector.connect(
    user=snowflake_user,
    password=snowflake_password,
    account=snowflake_account,
    warehouse=snowflake_warehouse,
    database=snowflake_database,
    role=snowflake_role,
    schema=snowflake_schema
)

# Snowflake cursor
snowflake_cursor = snowflake_connection.cursor()

# Snowflake SQL query to get row count
snowflake_query = f"SELECT COUNT(*) FROM {snowflake_table}"

# Execute Snowflake query
snowflake_cursor.execute(snowflake_query)

# Fetch the Snowflake row count
snowflake_row_count = snowflake_cursor.fetchone()[0]

print(f"Row count in Snowflake table '{snowflake_table}': {snowflake_row_count}")

# Create a DataFrame with table name and row counts
row_count_df = pd.DataFrame({
    'Table Name': [snowflake_table],
    'Snowflake Row Count': [snowflake_row_count],
    'PostgreSQL Row Count': [pg_row_count],
})

# Save the DataFrame to a local CSV file
row_count_csv_path = "row_counts.csv"
row_count_df.to_csv(row_count_csv_path, index=False)

# Compose the email body
body = "Data load has successfully completed."
body += f"\n\nRow count in PostgreSQL table '{pg_table}': {pg_row_count}"
body += f"\nRow count in Snowflake table '{snowflake_table}': {snowflake_row_count}"

# Send email notification with attachment and row counts
send_mail_notification(local_csv_path, body, row_count_csv_path)

# Close PostgreSQL and Snowflake connections
pg_cursor.close()
pg_connection.close()
snowflake_cursor.close()
snowflake_connection.close()

# Remove the local CSV files
os.remove(local_csv_path)
os.remove(row_count_csv_path)

print(f"Data loaded from PostgreSQL to S3: s3://{bucket_name}/{s3_key}")
