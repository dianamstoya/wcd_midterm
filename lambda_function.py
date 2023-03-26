import boto3
from datetime import datetime, timedelta
import subprocess
from send_email import send_email
import json


def lambda_handler(event, context):
    s3_file_list = []
    
    s3_client=boto3.client('s3')
    for object in s3_client.list_objects_v2(Bucket='dwcd-midterm', Prefix='midterm-data-wcd/')['Contents']:
        s3_file_list.append(object['Key'])
        
    s3_file_list.remove('midterm-data-wcd/')
    print('s3_file_list: ', s3_file_list)
    yestrday = datetime.now() - timedelta(days=1)
    datestr = yestrday.strftime("%Y-%m-%d")
    required_file_list = [f'midterm-data-wcd/calendar_{datestr}.csv', 
                          f'midterm-data-wcd/inventory_{datestr}.csv', 
                          f'midterm-data-wcd/product_{datestr}.csv', 
                          f'midterm-data-wcd/sales_{datestr}.csv', 
                          f'midterm-data-wcd/store_{datestr}.csv'
                          ]
    print('required_file_list: ', required_file_list)
    
    # scan S3 bucket
    if set(required_file_list).issubset(set(s3_file_list)):
        s3_file_url = ['s3://' + 'dwcd-midterm/' + a for a in required_file_list]
        print(s3_file_url)
        # table_name = [a[:-15] for a in s3_file_list]  # works wo Prefix
        table_name = []
        # Loop through the list of strings and extract the table name
        for s in required_file_list:
            # Split the string using the underscore delimiter
            parts = s.split('_')
            # Extract the part after the slash delimiter
            tbl_name = parts[0].split('/')[-1]
            # Print the extracted table name
            table_name.append(tbl_name)
        print('table_name: ', table_name)
    
        data = json.dumps({'conf':{a:b for a, b in zip(table_name, required_file_list)}})
        # send signal to Airflow    
        dag_name = 'midterm_dag' # name of my dag
        endpoint= f'http://3.76.159.89:8080/api/v1/dags/{dag_name}/dagRuns'
        subprocess.run([
            'curl', 
            '-X', 
            'POST', endpoint, 
            '-H', 'Content-Type: application/json',
            '--user', 'airflow:airflow',
            '--data', data
            ])
        # print('Files are sent to Airflow')
        print('data sent is: ', data)
    else:
        send_email()
        
	