from airflow.decorators import task
from airflow.utils.email import send_email
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from datetime import datetime

@task
def check_invalid_rows_and_notify(table_name, dataset_name, project_name, gcp_conn_id, developer_email):
    hook = BigQueryHook(gcp_conn_id=gcp_conn_id, use_legacy_sql=False)
    table_id = f"{project_name}.{dataset_name}.{table_name}_invalid"
    query = f"SELECT COUNT(1) as cnt FROM `{table_id}`"

    result = hook.get_pandas_df(sql=query)
    cnt = result.iloc[0]['cnt']

    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
    <style>
        body {{
        font-family: 'Segoe UI', Roboto, sans-serif;
        background-color: #f6f8fa;
        color: #333;
        padding: 20px;
        }}
        .container {{
        max-width: 600px;
        margin: auto;
        background-color: #ffffff;
        border-radius: 8px;
        padding: 30px;
        box-shadow: 0 2px 6px rgba(0, 0, 0, 0.1);
        }}
        .header {{
        font-size: 20px;
        font-weight: bold;
        color: #d93025;
        border-bottom: 2px solid #d93025;
        padding-bottom: 10px;
        margin-bottom: 20px;
        }}
        .content {{
        font-size: 16px;
        line-height: 1.6;
        }}
        .content strong {{
        color: #d93025;
        }}
        .footer {{
        margin-top: 30px;
        font-size: 13px;
        color: #888;
        text-align: center;
        }}
        .highlight {{
        background-color: #fef2f2;
        border-left: 4px solid #d93025;
        padding: 10px 15px;
        margin: 15px 0;
        border-radius: 4px;
        color: #b91c1c;
        }}
    </style>
    </head>
    <body>
    <div class="container">
        <div class="header">🚨 Data Quality Alert: Invalid Records Detected</div>
        <div class="content">
        Hello,<br><br>
        Airflow has detected <strong>{cnt}</strong> invalid record(s) in the table:
        <div class="highlight"><strong>{table_name}_invalid_data</strong></div>
        Please review and take appropriate actions.<br><br>
        🔎 <strong>Details:</strong><br>
        - Project: <code>{project_name}</code><br>
        - Dataset: <code>{dataset_name}</code><br>
        - Table: <code>{table_name}_invalid</code><br><br>
        If you believe this is unexpected, please check the data transformation logic or upstream source.
        </div>
        <div class="footer">
        Sent from your Airflow data pipeline | {datetime.now().strftime('%Y-%m-%d %H:%M')}
        </div>
    </div>
    </body>
    </html>
    """

    if cnt > 0:
        send_email(
            to=developer_email,
            subject=f"[Data Alert] Invalid rows found in {table_name}_invalid",
            html_content=html_content
        )
