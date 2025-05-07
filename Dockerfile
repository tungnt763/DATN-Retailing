FROM apache/airflow:2.8.1

USER airflow

COPY ./requirements.txt /opt/airflow/requirements.txt

RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r /opt/airflow/requirements.txt

# install soda into a virtual environment
RUN python -m venv soda_venv && source soda_venv/bin/activate && \
    pip install --no-cache-dir --no-user soda-core-bigquery==3.0.45 &&\
    pip install --no-cache-dir --no-user soda-core-scientific==3.0.45 && deactivate
