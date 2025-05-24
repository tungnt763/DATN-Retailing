FROM apache/airflow:2.8.1-python3.11

USER root

# Install OpenJDK-17
RUN apt update && \
    apt-get install -y openjdk-17-jdk wget && \
    apt-get install -y ant && \
    apt-get clean;

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-arm64
RUN export JAVA_HOME

USER airflow

COPY ./requirements.txt /opt/airflow/requirements.txt

RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r /opt/airflow/requirements.txt

# install soda into a virtual environment
RUN python -m venv soda_venv && source soda_venv/bin/activate && \
    pip install --no-cache-dir --no-user soda-core-bigquery==3.0.45 &&\
    pip install --no-cache-dir --no-user soda-core-scientific==3.0.45 && deactivate
