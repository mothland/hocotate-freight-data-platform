FROM apache/airflow:2.9.0

# Copy files to tmp first
COPY requirements.txt /tmp/requirements.txt
COPY init_prestart.sh /tmp/init_prestart.sh

USER airflow
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# ✅ make sure airflow CLI is discoverable
ENV PATH="/home/airflow/.local/bin:${PATH}"

USER root
RUN mkdir -p /opt/airflow && cp /tmp/init_prestart.sh /opt/airflow/init_prestart.sh && chmod +x /opt/airflow/init_prestart.sh
RUN rm /tmp/init_prestart.sh /tmp/requirements.txt

USER airflow
WORKDIR /opt/airflow