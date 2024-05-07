FROM apache/airflow:2.7.3 
ADD requirements.txt . 



USER root

USER airflow
RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements.txt


# # Set entrypoint script to run on container start
# ENTRYPOINT ["/usr/src/app/odbc_installation.sh"]
