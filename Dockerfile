FROM apache/airflow:3.1.5

# Install python liberies
RUN pip install --user pyspark==3.5.2 apache-airflow-providers-apache-spark


# Install Java JDK
USER root

RUN apt update && \
    apt install -y openjdk-17-jdk && \
    apt install -y ant

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
RUN export JAVA_HOME

USER airflow