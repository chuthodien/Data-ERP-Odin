FROM apache/airflow:2.2.3

USER root

# Install OpenJDK-11
RUN apt update && \
    apt-get install -y wkhtmltopdf && \
    apt-get install -y xvfb && \
    apt-get install -y openjdk-11-jdk && \
    apt-get install -y ant && \
    apt-get clean;

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
RUN export JAVA_HOME

RUN apt-get update && \
      apt-get -y install sudo

RUN adduser airflow sudo


USER airflow

RUN pip install --no-cache-dir apache-airflow-providers-airbyte
RUN pip install --no-cache-dir dbt-postgres==1.0.0
RUN pip install --no-cache-dir airflow-dbt
RUN pip install --no-cache-dir SQLAlchemy
RUN pip install --no-cache-dir playwright
RUN pip install --no-cache-dir pdfkit
RUN pip install --no-cache-dir py_vncorenlp
RUN pip install --no-cache-dir phonlp
RUN playwright install


