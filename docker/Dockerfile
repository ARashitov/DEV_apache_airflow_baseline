FROM apache/airflow:2.1.2-python3.9

USER root

COPY ./requirements.txt requirements.txt
COPY ./install_private_dependencies.sh install_private_dependencies.sh

RUN apt-get update \
    && apt-get upgrade -y \
    && apt-get install -y git \
    && pip3 install --upgrade pip \
    && pip3 install -r requirements.txt --no-warn-script-location \
    && rm -rf requirements.txt \
    && source install_private_dependencies.sh \
    && rm -rf 

USER airflow
