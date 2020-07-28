# Use puckel/docker-airflow as our base image; contains basic python
# deps + Airflow deps
FROM puckel/docker-airflow:1.10.9

USER root

# To our existing airflow image, we're going to add:
# 1. A MongoDB client to interact w. Mongo
RUN apt-get update &&\ 
    apt-get install -y wget gnupg &&\
    wget -qO - https://www.mongodb.org/static/pgp/server-4.2.asc | apt-key add - && \
    echo "deb http://repo.mongodb.org/apt/debian buster/mongodb-org/4.2 main" | tee /etc/apt/sources.list.d/mongodb-org-4.2.list &&\
    apt-get update &&\
    apt-get install -y mongodb-org-shell mongodb-org-tools
    
# 2. A few command line utilities
RUN apt-get install -y jq curl &&\
    /usr/local/bin/python -m pip install --upgrade pip &&\
    pip3 install yq &&\
    pip3 install pymongo

# 3. A bash function nwe've written to process data
COPY ./utils/processF4File.sh $AIRFLOW_HOME/utils/
    
USER airflow
