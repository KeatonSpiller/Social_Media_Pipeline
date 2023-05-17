FROM python:latest

WORKDIR /Social_Media_Pipeline

COPY src/ /Social_Media_Pipeline/src/
COPY credentials/ /Social_Media_Pipeline/credentials/
COPY user_input/ /Social_Media_Pipeline/user_input/
COPY requirements.txt /Social_Media_Pipeline/requirements.txt

RUN pip install -r requirements.txt

# ----Docker Commands----
# runas /user:Administrator cmd
# docker login
# -----image------
# docker build -f Dockerfile -t py .
# docker build -f src/python/Dockerfile -t py .
# docker run py:latest
# -----container------
# docker container ls      ----check list of containers
# docker ps                ----check if container is running
# docker run py:latest
# docker exec -it [CONTAINER ID] /bin/bash
# docker exec -it 22947f89f5c0 /bin/bash