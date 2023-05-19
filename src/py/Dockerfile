FROM python:latest

WORKDIR /Social_Media_Pipeline

RUN mkdir -p /Social_Media_Pipeline/src/py

COPY src/ /Social_Media_Pipeline/src/
COPY credentials/ /Social_Media_Pipeline/credentials/
COPY user_input/ /Social_Media_Pipeline/user_input/
COPY requirements.txt /Social_Media_Pipeline/requirements.txt
COPY __init__.py /Social_Media_Pipeline/__init__.py

CMD ["python", "src/py/main.py"]

RUN pip install -r requirements.txt

# ----Docker Commands----
# runas /user:Administrator cmd
# docker login
# -----image------
# docker build -f src/py/Dockerfile -t py .
# docker run py:latest
# -----container------
# docker container ls      ----check list of containers
# docker ps                ----check if container is running
# docker run py:latest
# docker exec -it [CONTAINER ID] /bin/bash
# docker exec -it 22947f89f5c0 /bin/bash