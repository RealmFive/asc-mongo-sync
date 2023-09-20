FROM python:3.8-slim

# Base system and packages
RUN apt update -y --fix-missing && apt install -y dos2unix
RUN pip install --upgrade pip

COPY . /app
RUN rm -rf /app/.git

RUN pip install -r /app/requirements.txt

# Run the test
RUN dos2unix /app/pre_deploy_tests.sh && \
    chmod +x /app/pre_deploy_tests.sh
RUN /app/pre_deploy_tests.sh

# Entrypoint of the app
RUN dos2unix /app/entrypoint.sh && \
    chmod +x /app/entrypoint.sh
ENTRYPOINT ["./app/entrypoint.sh"]

