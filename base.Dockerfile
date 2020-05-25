FROM python:3.6-alpine
RUN apk add --no-cache mariadb-dev libffi-dev mariadb-connector-c build-base libxslt-dev libxml2-dev
COPY ./requirements.txt  /deploy/app/eth_balance_crawler/requirements.txt
RUN pip install --no-cache-dir -r /deploy/app/eth_balance_crawler/requirements.txt
RUN rm -rf .cache/pip
