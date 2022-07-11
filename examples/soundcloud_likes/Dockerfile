FROM python:3.9-alpine

WORKDIR /opt/batchout

RUN apk add --no-cache postgresql-libs && \
 apk add --no-cache --virtual .build-deps gcc linux-headers musl-dev \
 postgresql-dev libxml2-dev libxslt-dev

RUN pip install --no-cache-dir pipenv

COPY . .

RUN pipenv install

ENV PYTHONPATH=/opt/batchout

ENTRYPOINT ["pipenv", "run", "python", "run_batchout.py"]