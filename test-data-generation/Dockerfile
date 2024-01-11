FROM python:3.12

RUN mkdir /data
RUN mkdir /logs

COPY /generation /app/generation
COPY logging.ini /app
COPY pyproject.toml /app

WORKDIR /app
ENV PYTHONPATH=/app

RUN pip3 install poetry
RUN poetry config virtualenvs.create false
RUN poetry install --no-root