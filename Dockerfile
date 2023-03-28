FROM python:3.9-alpine3.15

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

COPY requirements.txt .

RUN python3 -m pip install --upgrade pip && python3 -m pip install -r requirements.txt --no-cache-dir

COPY ./src .

COPY entrypoint.sh /app
RUN chmod +x entrypoint.sh

RUN addgroup -S web && adduser -S web -G web \
    && chown web:web -R /app
USER web

ENTRYPOINT ["./entrypoint.sh", "python3", "main.py"]
