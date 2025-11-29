FROM python:3.12-slim

RUN apt-get update && apt-get install -y
RUN apt-get install -y python3-pip

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install -r requirements.txt

COPY . /app

CMD ["python", "main.py"]
