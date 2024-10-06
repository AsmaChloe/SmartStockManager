FROM python:3.8

WORKDIR /app
COPY requirements.txt ./requirements.txt
COPY producer.py ./producer.py
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "-u", "producer.py"]
