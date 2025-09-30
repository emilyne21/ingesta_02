FROM python:3.11-slim
WORKDIR /programas/ingesta

# Dependencias necesarias para: MySQL -> CSV -> S3
RUN pip install --no-cache-dir boto3 mysql-connector-python

# Solo copiamos el script (evita arrastrar archivos innecesarios)
COPY ingesta.py .

CMD ["python", "ingesta.py"]
