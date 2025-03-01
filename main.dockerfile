FROM python:3.9


WORKDIR /godig


COPY . /godig


RUN pip install boto3 pymysql pandas


ENV RDS_HOST="your-rds-host"
ENV RDS_USER="your-rds-user"
ENV RDS_PASSWORD="your-rds-password"
ENV RDS_DB="your-rds-db"


CMD ["python", "sample_program.py"]
