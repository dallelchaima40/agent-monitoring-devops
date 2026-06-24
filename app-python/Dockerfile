FROM python:3.11-slim
WORKDIR /app
COPY app.py .
RUN pip install redis requests elasticsearch==8.11.0
CMD ["python", "-u", "app.py"]