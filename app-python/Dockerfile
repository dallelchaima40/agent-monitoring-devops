FROM python:3.11-slim
WORKDIR /app
COPY app.py .
COPY llm_analyzer.py .
RUN pip install redis requests elasticsearch==8.11.0 google-genai
CMD ["python", "-u", "app.py"]
