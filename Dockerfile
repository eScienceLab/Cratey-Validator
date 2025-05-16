FROM python:3.11-slim

# Install required system packages, including git
RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

COPY cratey.py LICENSE /app/
COPY app /app/app

RUN useradd -ms /bin/bash flaskuser
RUN chown -R flaskuser:flaskuser /app

USER flaskuser

EXPOSE 5000

CMD ["flask", "run", "--host=0.0.0.0"]

LABEL org.opencontainers.image.source="https://github.com/eScienceLab/Cratey-Validator"