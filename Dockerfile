# FROM python:3.10-slim

# WORKDIR /app
# COPY requirements.txt .
# RUN pip install -r requirements.txt
# COPY . .

# CMD ["python", "consumer.py"]

FROM python:3.10-slim

# Set environment variables to prevent Python from writing .pyc files and buffering stdout/stderr
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Set working directory
WORKDIR /app

# Install system dependencies if needed (e.g., nano, gcc, etc.)
# RUN apt-get update && apt-get install -y build-essential && rm -rf /var/lib/apt/lists/*

# Install only requirements (to leverage Docker layer cache)
COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

# Copy project files (do this after installing deps to avoid invalidating cache unnecessarily)
COPY . .

# Run the app
CMD ["python", "consumer.py"]
