# Use an official Python runtime as a parent image
FROM python:3.9

WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install dependencies
RUN pip install --no-cache-dir pandas==1.3.3 sqlalchemy==1.4.25 psycopg2==2.9.1