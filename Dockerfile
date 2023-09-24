# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Update system and install dependencies
# Install system dependencies and Microsoft ODBC Driver for SQL Server
RUN apt-get update -q && \
    apt-get install -yq unixodbc unixodbc-dev gnupg curl apt-transport-https && \
    curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update -q && \
    ACCEPT_EULA=Y apt-get install -yq msodbcsql17

# Set the working directory in docker
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Run python_pred.py when the container launches
CMD ["python", "python_pred.py"]