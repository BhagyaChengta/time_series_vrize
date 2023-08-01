# Use the base image for FastAPI application
FROM tiangolo/uvicorn-gunicorn-fastapi:python3.9

# Set the working directory in the container
WORKDIR /app

# Install required dependencies
COPY ./requirements.txt /app/
RUN pip install --upgrade pip && pip install -r requirements.txt

# Copy the FastAPI application files to the container
COPY ./app /app





