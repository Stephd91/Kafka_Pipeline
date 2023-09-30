# Use a Python base image
FROM python:3.10

# Set working directory
WORKDIR /consumer_scripts

# Install all the dependencies (including Flask)
COPY ./requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir --upgrade -r /app/requirements.txt

# Copy the Flask API code into the container
COPY ./consumer /consumer_scripts

# Expose the port where your Flask API will run
EXPOSE 1111

# Define the command to run your Flask API
CMD ["python", "consumer_api.py"]