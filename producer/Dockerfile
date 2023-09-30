# Use a Python base image
FROM python:3.10

# Set working directory
WORKDIR /producer_script

# Install all the dependencies (including Flask)
COPY ./requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir --upgrade -r /app/requirements.txt

# Copy the producer python scripts into the container
COPY ./producer /producer_script

# Expose the port where your FlaskAPI will run
EXPOSE 5000

# Define the command to run the producers
CMD ["python3", "producer_footdata.py", "producer_somedata.py"]
