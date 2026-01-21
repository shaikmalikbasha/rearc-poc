# Use a Python 3.9 image as the base image
FROM python:3.9.17-slim-bullseye

# Set a working directory for the app
WORKDIR /app

# Copy the application code into the container
COPY . /app/

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

RUN ls -la /

# Expose the port that App will run on
EXPOSE 5000

# Run the Flask application
CMD ["python", "app.py"]
