# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Install Poetry
RUN pip install poetry

# Set the working directory in the container
WORKDIR /app

# Copy only dependencies
COPY pyproject.toml poetry.lock* ./
COPY ./README.md ./

# Install all dependencies
RUN poetry config virtualenvs.create false \
    && poetry install --no-interaction --no-ansi

# Copy the rest of application code
COPY . .

EXPOSE 8000

# Run app.py when the container launches
CMD ["python", "run.py"]