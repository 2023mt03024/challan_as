# Start from the official Python base image.
FROM python:3.9

# Set the current working directory to /code.
# This is where we'll put the requirements.txt file and the app directory.
WORKDIR /code

# Copy the file with the requirements to the /code directory.
# Copy only the file with the requirements first, not the rest of the code.
# As this file doesn't change often, Docker will detect it and use the cache
# for this step, enabling the cache for the next step too.
COPY ./requirements.txt /code/requirements.txt

# Install the package dependencies in the requirements file.
# Disable pip's caching; since docker will cache
# The --no-cache-dir option tells pip to not save the downloaded packages 
# locally, as that is only if pip was going to be run again to install the
# same packages, but that's not the case when working with containers.
RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

# Copy app.py to the /code directory.
# As this has all the code which is what changes most frequently ,the Docker cache
# won't be used for this or any following steps easily.
COPY ./app.py /code/

# Define flask app config variables
ENV FLASK_KEY=SECRET_KEY
ENV FLASK_KEY_VALUE='the random string'

# app requires ctrl-c(SIGINT) for termination.
# The app must run as PID 1 inside docker to receive a SIGINT.
# To do so, one must use ENTRYPOINT instead of CMD. 
ENTRYPOINT ["python", "app.py"]