# base Docker image that we will build on
FROM python:3.13.11-slim

WORKDIR /code

# Copy uv binary from official uv image (multi-stage build pattern)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/


COPY pyproject.toml .python-version uv.lock ./

RUN uv sync --locked

# set up our image by installing prerequisites; pandas in this case
RUN pip install pandas pyarrow

# set up the working directory inside the container

# copy the script to the container. 1st name is source file, 2nd is destination
COPY pipeline.py pipeline.py

# define what to do first when the container runs
# in this example, we will just run the script
ENTRYPOINT ["uv", "run", "python", "pipeline.py"]