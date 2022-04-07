FROM debian:buster-slim
RUN apt update && apt install -y python3-pip casacore-dev
RUN python3 -m pip install -U pip
RUN apt install -y python3-numpy

# cache packages
RUN pip3 install daliuge-engine pyarrow python-casacore
RUN pip3 install ska-sdp-realtime-receive-core ska-sdp-realtime-receive-modules[plasma] -i https://artefact.skao.int/repository/pypi-all/simple
RUN pip3 install pytest coverage flake8 black isort pytest-cov codecov mypy gitchangelog mkdocs

COPY . /app
WORKDIR /app
ARG PYPI_REPOSITORY_URL=https://artefact.skao.int/repository/pypi-all
RUN pip3 install --extra-index-url=$PYPI_REPOSITORY_URL/simple .
RUN pip3 install -r requirements-test.txt
CMD pytest && make lint
