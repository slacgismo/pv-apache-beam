ARG PYTHON_VERSION='3.9.6'
ARG BEAM_VERSION='2.42.0'

FROM debian:11
ENV PYTHONUNBUFFERED 1
ENV PYTHONDONTWRITEBYTECODE 1
RUN apt-get update \
  # dependencies for building Python packages
  && apt-get install -y build-essential \
  # psycopg2 dependencies
  && apt-get install -y libpq-dev \
  # Additional dependencies
  && apt-get install -y telnet netcat \
  # cleaning up unused files
  && apt-get purge -y --auto-remove -o APT::AutoRemove::RecommendsImportant=false \
  && rm -rf /var/lib/apt/lists/*
RUN apt update && apt install libblas3 liblapack3 liblapack-dev libblas-dev cmake -y
RUN apt install libhdf5-dev -y

RUN apt-get -q update
RUN apt-get -q install git -y
RUN apt-get -q install sudo -y

RUN apt install build-essential zlib1g-dev libncurses5-dev libgdbm-dev libnss3-dev libssl-dev libsqlite3-dev libreadline-dev libffi-dev curl libbz2-dev lzma liblzma-dev libbz2-dev -y

WORKDIR /usr/local/src
# download gridlabd from a branch
RUN git clone -b develop-add-aws-install https://github.com/slacgismo/gridlabd.git
WORKDIR /usr/local/src/gridlabd
# install gridlabd
RUN ./install.sh -t -v
RUN ln -sf /usr/bin/python3.9 /usr/bin/python
RUN apt-get install python3-pip -y
RUN pip install --upgrade pip
RUN pip install --no-cache-dir apache-beam[gcp]==2.42.0
# install custom package
ADD /transformers /opt/transformers
RUN pip install /opt/transformers
ADD /data/PGE /usr/local/src/gridlabd

COPY --from=apache/beam_python3.9_sdk:2.42.0 /opt/apache/beam /opt/apache/beam
ENTRYPOINT [ "/opt/apache/beam/boot" ]

