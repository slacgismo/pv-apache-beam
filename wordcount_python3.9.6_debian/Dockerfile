ARG PYTHON_VERSION='3.9.6'
ARG BEAM_VERSION='2.42.0'

FROM debian:11


# install python 3.9.6
RUN apt update
RUN apt install build-essential zlib1g-dev libncurses5-dev libgdbm-dev libnss3-dev libssl-dev libsqlite3-dev libreadline-dev libffi-dev curl libbz2-dev lzma liblzma-dev libbz2-dev -y
RUN apt install wget -y
# FROM apache/beam_python${PYTHON_VERSION}_sdk:${BEAM_VERSION} AS beam_python
RUN wget https://www.python.org/ftp/python/3.9.6/Python-3.9.6.tgz
RUN tar -xf Python-3.9.6.tgz
WORKDIR /Python-3.9.6
RUN ./configure --enable-optimizations
RUN make -j 8
RUN make altinstall
# alias python3.8 to python
RUN ln -sf /usr/local/bin/python3.9  /usr/local/bin/python 
RUN python -m pip install --upgrade pip


RUN python -m pip install --no-cache-dir apache-beam[gcp]==2.42.0

# ADD /transformers /opt/transformers
# RUN python -m pip install /opt/transformers


COPY --from=apache/beam_python3.9_sdk:2.42.0 /opt/apache/beam /opt/apache/beam

ENTRYPOINT [ "/opt/apache/beam/boot" ]