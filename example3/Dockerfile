
ARG PYTHON_VERSION='3.8'
ARG BEAM_VERSION='2.42.0'


FROM apache/beam_python${PYTHON_VERSION}_sdk:${BEAM_VERSION} AS beam_python

FROM python:${PYTHON_VERSION}-slim
ARG BEAM_VERSION


RUN pip install --no-cache-dir --upgrade pip


# COPY MOSEK License 
ADD  ./license /root/mosek 
# COPY . /
# RUN pip install --no-cache-dir -r requirements.txt
# Add custom package files (could also be a py wheel)
ADD /transformers /opt/transformers
RUN pip install /opt/transformers


COPY --from=beam_python /opt/apache/beam /opt/apache/beam

ENTRYPOINT [ "/opt/apache/beam/boot" ]