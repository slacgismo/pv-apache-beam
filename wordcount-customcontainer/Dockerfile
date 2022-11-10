# This can be any container image,
FROM python:3.8-slim

# Install SDK. (needed for Python SDK)
RUN pip install --no-cache-dir apache-beam[gcp]==2.42.0
ADD /transformers /opt/transformers
RUN pip install /opt/transformers

# Copy files from official SDK image, including script/dependencies.
COPY --from=apache/beam_python3.8_sdk:2.42.0 /opt/apache/beam /opt/apache/beam

# Perform any additional customizations if desired

# Set the entrypoint to Apache Beam SDK launcher.
ENTRYPOINT ["/opt/apache/beam/boot"]


# ARG PYTHON_VERSION='3.8'
# ARG BEAM_VERSION='2.42.0'


# FROM apache/beam_python${PYTHON_VERSION}_sdk:${BEAM_VERSION} AS beam_python

# FROM python:${PYTHON_VERSION}-slim
# ARG BEAM_VERSION


# RUN pip install --no-cache-dir --upgrade pip


# # COPY . /
# # RUN pip install --no-cache-dir -r requirements.txt
# # Add custom package files (could also be a py wheel)
# ADD /transformers /opt/transformers
# RUN pip install /opt/transformers


# COPY --from=beam_python /opt/apache/beam /opt/apache/beam

# ENTRYPOINT [ "/opt/apache/beam/boot" ]

