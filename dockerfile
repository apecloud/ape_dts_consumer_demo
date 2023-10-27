ARG PYTHON_VERSION=3.11
FROM --platform=${TARGETPLATFORM:-linux/amd64} python:${PYTHON_VERSION}-alpine

ARG TARGETPLATFORM

# Add non root user
# RUN addgroup -S root && adduser app -S -G root

WORKDIR /home/root/

COPY entrypoint.sh index.py avro_schema.py config_util.py log.py requirements.txt /home/root/

RUN chown -R root /home/root && \
  mkdir -p /home/root/python && chown -R root /home/root

USER root
ENV PATH=$PATH:/home/root/.local/bin:/home/root/python/bin/
ENV PYTHONPATH=$PYTHONPATH:/home/root/python

RUN pip install -r ./requirements.txt --target=/home/root/python

ENTRYPOINT ["sh", "/home/root/entrypoint.sh"]