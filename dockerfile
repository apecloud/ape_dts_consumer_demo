ARG PYTHON_VERSION=3.11
FROM --platform=${TARGETPLATFORM:-linux/amd64} python:${PYTHON_VERSION}-alpine

ARG TARGETPLATFORM

# Add non root user
RUN addgroup -S app && adduser app -S -G app

WORKDIR /home/app/

COPY entrypoint.sh index.py avro_schema.py config_util.py requirements.txt /home/app/

RUN chown -R app /home/app && \
  mkdir -p /home/app/python && chown -R app /home/app

USER app
ENV PATH=$PATH:/home/app/.local/bin:/home/app/python/bin/
ENV PYTHONPATH=$PYTHONPATH:/home/app/python

RUN pip install -r ./requirements.txt --target=/home/app/python

ENTRYPOINT ["sh", "/home/app/entrypoint.sh"]