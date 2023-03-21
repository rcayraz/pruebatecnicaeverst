FROM python:3.8-slim

ENV LANG=C.UTF-8 LC_ALL=C.UTF-8
ENV PYTHON_VERSION 3.8

RUN set -x \
  && apt-get update \
  && apt-get install -y python3-pip  \
  && apt-get clean

COPY . .

WORKDIR .

RUN python -m pip install --upgrade pip
RUN python3 -m pip install -r requirements.txt
RUN chmod +x /script-run.sh
ENTRYPOINT ["python3","main.py"]
CMD []
