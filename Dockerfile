FROM python:3.7
RUN apt-get update&&\
    apt-get install tini

RUN pip install --upgrade pip

COPY ./service/requirements.txt /service/requirements.txt
RUN pip install -r /service/requirements.txt

COPY ./service /service
WORKDIR /service/

EXPOSE 5000

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["python3", "insight.py"]