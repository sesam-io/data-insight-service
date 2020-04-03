FROM python:3-alpine
RUN apk update&&\
    apk add --no-cache tini

RUN pip install --upgrade pip

COPY ./service/requirements.txt /service/requirements.txt
RUN pip install -r /service/requirements.txt

COPY ./service /service
WORKDIR /service/

EXPOSE 5000

ENTRYPOINT ["/sbin/tini", "--"]
CMD ["python3", "insight.py"]