FROM python:alpine 
MAINTAINER Guilherme Thomazi Bonicontro <thomazi@linux.com>

ADD . /shiftpy
WORKDIR /shiftpy
ENV FLASK_APP=app.py
EXPOSE 5000

RUN pip install -r pip-requirements.txt
CMD ["flask", "run"]
