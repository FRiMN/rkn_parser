FROM python:3.9
# Need python3.9 for open dbm with fast mode
# <https://docs.python.org/3.9/library/dbm.html#module-dbm.gnu>

WORKDIR /usr/src/app
ENV PYTHONPATH=.

COPY requirements.txt ./
RUN pip3 install --no-cache-dir -r requirements.txt

RUN apt update && apt install python3-gdbm

COPY ./src ./
COPY ./tests ./tests
