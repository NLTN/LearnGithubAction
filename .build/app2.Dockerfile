FROM python:3.9

WORKDIR /usr/src/myapp2

COPY app2/*.py ./
RUN pip3 install requests schedule

CMD ["python3", "app2.py"]