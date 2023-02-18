FROM python:3.9

WORKDIR /usr/src/myapp

COPY app2/*.py ./
RUN pip3 install requests schedule

CMD ["python3", "app2.py"]