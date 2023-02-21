FROM python:3.9

WORKDIR /usr/src/myapp

COPY app1/*.py ./
RUN pip3 install requests schedule

CMD ["python3", "app1.py"]