FROM python:3.10-slim-buster

WORKDIR /model-deploy

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

COPY . .

CMD [ "python3","app.py" ]