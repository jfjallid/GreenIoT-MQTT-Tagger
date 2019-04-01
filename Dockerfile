FROM python:3
RUN mkdir /app
WORKDIR /app
ADD tagger/tagger.py /app/
RUN pip install -r requirements.txt

CMD["python", "/app/tagger.py"]
