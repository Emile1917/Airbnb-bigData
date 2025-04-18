FROM python:3.13

COPY main.py .

COPY requirements.txt .

RUN pip install -r requirements.txt

CMD [ "python3","./main.py" ]