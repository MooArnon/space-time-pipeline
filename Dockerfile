FROM python:3.9

COPY requirements.txt requirements.txt
COPY space_time_pipeline space_time_pipeline
COPY debug.py debug.py

RUN pip install -r requirements.txt

CMD ["bin/bash"]