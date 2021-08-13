FROM python:3.6-slim

WORKDIR /usr/src/app

COPY requirements.txt ./
COPY Dynamodb_Analysis_upload.py ./
COPY S3_AnalysisOutput_upload.py ./
COPY nih_rcr.py ./
COPY altmetric.py ./
COPY run-data-import.sh ./
COPY init.py ./

RUN pip3 install --no-cache-dir -r requirements.txt
RUN mkdir -p ReCiter
RUN mkdir -p AnalysisOutput


RUN chmod a+x run-data-import.sh

CMD [ "/bin/bash", "-c", "python3 ./reciter_create_table.py && python3 ./Dynamodb_Analysis_upload.py && python3 ./S3_AnalysisOutput_upload.py && python3 ./nih_rcr.py && python3 ./altmetric.py" ]