FROM python:3

WORKDIR /usr/src/app

COPY requirements.txt ./
COPY Dynamodb_Analysis_upload.py ./
COPY S3_AnalysisOutput_upload.py ./
COPY run-data-import.sh ./
COPY init.py ./

RUN pip install --no-cache-dir -r requirements.txt
RUN mkdir -p ReCiter
RUN mkdir -p AnalysisOutput


RUN chmod a+x run-data-import.sh

CMD [ "/bin/bash", "-c", "python Dynamodb_Analysis_upload.py && python S3_AnalysisOutput_upload.py" ]