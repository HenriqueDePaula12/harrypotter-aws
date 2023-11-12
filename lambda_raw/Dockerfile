FROM public.ecr.aws/lambda/python:3.10

ENV AWS_ACCESS_KEY_ID="XXXXXXXXXXXXXX"
ENV AWS_SECRET_ACCESS_KEY="XXXXXXXXXXXXXX"
ENV AWS_DEFAULT_REGION="us-east-1"
ENV AWS_DEFAULT_OUTPUT="json"

COPY requirements.txt ./  
COPY lambda_function.py ./
 
RUN python3 -m pip install --upgrade pip  
RUN python3 -m pip install -r requirements.txt  

CMD [ "lambda_function.lambda_handler" ]