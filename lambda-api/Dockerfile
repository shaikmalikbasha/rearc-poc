FROM public.ecr.aws/lambda/python:3.12

# Copy your code into the task root directory
COPY lambda_function.py ${LAMBDA_TASK_ROOT}

# Set the CMD to your handler (filename.function_name)
CMD [ "lambda_function.handler" ]