FROM tiangolo/uvicorn-gunicorn-fastapi:python3.9

WORKDIR /usr/src/application

# COPY ./requirements.txt /api/requirements.txt

COPY requirements.txt ./

# RUN pip install --no-cache-dir --upgrade -r /api/requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

# COPY ./api /app/app

COPY . .

CMD [ "uvicorn", "main:analyzer", "--host", "0.0.0.0", "--port", "8080"]