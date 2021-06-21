FROM scipy/scipy-dev:20210524-master-aec8decd
#FROM python:3.8-alpine3.12


#USER nobody
ENV PYTHON_MODULES="matplotlib numpy pika psycopg2 scikit-image sqlalchemy"

#RUN apk add --no-cache build-base postgresql-dev lapack lapack-dev blas-dev
#RUN pip3 install --upgrade pip
#RUN pip install --upgrade pip setuptools wheel
RUN pip install $PYTHON_MODULES
