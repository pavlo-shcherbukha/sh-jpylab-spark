FROM jupyter/pyspark-notebook:latest
#RUN pip install delta-spark==3.2.0
COPY ./requirements_dev.txt /home/jovyan/requirements_dev.txt
RUN pip install -r /home/jovyan/requirements_dev.txt