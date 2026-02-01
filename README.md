#  Learning SPARK DataLake

## 2. Your personal alternative

My personal alternative is buld using docker image: [/jupyter/pyspark-notebook](https://hub.docker.com/r/jupyter/pyspark-notebook).

### 2.1. How to run 1-node runtime master and worker simultaneously

To run 1-node  **jupyter/pyspark-notebook**  you can use *docker run*.

Before, you must decide where is folder for persitant storage  and map it with  container folder.
For instance: 
my Jupyter Notebooks  I would like to see in  **C:/DEV/pyspark/notebooks**, so, I add it into docker run command like below.

- Create container

```bash

docker run -p 8888:8888 -p 4040:4040  -v C:/DEV/pyspark/notebooks:/home/jovyan/work jupyter/pyspark-notebook
```

- Check if it running:

```bash

docker ps
```

- Start contaiiner

```bash
docker start <container name/container id>
```

- Shutdown container

```bash

docker stop <container name/container id>

```

- See container log to find url for access Jupyter Lab

```bash

docker logs <container name/container id> -f
```
After run docker your must install packege to use Delta Lake in your first notebook. 

```bash
# Install Delta Lake for Spark 3.2.x
pip install delta-spark==3.2.0
```



### 2.2. Professional approach: Docker Compose

I build my own image, because of to work with delta lake I need install addition packeges. In addition if you need to add some pyhton packages you need to build your own image. So, I have done it at the beginning.

My Docker file is here: [myspark.Dockerfile](./myspark.Dockerfile) and  do not forget  .dockerignore.
In docker file you can see only one command:

```bash
pip install delta-spark==3.2.0

```

It is important to use compatible versions.
At the first build images for docker compose separeatly using command:

```bash
docker-compose build --no-cache
```

To run docker compose by commnad:

```bash
 docker-compose up -d
```
In case of successfull start your can access:

- Jupyter Lab by link http://localhost:8888;
- Spark UI by link http://localhost:8080

**docker-compose.yml**

```yaml
services:
  spark-master:
    #image: jupyter/pyspark-notebook:latest
    build:
      context: .
      dockerfile: ./myspark.Dockerfile
    container_name: spark-master
    user: root
    ports:
      - "8888:8888" # Jupyter Lab
      - "8080:8080" # Spark Master Web UI
      - "7077:7077" # Spark Master Port
    environment:
      - JUPYTER_ENABLE_LAB=yes
    volumes:
      - ./notebooks:/home/jovyan/work
    # Явно запускаємо Master як фоновий процес, а потім Jupyter
    command: >
      sh -c "/usr/local/spark/bin/spark-class org.apache.spark.deploy.master.Master --ip 0.0.0.0 & 
             start-notebook.sh --NotebookApp.token=''"

  spark-worker-1:
    #image: jupyter/pyspark-notebook:latest
    build:
      context: .
      dockerfile: ./myspark.Dockerfile
    container_name: spark-worker-1
    user: root
    depends_on:
      - spark-master
    environment:
      - SPARK_WORKER_CORES=1
      - SPARK_WORKER_MEMORY=1G
    # Запускаємо воркер через spark-class
    command: >
      sh -c "/usr/local/spark/bin/spark-class org.apache.spark.deploy.worker.Worker spark://spark-master:7077"

  spark-worker-2:
    #image: jupyter/pyspark-notebook:latest
    build:
      context: .
      dockerfile: ./myspark.Dockerfile
    container_name: spark-worker-2
    user: root
    depends_on:
      - spark-master
    environment:
      - SPARK_WORKER_CORES=1
      - SPARK_WORKER_MEMORY=1G
    command: >
      sh -c "/usr/local/spark/bin/spark-class org.apache.spark.deploy.worker.Worker spark://spark-master:7077"
```

- start docker compose 

```bash
docker-compose up -d
```

- stop docker compose

```bash
docker-compose down
```

it deletes all containers, but your files are remains in "./notebooks" directory in your persistent storage

- You can also use command:

```bash
docker-compose stop
```

It a bit quicker but your resource will not free


## 3. Your first notebook

I hope your remenber that delta-spark packege must be installed, see p. 2.1 and 2.2. with particular this version, because in this container I see spark version 3.5.

```bash
# Install Delta Lake for Spark 3.2.x
pip install delta-spark==3.2.0
```

So connection to spark with delta support wikk be like this:

```python
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType, DoubleType
from pyspark.sql.functions import col, when, sum, count, substring, lit, coalesce

from delta import configure_spark_with_delta_pip
from datetime import date
from delta.tables import DeltaTable

builder = SparkSession.builder \
    .appName("FabricSimulation") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.warehouse.dir", "lab_0/lakehouse") 

spark = configure_spark_with_delta_pip(builder).getOrCreate()
print(f"Spark version: {spark.version} with Delta support")
```

To run this code we must get this response.

``` text
    Spark version: 3.5.0 with Delta support

```

This fragment you need  paste in first row of your  notebooks  in order to establish connect connection.


If you try conect without Delta Lake, use this script:

```py
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("TestSparkConnection") \
    .getOrCreate()

print(f"Spark version: {spark.version} without Delta support")
```

response hould be like this

```text
Spark version: 3.5.0 without Delta support

```

## How organized work with labs

Every lab has lab own data store, which is described in this fragmant:

```py
  .....
  .config("spark.sql.warehouse.dir", "lab_0/lakehouse") 
```

Lab with numner 1  will be like this:

```py
  .....
  .config("spark.sql.warehouse.dir", "lab_0/lakehouse") 
```

lab 0 is about test conection. So you can see in notebooks 2 files:

- Lab-0_TestConnection-1.ipyn, whith example, how to connect to Spark withoud Delta Lake.
- Lab-0_TestConnection-1.ipyn, whith example, how to connect to Spark with Delta Lake.

During execution the second one you will see folder lab_0/lakehouse in your persistent storage and your table.

## Labs descriptions

### Lab_0 - test conection to the Spark
- Lab-0_TestConnection-1.ipyn, whith example, how to connect to Spark withoud Delta Lake.
- Lab-0_TestConnection-1.ipyn, whith example, how to connect to Spark with Delta Lake.

### Lab_0 - tbd

tbd


## Helpfull links

- [Ms pyspark basics](https://learn.microsoft.com/en-en/azure/databricks/pyspark/basics);
- [PySpark data types](https://learn.microsoft.com/en-en/azure/databricks/pyspark/reference/datatypes)
- [PySpark functions](https://learn.microsoft.com/en-us/azure/databricks/pyspark/reference/functions/)
- [SQL language reference](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/)
- [py-spark SQL](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/index.html)


- [Welcome to the Delta Lake documentation](https://docs.delta.io/index.html)


- [delta.io](https://delta.io/)
- [delta-faq](https://docs.delta.io/delta-faq/)
- [About Delta](https://www.cidrdb.org/cidr2021/papers/cidr2021_paper17.pdf)

