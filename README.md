#  Learning SPARK DataLake

<!-- TOC BEGIN -->

- [1. About](#p-1")
- [2. Your personal alternative](#p-2)
- [2.1. How to run 1-node runtime master and worker simultaneously](#p-2.1)
- [2.2. Professional approach: Docker Compose](#p-2.2)
- [3. Your first notebook](#p-3)
- [4. How organized work with labs](#p-4)
- [5. Labs descriptions](#p-5)
- [5.1. Lab_0 - test conection to the Spark](#p-5.1)
- [5.2. Lab_1 - Scenario: Merge/Upsert](#p-5.2)
- [5.3. Lab-2: Parallel computing and Partitioning](#p-5.3)
- [6. Helpfull links](#p-6)


<!-- TOC END -->
## <a name="p-1">1. About</a> 


## <a name="p-2">2. Your personal alternative</a>



My personal alternative is buld using docker image: [/jupyter/pyspark-notebook](https://hub.docker.com/r/jupyter/pyspark-notebook).

### <a name="p-2.1">2.1. How to run 1-node runtime master and worker simultaneously</a>

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



### <a name="p-2.2">2.2. Professional approach: Docker Compose</a>

I build my own image, because of to work with delta lake I need install addition packeges. In addition if you need to add some pyhton packages you need to build your own image. So, I have done it at the beginning.

My Docker file is here: [myspark.Dockerfile](./myspark.Dockerfile) and  do not forget  .dockerignore.
In docker file you can see only one command:

```bash

COPY ./requirements_dev.txt /home/jovyan/requirements_dev.txt
RUN pip install -r /home/jovyan/requirements_dev.txt

```

It is important to use compatible versions  delta-spark:  **delta-spark==3.2.0**.
I put into file: **requirements_dev.txt** all nessesary packages, which I am going to use.
If you need any package, and it into **requirements_dev.txt** and rebuild docker-compose.

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
      - "4040-4045:4040-4045" # Ports for Application UI
      - "8081:8081"
      - "8082:8082"
    environment:
      - JUPYTER_ENABLE_LAB=yes
    volumes:
      - ./notebooks:/home/jovyan/work
    # Start  Master as background process, then Jupyter
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
    # start worker using spark-class
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


## <a name="p-3">3. Your first notebook

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

## <a name="p-4">How organized work with labs</a>

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

## <a name="p-5">5. Labs descriptions</a>

### <a name="p-5.1">5.1. Lab_0 - test conection to the Spark</a>

- [Lab-0_TestConnection-1.ipynb](./notebooks/Lab-0_TestConnection-1.ipynb), whith example, how to connect to Spark withoud Delta Lake.
- [Lab-0_TestConnection-2.ipynb](./notebooks/Lab-0_TestConnection-2.ipynb), whith example, how to connect to Spark with Delta Lake.

### <a name="p-5.2">5.2. Lab_1 - Scenario: Merge/Upsert</a>

In Lab-1, we focus on the "magic" of Delta Lake, namely the ability to do MERGE (Upsert), which is an impossible task for conventional file systems (such as pure Parquet or CSV).

**Notebook:** [Lab-1_PrepareTestData.ipynb](./notebooks/Lab-1_PrepareTestData.ipynb)

Using python package **Faker**, we can do the following:

- Generate 1000 customers (Version 1).
- Generate another 200 new customers + 100 customers with changed balances (Version 2).
- Use MERGE INTO to update our Delta table.
- Using DESCRIBE HISTORY, we see how Delta Lake recorded these changes.

**Things to note:**

- Pandas generation: We used pd.DataFrame(data), which is the fastest way to generate medium-sized test sets.
- Localization: Using Faker allows you to create realistic customer profiles (email, addresses, names).
- SQL validation: We immediately confirmed the success of the write via SELECT * FROM bronze_clients.

### <a name="p-5.3">5.3. Lab-2: Parallel computing and Partitioning</a>

**Objective:**

- Generate a large amount of data (e.g. 1 million transactions).
- See task distribution in Spark Web UI.
- Learn to do Partitioning to speed up queries.

**Notebook**: [Lab-2_ParallelComputing.ipynb](./notebooks/Lab-2_ParallelComputing.ipynb)

**Things to note:**

- **Connection**

See and remeber addition options

```py
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

builder = SparkSession.builder \
    .appName("Lab-2-2-ParallelProcessing") \
    .config("spark.ui.port", "4040") \ # request port for monitor application UI wich is registered as .appName
    .config("spark.ui.enabled", "true") \ #  enable  application UI
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.executor.instances", "2") \  # request explicitly 2 workers
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.warehouse.dir", "lab_2/lakehouse") 

spark = configure_spark_with_delta_pip(builder).getOrCreate()
print(f"Spark version: {spark.version} with Delta support Lab-2")

```

- **Paritioning**

If you go to the notebooks/lab_2/lakehouse/partitioned_transactions folder in Windows Explorer now, you'll see the magic:

- There's no one big file there.
- There are separate folders there: city=Kyiv, city=Lviv, city=Odesa, etc.
- Inside each folder are your .parquet files.

In Oracle you use PARTITION BY RANGE/LIST for large tables. In Spark it works the same way:
The next time you write SELECT * FROM table WHERE city='Kyiv', Spark won't even open the folders with other cities. It will go straight to the desired directory. This is called Partition Pruning.

## <a name="p-6">6. Helpfull links</a>

- [Ms pyspark basics](https://learn.microsoft.com/en-en/azure/databricks/pyspark/basics);
- [PySpark data types](https://learn.microsoft.com/en-en/azure/databricks/pyspark/reference/datatypes)
- [PySpark functions](https://learn.microsoft.com/en-us/azure/databricks/pyspark/reference/functions/)
- [SQL language reference](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/)
- [py-spark SQL](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/index.html)


- [Welcome to the Delta Lake documentation](https://docs.delta.io/index.html)


- [delta.io](https://delta.io/)
- [delta-faq](https://docs.delta.io/delta-faq/)
- [About Delta](https://www.cidrdb.org/cidr2021/papers/cidr2021_paper17.pdf)

