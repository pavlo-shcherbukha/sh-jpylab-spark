# Вивчення SPARK

## корисні лінки

- [Ms pyspark basics](https://learn.microsoft.com/ru-ru/azure/databricks/pyspark/basics);

- [py-spark SQL](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/index.html)

- [Welcome to the Delta Lake documentation](https://docs.delta.io/index.html)


```bash

docker run -p 8888:8888 -v C:/DEV/pyspark/notebooks:/home/jovyan/work jupyter/pyspark-notebook
```

3. Професійний підхід: Docker Compose

Якщо ви хочете поекспериментувати зі справжнім кластером (один Master-вузол і кілька Worker-вузлів), краще використовувати docker-compose. Це дозволить вам побачити, як дані розподіляються між різними "машинами".

Ось приклад простого файлу docker-compose.yml (використовуючи популярні образи Bitnami):

```yaml

version: '3'
services:
  spark-master:
    image: bitnami/spark:latest
    environment:
      - SPARK_MODE=master
    ports:
      - '8080:8080'
      - '7077:7077'

  spark-worker:
    image: bitnami/spark:latest
    environment:
      - SPARK_MODE=worker
      - SPARK_MASTER_URL=spark://spark-master:7077
    depends_on:
      - spark-master

Запустивши це командою docker-compose up, ви отримаєте:

    Spark Master (веб-інтерфейс на порту 8080).

    Spark Worker (який виконуватиме обчислення).
```    


1. Spark UI (Веб-інтерфейс)

Це головна причина давати зрозумілу назву. Якщо ви перейдете в браузері за адресою localhost:4040 (поки ваша сесія активна), ви побачите панель моніторингу. Там, у списку запущених застосунків, ви побачите саме "LearningStep1".

    Якщо у вас запущено кілька різних скриптів, за appName ви зрозумієте, де саме зараз рахується важкий Join, а де виникла помилка.

    Треба тільки порт в контейнері виставити

2. Логи (Logs)

Коли Spark пише звіти про свою роботу в консоль або у файли, він позначає їх цією назвою. В умовах реальної роботи в компанії (на великому кластері), системні адміністратори шукають ваші помилки саме за цим ім'ям.
Як правильно її вибирати?

Хоча технічно можна написати будь-що, зазвичай дотримуються таких правил:

    Для навчання: як ви і зробили (LearningStep1, TestApp).

    Для продакшн-задач: зазвичай вказують назву процесу та дату або версію, наприклад: ETL_Sales_Data_Daily або User_Behavior_Analytics_v2.

Чи можна її змінити?

Після того, як ви виконали .getOrCreate(), змінити назву вже існуючої сесії не можна. Потрібно або закрити сесію (spark.stop()), або перезапустити ядро ноутбука.
Відповідь на моє попереднє питання про Lazy Evaluation:

Ви правильно помітили, що код працює. Щодо мого питання: якби ви написали 10 фільтрів, але не викликали .show(), процесор би нічого не робив. Spark просто побудував би логічний план обробки в пам'яті. Це круто тим, що перед тим як реально рахувати, Spark "дивиться" на всі ваші 10 фільтрів і намагається їх оптимізувати (наприклад, об'єднати в один), щоб не ганяти дані туди-сюди дарма.

- Запустити контейнер

```bash
docker start dreamy_wescoff
```

- ЗУпинити

```bash

docker stop dreamy_wescoff

```

- Подивитися лог, що знайти url для запуску Jupyter Lab

```bash

docker logs dreamy_wescoff -f
```