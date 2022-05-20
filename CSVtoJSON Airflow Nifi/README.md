This ETL process converts data from DSV to Json. To accomplish this task, a script was written in python, which runs in Apache Airflow.

![image](https://user-images.githubusercontent.com/89353523/169509156-f2c42efb-1767-4c14-9f12-690303bc2968.png)

This task was also solved by constructing pipeline in Apache Nifi. The pipeline not only converts the data but also parses it by name

![image](https://user-images.githubusercontent.com/89353523/169519959-433c3a10-baf7-4d2d-ae08-4a05f68ccd01.png)

Parsing result

![image](https://user-images.githubusercontent.com/89353523/169520124-8054623a-aa27-4cb1-955d-6f2179ab1a3e.png)
