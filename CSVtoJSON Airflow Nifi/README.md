This ETL process converts data from DSV to Json. To accomplish this task, a script was written in python, which runs in Apache Airflow.

![image](https://user-images.githubusercontent.com/89353523/169509156-f2c42efb-1767-4c14-9f12-690303bc2968.png)

This task was also solved by constructing pipeline in Apache Nifi. The pipeline not only converts the data but also parses.

![image](https://user-images.githubusercontent.com/89353523/169520679-55ab490f-ee18-4936-9a6b-d678e9329d4c.png)

Parsing result: Get a subset of 1000 files - all people over 40 - on disk and named by their unique ID.

![image](https://user-images.githubusercontent.com/89353523/169520724-20b42b5e-bf60-4c62-aa73-0c3b16cac504.png)
