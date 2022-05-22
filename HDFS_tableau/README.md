# Data visualization in Tableau
At the first step, we unload and select data from hdfs using Spark and load this data into PostgreSQL
![Снимок экрана от 2022-05-22 16-49-10](https://user-images.githubusercontent.com/89353523/169698395-4df1192e-a0ae-4dec-b9c8-a6f84f8d8c00.png)
In the second step, using Apache Airflow, we upload the table to the CSV
At the third stage, we load the data into the tableau and prepare the visualization
Result: Displaying cumulative deaths by U.S. region for three years
![Sheet 2 (1)](https://user-images.githubusercontent.com/89353523/169698573-8ad7a05d-1282-4d54-a2f1-607e22582211.png)
