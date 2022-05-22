# Data visualization in Tableau
At the first step, we unload and select data from hdfs using Spark and load this data into PostgreSQL. 
![Снимок экрана от 2022-05-22 17-00-12](https://user-images.githubusercontent.com/89353523/169698923-aa1a6f26-be1a-4db7-a258-4e2248afb52f.png)
In the second step, using Apache Airflow, we upload the table to the CSV.
At the third stage, we load the data into the tableau and prepare the visualization.

# Result
Displaying cumulative deaths by U.S. region for three years from coronovirus.
![Sheet 2 (1)](https://user-images.githubusercontent.com/89353523/169698573-8ad7a05d-1282-4d54-a2f1-607e22582211.png)
