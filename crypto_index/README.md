# Cryptocurrency and stock index statistics

## Create flows for collecting API data from the crypto exchange and NASDAQ

![Снимок экрана от 2022-06-06 01-35-57](https://user-images.githubusercontent.com/89353523/172073461-40b00b9b-9f3b-48f1-bc9d-4f5eb3310cac.png)

![Снимок экрана от 2022-06-06 01-35-54](https://user-images.githubusercontent.com/89353523/172073464-dbe2cc05-afe5-4a3a-a9e8-ed4821e18e4c.png)

## Configure saving data in HDFS.
Then we set up orchestration using crons
Due to the fact that the exchange is open from 17:30 to 00:00, data is collected during this period in order to be able to compare data and determine the impact of some indicators on others.
Save data set to 00:10

![Снимок экрана от 2022-06-06 01-42-18](https://user-images.githubusercontent.com/89353523/172073657-2503badb-c92c-4e8e-a6aa-3963de8204fb.png)

You can look at the data loading processes in more detail in the NIFI files

## Data visualization in ploty

![Снимок экрана от 2022-06-06 01-46-16](https://user-images.githubusercontent.com/89353523/172073781-3d1b78c0-7c23-48fa-90a9-39983762086e.png)
