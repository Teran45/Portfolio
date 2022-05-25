# Aviation statistics

At the first step, the task was to set up the rest api and transfer information to PostgreSQL. Nifi using for this task.

![Снимок экрана от 2022-05-26 02-26-06](https://user-images.githubusercontent.com/89353523/170386095-64691af9-c2d4-4bb0-b206-7ad8a661e909.png)

An important task was to obtain high-quality api in real time and parse the received data. JoltTransformJSON was used for the correct JSON schema.

![Снимок экрана от 2022-05-26 02-35-10](https://user-images.githubusercontent.com/89353523/170386407-42f63fbd-8761-44f7-9868-e1f7609c453b.png)

At the second stage, using spark, information is saved in the format we need in HDFS.

![Снимок экрана от 2022-05-26 02-24-59](https://user-images.githubusercontent.com/89353523/170386457-fa3d399c-ed84-4abf-9b2a-f013884aecb2.png)

The third step is data visualization.

## Display of aircraft speed at the time of request.

![Снимок экрана от 2022-05-26 02-53-31](https://user-images.githubusercontent.com/89353523/170388141-207eb275-7923-4597-afa2-1de233d9360f.png)

## Displaying the number of unique aircraft for each country

![Снимок экрана от 2022-05-26 02-23-49](https://user-images.githubusercontent.com/89353523/170386559-2c4550f6-dfb6-4811-9bf1-033113c2ba37.png)
