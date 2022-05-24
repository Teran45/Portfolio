# Creating a Data View
1. Using Oracle VMBox, we create a virtual machine based on the Ubuntu 18.04 image;
2. Install Docker;
3. Install ClickHouse Server;
4. Check the server's performance;
5. We establish a connection in DBeaver with the database;
6. Create a table to load raw data from CSV;

![image](https://user-images.githubusercontent.com/89353523/169996217-e33c4c66-0a54-455f-81e5-41800de79d66.png)

7. Next, load the data using the command line;

![image](https://user-images.githubusercontent.com/89353523/169996281-04564b4a-f415-4f89-abad-e40a0ae70c21.png)

8. Create a data view with the necessary query;

![image](https://user-images.githubusercontent.com/89353523/169996433-56b5bb27-1852-4620-be87-558e41664247.png)

In the CTE, the request delimits the beginning and end of the session, in the subquery of the request itself, we determine the sessions that did not end and display data on the end of the session in a separate column, in the request we add a random value for the session number and apply the necessary filters;

# Result

![image](https://user-images.githubusercontent.com/89353523/169996739-6869ff31-43a8-4cad-a758-6c5b6894bca8.png)
