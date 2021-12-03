# Provectus-Airflow Task 2

_Provectus Internship, 2021_

## Table of contents
1. [ Introduction ](#intro)
2. [ Running the coed ](#Running)


___
<a name="intro"></a>
## 1. Introduction
In this task we create a docker-compose that does the following :
- `tweets.csv` dataset up upload it to the service minio
- download the dataset `tweet.csv` from minio
- apply map-reduce operations
- store the output on Postgresql database 

![alt text](https://i.ibb.co/wNNdrqw/Map-Reduce-DAG.png)



<a name="Running"></a>
## 2. Running the code

First, it's needed to [download] the dataset `tweets.csv` you can download it (https://dataverse.harvard.edu/dataset.xhtml?id=3047332)
and then store inside the dags file

Then we need to set up the environment, by running the commands:
```
mkdir minio
sudo chmod -R 777 minio
mkdir pgadmin
sudo chmod -R 777 pgadmin
mkdir logs
```

Finally, for the rest of the work, just building and running the docker will automatically set up the scheduler, webserver, connection links, and trigger the dag. so we just run the command:
```
docker-compose up -build
```

To check the DAG progress, we can visit the webserver ui at http://0.0.0.0:8080, by default the username = airflow, and password = airflow.
To check the database, we can visit the webserver ui at http://localhost:5050. 


