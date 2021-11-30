# Provectus-Airflow Task 1

Assignment submission for the Introduction to Big Data course, 

_Provectus Internship, 2021_

## Table of contents
1. [ Introduction ](#intro)
2. [ Implementation in Scala ](#Implementation)
3. [ Running the coed ](#Running)


___
<a name="intro"></a>
## 1. Introduction
In this task we take the `tweets.csv` dataset and read its words. then we implement a map-reduce operation to count the frequancy of each word in all the text of the dataset. To do so we used Apache Airflow.

<a name="Implementation"></a>
## 2. Implementation 
The created DAG contains three tasks `map1`, `map2`, `map3`, and `reduce`. and two custom created operators `MapOperator` and `ReduceOperator`, the first is the one responsible of getting the frequency of each word in its part of the data set, the latter is resposnible of putting these results together to provide the final output.

<a name="Running"></a>
## 3. Running the code

first things first install the requirements from `requirements.txt`:
```
pip install -r requirements.txt
```
Then run the airflow webserver:
```
airflow webserver
```
after the running the server you can checkout your DAGs and their status on http://localhost:8080/

next we run the DAG using the command:
```
airflow dags backfill map-reduce --start-date 2021-11-30
```
you may setup the date to whatever date you want.
after running the command you should be able to view the status and logs of the map-reduce dag. the output map will be saved in `output.csv`a
