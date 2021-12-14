import json
from airflow.models.baseoperator import BaseOperator
from sqlalchemy import create_engine
import airflow.providers.amazon.aws.hooks.s3



class MapOperator(BaseOperator):
    def __init__(self, name: str, data, **kwargs) -> None:
        super().__init__(**kwargs)
        self.name = name
        self.data = data

    def execute(self, context):
        task_instance = context['task_instance']
        dic = {}
        data = json.loads(task_instance.xcom_pull(task_ids='read', key='read'))

        for word in data:
            filtered_word = ""
            for letter in word:
                if str.isalpha(letter):
                    filtered_word += letter

            if filtered_word in dic.keys():
                dic[filtered_word] += 1
            else:
                dic[filtered_word] = 1


        task_instance.xcom_push(self.name, json.dumps(dic))
        return dic


class ReduceOperator(BaseOperator):
    def __init__(self, name: str, data, **kwargs) -> None:
        super().__init__(**kwargs)
        self.name = name
        self.data = data

    def execute(self, context):
        res = {}
        task_instance = context['task_instance']
        map1 = json.loads(task_instance.xcom_pull(task_ids='map1', key='map1'))
        map2 = json.loads(task_instance.xcom_pull(task_ids='map2', key='map2'))
        map3 = json.loads(task_instance.xcom_pull(task_ids='map3', key='map3'))
        maps = [map1, map2, map3]
        for dic in maps:
            for word in dic.keys():
                if word in res.keys():
                    res[word] += dic[word]
                else:
                    res[word] = dic[word]

        print(res)
        task_instance.xcom_push(self.name, json.dumps(res))
        return res

class PGOperator(BaseOperator):
    def __init__(self, name: str, data, **kwargs) -> None:
        super().__init__(**kwargs)
        self.name = name
        self.data = data



    def execute(self, context):
        task_instance = context['task_instance']

        dic = json.loads(task_instance.xcom_pull(task_ids='reduce', key='reduce'))

        # connect to db
        url = "postgresql+psycopg2://airflow:airflow@postgres/Tweets"
        engine = create_engine(url)


        for key, value in dic.items():
            engine.execute("INSERT INTO Tweets (word, frequncy) VALUES(%s, %s)", (key, value))


        task_instance.xcom_push(self.name, json.dumps(dic))
        return dic





class Read_dataOperator(BaseOperator):
    def __init__(self, name: str, data, **kwargs) -> None:
        super().__init__(**kwargs)
        self.name = name
        self.data = data

    def execute(self, context):
        task_instance = context['task_instance']

        hook = airflow.providers.amazon.aws.hooks.s3.S3Hook('minio')
        if not hook.check_for_bucket("tweetsbucket"):
            hook.create_bucket("tweetsbucket")

        hook.load_file(filename="dags/tweets.csv", key='tweets.csv', bucket_name="tweetsbucket", replace=True)

        data = hook.read_key(key='tweets.csv', bucket_name="tweetsbucket")
        data = data.replace(",", " ")
        data = data.split(" ")
        task_instance.xcom_push(self.name, json.dumps(data))
        return data
