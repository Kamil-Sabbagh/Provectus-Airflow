import json
from airflow.models.baseoperator import BaseOperator
import csv


class MapOperator(BaseOperator):
    def __init__(self, name: str, data, **kwargs) -> None:
        super().__init__(**kwargs)
        self.name = name
        self.data = data



    def execute(self, context):
        task_instance = context['task_instance']
        dic = {}

        print(len(self.data))
        for word in self.data:

            filtered_word = ""
            for letter in word:
                if str.isalpha(letter):
                    filtered_word += letter

            if filtered_word in dic.keys():
                dic[filtered_word] += 1
            else:
                dic[filtered_word] = 1


        task_instance.xcom_push(self.name, json.dumps(dic))
        print("-------------------------------------------------------------")
        print(json.dumps(dic))
        return dic



##############################################################
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

        a_file = open("output.csv", "w")
        writer = csv.writer(a_file)

        for key, value in res.items():
            writer.writerow([key, value])

        a_file.close()
        print(res)
        task_instance.xcom_push(self.name, res)
        return res


