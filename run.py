from src.util.database import Database
from sklearn.datasets import load_iris
from time import sleep
from src.util.event import EventDatabaseManager

iris = load_iris()

def teste(valor):
    sleep(10)
    return valor

def teste2(valor):
    sleep(10)
    return valor

df = Database(data=iris.data, columns=iris.feature_names)
e = EventDatabaseManager(df)

e.exec(teste,['sepal length (cm)'],['teste'])
e.exec(teste2,['teste'],['teste2'])
e._join()
print(df)