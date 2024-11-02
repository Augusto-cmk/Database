from src.util.database import Database
from sklearn.datasets import load_iris
from time import sleep
from src.util.event import EventDatabaseManager

iris = load_iris()

df = Database(data=iris.data, columns=iris.feature_names)
e = EventDatabaseManager()

@e.exec(['sepal length (cm)'],['teste'])
def teste(valor):
    sleep(10)
    return valor

@e.exec(['teste'],['teste'])
def teste2(valor):
    sleep(10)
    return valor*2

teste(df)
teste2(df)
e.start()
print(df)