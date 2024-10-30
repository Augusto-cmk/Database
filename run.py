from src.util.database import Database,DataFrame
from sklearn.datasets import load_iris
from time import time,sleep

iris = load_iris()

def teste(valor):
    sleep(10)
    return valor

df = Database(data=iris.data, columns=iris.feature_names)

start = time()
df['sepal length (cm)'].apply(teste,axis=1)
finish = time()
print("Tempo apply com threads: ",finish-start)

start = time()
DataFrame(df['sepal length (cm)']).apply(lambda x:teste(x),axis=1)
finish = time()
print("Tempo apply sem threads: ",finish-start)