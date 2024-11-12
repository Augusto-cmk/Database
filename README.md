# Database e EventDatabaseManager

## Descrição Geral

As classes `EventDatabaseManager` e `Database` foram desenvolvidas para facilitar a execução paralela e o monitoramento de tarefas em segundo plano, com uma interface amigável para manipulação de dados em larga escala. A `EventDatabaseManager` gerencia o status e o monitoramento das tarefas, enquanto a `Database` é uma extensão do `DataFrame` do Pandas que oferece métodos otimizados para exibição e aplicação de funções.

### Instalação

`pip install database-event`

### Utilização


```python
from data.util.database import Database
from sklearn.datasets import load_iris
from time import sleep
from data.util.event import EventDatabaseManager

# Carrega o conjunto de dados e inicializa as classes
iris = load_iris()
df = Database(data=iris.data, columns=iris.feature_names)
e = EventDatabaseManager()

# Define as funções a serem executadas em segundo plano
@e.exec(['sepal length (cm)'], ['teste'])
def teste(valor):
    sleep(10)
    return valor

@e.exec(['teste'], ['teste'], overwrite=True)
def teste2(valor):
    sleep(10)
    return valor * 2

# Inicia as execuções e o monitoramento
teste(df)
teste2(df)
e.start()
print(df)
```


### Classes
**EventDatabaseManager**
A classe EventDatabaseManager gerencia tarefas executadas em threads separadas, controlando o status de execução e permitindo o monitoramento contínuo das tarefas.

**Atributos Privados**
**__status**: Dicionário que armazena o status atual de cada função.
**__fila_monitor**: Fila de monitoramento para atualizar o status das tarefas.
**__skip_thread**: Thread para monitorar uma tecla de atalho que permite encerrar as execuções.
**__threads**: Lista de threads onde cada função definida pelo usuário é associada a uma thread.
**__monitor**: Thread para monitoramento e exibição em tempo real do status das funções.
**__runn**: Dicionário que controla o estado de execução de cada função.
**__stop_event**: Evento de parada global para todas as execuções.
**__stop_skip_key**: Evento de parada específico para a thread de monitoramento de teclas.
**__time**: Armazena o tempo inicial das execuções para cálculo de tempo total.
Métodos
**start()**: Inicia as execuções e o monitoramento de status das tarefas. Se não houver tarefas na fila, exibe uma mensagem indicando que não há tarefas para executar.

**exec(names_in: list[str], names_out: list[str], overwrite=False)**: Define uma função a ser executada em uma thread separada. Recebe os nomes das colunas de entrada (names_in) e saída (names_out) que serão utilizados no Database, além de um parâmetro overwrite que indica se os dados existentes nas colunas devem ser sobrescritos.

**_set_status(fun_name: str, status: str, time: float, size: int)**: Define o status de uma função específica durante a execução, incluindo informações sobre o tempo e o tamanho dos dados processados.

**__is_key_pressed(key)**: Verifica se uma tecla específica foi pressionada, para permitir a finalização antecipada das execuções.

**__all_skip()**: Monitora se a tecla de atalho foi pressionada, interrompendo todas as execuções e encerrando as threads.

**__join()**: Aguarda a finalização das threads e encerra o monitoramento, exibindo o tempo total de execução ao final.

**monitor_status()**: Exibe o status de execução de cada função em uma tabela atualizada em tempo real. Exibe uma mensagem de instrução para finalizar as execuções pressionando a tecla q.

**__print_total_time()**: Imprime o tempo total de execução de todas as tarefas processadas, incluindo uma média de tarefas executadas por minuto.

**__skip()**: Define a interrupção de todas as funções em execução e para o monitoramento.

**Database**
A classe Database é uma extensão de DataFrame que oferece métodos para uma exibição aprimorada e aplicação de funções de forma paralela.

**Métodos**
__str__(max_rows=10, max_cols=6): Retorna uma representação da estrutura de dados em formato de tabela, limitando o número de linhas e colunas exibidas para melhor visualização.

__getitem__(column: str | list[str]): Sobrescreve o método de obtenção de itens do DataFrame, retornando um novo Database com as colunas especificadas.

apply(func, axis=0, *args, **kwargs): Aplica uma função especificada de forma assíncrona em cada coluna (axis=0) ou linha (axis=1), utilizando threads para paralelizar a execução.

