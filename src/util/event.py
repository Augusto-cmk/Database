from src.util.database import Database,DataFrame
from threading import Thread,Event
import time
import os
from tabulate import tabulate
from queue import Queue
from copy import copy
import readchar
import numpy as np
import sys
import select


class EventDatabaseManager:
    def __init__(self,db:Database):
        self.__status = {}
        self.__fila_monitor = Queue()
        self.__skip_thread = Thread(target=self.__all_skip)
        self.__threads:list[Thread] = []
        self.__monitor = Thread(target=self.monitor_status)
        self.__monitor.start()
        self.__runn = {}
        self.__columns_to_out = []
        self.__columns_to_in = []
        self.__stop_event = Event()
        self.__stop_skip_key = Event()
        self.__time = None
        self.db = db
    
    def __all_skip(self):
        while not self.__stop_skip_key.is_set():
            if self.__is_key_pressed('q'):
                self.__skip()
                self._join()
                break
    
    def __is_key_pressed(self, key):
        if sys.stdin in select.select([sys.stdin],[],[],0)[0]:
            pressed_key = readchar.readchar()
            if pressed_key == key:
                return True
        return False

    def __main(self):
        if not set(np.unique(self.__columns_to_in)).issubset(np.unique([*self.db.columns,*self.__columns_to_out])):
            self.__skip()
        else:
            self.__skip_thread.start()
    
    def exec(self,func,names_in:list[str],names_out:list[str],thread=True,**args):
        def thread_func():
            self._set_status(func.__name__,"waiting",0)
            
            while True:
                if set(names_in).issubset(self.db.columns):
                    break
                if not self.__runn[func.__name__]:
                    self._set_status(func.__name__,"skiped",0)
                    return None
            
            self._set_status(func.__name__,"executing",time.time())
            if len(self.db) > 0:
                try:
                    result = self.db[names_in].apply(func, axis=1,**args)
                    if result.empty:
                        self.db[names_out] = [None for i in range(len(names_out))]
                    if len(result.columns) == len(names_out):
                        result.columns = names_out
                        existing_cols = list(set(self.db.columns) & set(names_out))
                        new_cols = list(set(names_out) - set(existing_cols))

                        for col in existing_cols: ## alterar isso para que funcione no database
                            mask = self.db[col].isna() | (self.db[col] == None)
                            self.db.loc[mask,col] = result.loc[mask, col]

                        self.db[new_cols] = result[new_cols]
                    
                    else:
                        raise ValueError(f"O número de colunas no resultado não corresponde ao número de colunas esperadas.")

                    if not self.__stop_event.is_set():
                        self._set_status(func.__name__,"finish",time.time()-self.__status[func.__name__]['time'])
                    
                    else:
                        self._set_status(func.__name__,"finish-forced",time.time() - self.__status[func.__name__]['time'])
                    
                except Exception as e:
                    self._set_status(func.__name__,f"error - {e}",time.time() - self.__status[func.__name__]['time'])
                    self.__skip()
            
            else:
                self._set_status(func.__name__,f"Nothing to do. Empty values",time.time() - self.__status[func.__name__]['time'])
        
        if thread:
            if self.__time == None:
                self.__time = time.time()
            
            thread = Thread(target=thread_func)
            self.__runn[func.__name__] = True
            thread.start()
            self.__threads.append(thread)
            self.__columns_to_out = [*self.__columns_to_out,*names_out]
            self.__columns_to_in = [*self.__columns_to_in,*names_in]
        
        else:
            if self.__time == None:
                self.__time = time.time()
            
            thread_func()
    
    def _join(self):
        self.__main()
        for thread in self.__threads:
            if thread.is_alive():
                thread.join()
            
        self.__fila_monitor.put(None)
        self.__stop_skip_key.set()
        if self.__monitor.is_alive():
            self.__monitor.join()
        
        self.__print_total_time()
    
    def _set_status(self,fun_name:str,status:str,time:float):
        self.__status[fun_name] = {'status':status,'time':time}
        self.__fila_monitor.put(copy(self.__status))
    
    def monitor_status(self):
        while True:
            status = self.__fila_monitor.get()
            if status:
                os.system("clear")
                table = []
                print("Press Enter and press Q to quit this execution...")
                print(f"Total size of database: {len(self.db)}")
                for func_name, status in status.items():
                    table.append([func_name,status['status'],f"{status['time']:.2f}"])
                print(tabulate(table,headers=["name","status","time_elapsed (s)"],tablefmt="simple_grid"))
            else:
                break
    
    def __print_total_time(self):
        total = (time.time() - self.__time)/60
        print(f"::::::::::::::| TOTAL TIME ELAPSED = {total:.2f} minutes | Solved {len(self.db)} items | {len(self.db)/total:.2f} items per minutes |::::::::::::::::")
        input("any-> continue...")
    
    def __skip(self):
        self.__stop_event.set()
        for key in self.__runn.keys():
            self.__runn[key] = False