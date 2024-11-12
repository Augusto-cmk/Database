from data.util.database import Database
from threading import Thread,Event
import time
import os
from tabulate import tabulate
from queue import Queue
from copy import copy
import readchar
import sys
import select


class EventDatabaseManager:
    def __init__(self):
        self.__status = {}
        self.__fila_monitor = Queue()
        self.__skip_thread = Thread(target=self.__all_skip)
        self.__threads:list[Thread] = []
        self.__monitor = Thread(target=self.monitor_status)
        self.__runn = {}
        self.__stop_event = Event()
        self.__stop_skip_key = Event()
        self.__time = None
    
    def __all_skip(self):
        while not self.__stop_skip_key.is_set():
            if self.__is_key_pressed('q'):
                self.__skip()
                self.__join()
                break
    
    def __is_key_pressed(self, key):
        if sys.stdin in select.select([sys.stdin],[],[],0)[0]:
            pressed_key = readchar.readchar()
            if pressed_key == key:
                return True
        return False
    
    def start(self):
        if len(self.__threads) > 0:
            self.__monitor.start()
            for thread in self.__threads:
                thread.start()
            self.__skip_thread.start()
            self.__join()
        else:
            print("No events do execute, empty tasks")
    
    def exec(self,names_in:list[str],names_out:list[str],overwrite=False):
        def thread_func(func,db:Database,*args,**kwargs):
            self._set_status(func.__name__,"waiting",0,len(db))
            
            while True:
                if set(names_in).issubset(db.columns):
                    break
                if not self.__runn[func.__name__]:
                    self._set_status(func.__name__,"skiped",0,len(db))
                    return None
            
            self._set_status(func.__name__,"executing",time.time(),len(db))
            if len(db) > 0:
                try:
                    result = db[names_in].apply(func, axis=1,*args,**kwargs)
                    if result.empty:
                        db[names_out] = [None for i in range(len(names_out))]
                    if len(result.columns) == len(names_out):
                        if not overwrite:
                            result.columns = names_out
                            existing_cols = list(set(db.columns) & set(names_out))
                            new_cols = list(set(names_out) - set(existing_cols))
                            
                            for col in existing_cols:
                                mask = db[col].isna() | (db[col] == None)
                                db[col] = db[col].where(~mask, result[col],axis=0)

                            db[new_cols] = result[new_cols]
                        else:
                            db[names_out] = result
                    
                    else:
                        raise ValueError(f"O número de colunas no resultado não corresponde ao número de colunas esperadas.")

                    if not self.__stop_event.is_set():
                        self._set_status(func.__name__,"finish",time.time()-self.__status[func.__name__]['time'],len(db))
                    
                    else:
                        self._set_status(func.__name__,"finish-forced",time.time() - self.__status[func.__name__]['time'],len(db))
                    
                except Exception as e:
                    self._set_status(func.__name__,f"error - {e}",time.time() - self.__status[func.__name__]['time'],len(db))
                    self.__skip()
            
            else:
                self._set_status(func.__name__,f"Nothing to do. Empty values",time.time() - self.__status[func.__name__]['time'],len(db))
        
        def do(func):
            def wrapper(db:Database,*args,**kwargs):
                if self.__time == None:
                    self.__time = time.time()
                thread = Thread(target=thread_func,args=[func,db,*args],kwargs=kwargs)
                self.__runn[func.__name__] = True
                self.__threads.append(thread)
            return wrapper
        return do
    
    def __join(self):
        for thread in self.__threads:
            if thread.is_alive():
                thread.join()
            
        self.__fila_monitor.put(None)
        self.__stop_skip_key.set()
        if self.__monitor.is_alive():
            self.__monitor.join()
        
        self.__print_total_time()
    
    def _set_status(self,fun_name:str,status:str,time:float,size:int):
        self.__status[fun_name] = {'status':status,'time':time,'size':size}
        self.__fila_monitor.put(copy(self.__status))
    
    def monitor_status(self):
        while True:
            status = self.__fila_monitor.get()
            if status:
                os.system("clear")
                table = []
                print("Press Enter and press Q to quit this execution...")
                print(f"Total of tasks: {len(self.__threads)}")
                for func_name, status in status.items():
                    table.append([func_name,status['status'],f"{status['time']:.2f}",status['size']])
                print(tabulate(table,headers=["name","status","time_elapsed (s)","size"],tablefmt="simple_grid"))
            else:
                break
    
    def __print_total_time(self):
        total = (time.time() - self.__time)/60
        print(f"::::::::::::::| TOTAL TIME ELAPSED = {total:.2f} minutes | Finished {len(self.__threads)} items | {len(self.__threads)/total:.2f} items per minutes |::::::::::::::::")
        input("any-> continue...")
    
    def __skip(self):
        self.__stop_event.set()
        for key in self.__runn.keys():
            self.__runn[key] = False