from pandas import DataFrame,Series
from threading import Thread
from tabulate import tabulate

class Database(DataFrame):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
    
    def __str__(self, max_rows=10, max_cols=6):
        n_rows, n_cols = self.shape

        indices = self.index.tolist()

        if n_rows > max_rows:
            top_rows = self.head(max_rows // 2).values
            bottom_rows = self.tail(max_rows // 2).values
            data = [*top_rows, ["..."] * n_cols, *bottom_rows]
            indices = indices[:max_rows // 2] + ["..."] + indices[-max_rows // 2:]
        else:
            data = self.values

        if n_cols > max_cols:
            top_cols = list(self.columns[:max_cols // 2])
            bottom_cols = list(self.columns[-(max_cols // 2):])
            cols = ["index", *top_cols, "...", *bottom_cols]

            data = [
                [idx, *row[:max_cols // 2], "...", *row[-(max_cols // 2):]]
                for idx, row in zip(indices, data)
            ]
        else:
            cols = ["index", *self.columns]
            data = [[idx, *row] for idx, row in zip(indices, data)]

        return tabulate(data, headers=cols, tablefmt='simple_grid')

    def __getitem__(self, column: str | list[str]):
        return Database(super().__getitem__(column))
    
    def apply(self, func, axis=0, *args, **kwargs):
        
        results = {}
        def process_column(col):
            results[col] = Series([func(value, *args, **kwargs) for value in self[col]])

        def process_row(idx):
            results[idx] = Series([func(*self.loc[idx].values, *args, **kwargs)])

        threads:list[Thread] = []
        
        if axis == 0:
            for col in self.columns:
                thread = Thread(target=process_column, args=(col,))
                threads.append(thread)
                thread.start()
        
        elif axis == 1:
            for idx in self.index:
                thread = Thread(target=process_row, args=(idx,))
                threads.append(thread)
                thread.start()
        
        for thread in threads:
            thread.join()

        if axis == 0:
            return Database(results)
        else:
            return Database(results).transpose()
