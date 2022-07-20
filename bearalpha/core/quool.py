import pandas as pd


class FrameWorkError(Exception):
    def __init__(self, func: str, hint: str) -> None:
        self.func = func
        self.hint = hint
    
    def __str__(self) -> str:
        return f'[-] <{self.func}> {self.hint}'

class Worker(object):
    TS = 1
    CS = 2
    PN = 3
    
    def __init__(self, data: 'pd.DataFrame | pd.Series'):
        self.data = data
        self._validate()
    
    def series2frame(self, data: pd.Series = None, name: str = None):
        if data is None:
            self.data.to_frame(name=name or 'frame')
        else:
            return data.to_frame(name=name or 'frame')
    
    def frame2series(self, data: pd.DataFrame = None, name: str = None):
        if data is None:
            self.data = self.data.iloc[:, 0]
        else:
            series = data.iloc[:, 0]
            series.name = name or data.columns[0]
            return series
    
    def ists(self, data: 'pd.DataFrame | pd.Series' = None):
        if data is None:
            data = self.data
        return not isinstance(data.index, pd.MultiIndex) and isinstance(data.index, pd.DatetimeIndex)
    
    def iscs(self, data: 'pd.DataFrame | pd.Series' = None):
        if data is None:
            data = self.data
        return not isinstance(data.index, pd.MultiIndex) and not isinstance(data.index, pd.DatetimeIndex)
    
    def ispanel(self, data: 'pd.DataFrame | pd.Series' = None):
        if data is None:
            data = self.data
        return isinstance(data.index, pd.MultiIndex) and len(data.index.levshape) >= 2 \
                and isinstance(data.index.levels[0], pd.DatetimeIndex)
    
    def isframe(self, data: 'pd.DataFrame | pd.Series' = None):
        if data is None:
            data = self.data
        return True if isinstance(data, pd.DataFrame) else False
    
    def isseries(self, data: 'pd.DataFrame | pd.Series' = None):
        if data is None:
            data = self.data
        return True if isinstance(data, pd.Series) else False

    def _validate(self):

        self.is_frame = self.isframe()
        self.is_series = self.isseries()
        
        if self.is_frame and self.data.columns.size == 1:
            self.is_frame = False
            self.frame2series()
            
        if self.data.empty:
            raise ValueError('[!] Dataframe or Series is empty')

        is_ts = self.ists(self.data)
        is_cs = self.iscs(self.data)
        is_panel = self.ispanel(self.data)
        
        if is_ts:
            self.type_ = Worker.TS
        elif is_cs:
            self.type_ = Worker.CS
        elif is_panel:
            self.type_ = Worker.PN
        else:
            raise ValueError("Your dataframe or series seems not supported in our framework")
 
    def _flat(self, datetime, asset, indicator):
        
        data = self.data.copy()
        
        if self.type_ == Worker.PN:
            check = (not isinstance(datetime, slice), 
                     not isinstance(asset, slice), 
                     not isinstance(indicator, slice))

            # is a panel and is a dataframe
            if check == (False, False, False) and self.is_frame:
                raise ValueError('Must assign at least one of dimension')
            elif check == (False, True, True) and self.is_frame:
                return data.loc[(datetime, asset), indicator].droplevel(1)
            elif check == (True, False, True) and self.is_frame:
                return data.loc[(datetime, asset), indicator].droplevel(0)
            elif check == (True, True, False) and self.is_frame:
                return data.loc[(datetime, asset), indicator]
            elif check == (True, False, False) and self.is_frame:
                return data.loc[(datetime, asset), indicator].droplevel(0)
            elif check == (False, True, False) and self.is_frame:
                return data.loc[(datetime, asset), indicator].droplevel(1)
            elif check == (False, False, True) and self.is_frame:
                return data.loc[(datetime, asset), indicator].unstack(level=1)
            elif check == (True, True, True) and self.is_frame:
                print('[!] single value was selected')
                return data.loc[(datetime, asset), indicator]
                
            # is a panel and is a series
            elif (check[-1] or not any(check)) and not self.is_frame:
                if check[-1]:
                    print("[!] Your data is not a dataframe, indicator will be ignored")
                return data.unstack()
            elif check[1] and not self.is_frame:
                return data.loc[(datetime, asset)].unstack()
            elif check[0] and not self.is_frame:
                return data.loc[(datetime, asset)]
                
        else:
            # not a panel and is a series
            if not self.is_frame:
                if self.type_ == Worker.TS:
                    return data.loc[datetime]
                elif self.type_ == Worker.CS:
                    return data.loc[asset]
            # not a panel and is a dataframe
            else:
                if self.type_ == Worker.TS:
                    return data.loc[(datetime, indicator)]
                elif self.type_ == Worker.CS:
                    return data.loc[(asset, indicator)]



if __name__ == "__main__":
    pass
