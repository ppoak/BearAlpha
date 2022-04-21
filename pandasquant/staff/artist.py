import matplotlib.pyplot as plt


class Drawer(object):
    '''A Parameter Class for Drawing'''
    def __init__(self, method: str = 'line', date: 'str | list | slice' = slice(None),
        asset: 'str | list | slice' = slice(None), name: str = None, 
        indicator: 'str | list | slice' = slice(None), title: str = 'Image', ax: plt.Axes = None, **kwargs):
        self.method = method
        self.date = date
        self.indexer = eval(str([(date, asset), (name, indicator)]).replace('None, ', ''))
        self.name = name
        self.ax = ax
        self.kwargs = kwargs
        self.title = title
        if not(self.is_ts or self.is_cs):
            raise TypeError('indexer is wrongly set, please check')
    
    @property
    def is_ts(self) -> bool:
        if isinstance(self.date, (slice, list)):
            return True
        else:
            return False
    
    @property
    def is_cs(self) -> bool:
        if isinstance(self.date, (str, tuple)):
            return True
        else:
            return False

    @property
    def unstack_level(self) -> str:
        if self.is_ts:
            return 'asset'
        else:
            return 'datetime'
        
class DataDrawer(Drawer):
    '''A Drawer for Data Class'''

    def __init__(self, method: str = 'line', date: 'str | list | slice' = slice(None), 
        asset: 'str | list | slice' = slice(None), indicator: 'str | list | slice' = slice(None), 
        title: str = 'Image', ax: plt.Axes = None, **kwargs):
        super().__init__(method, date, asset, None, indicator, title, ax, **kwargs)

class CollectionDrawer(Drawer):
    '''A Drawer for Collection Class'''

    def __init__(self, method: str = 'line', date: 'str | list | slice' = slice(None), 
        asset: 'str | list | slice' = slice(None), name: str = slice(None), 
        indicator: 'str | list | slice' = slice(None), 
        title: str = 'Image', ax: plt.Axes = None, **kwargs):
        super().__init__(method, date, asset, name, indicator, title, ax, **kwargs)

class PriceDataDrawer(DataDrawer):
    '''A Drawer for PriceData Class'''
    def __init__(self, method: str = 'line', date: 'str | list | slice' = slice(None), 
        asset: 'str | list | slice' = slice(None), indicator: 'str | list | slice' = None, title: str = 'Image', ax: plt.Axes = None, **kwargs):
        if isinstance(indicator, 'str'):
            indicator = [indicator, 'price']
        elif isinstance(indicator, list):
            indicator.append('price')
        super().__init__(method, date, asset, indicator, title, ax, **kwargs)

class PriceCollectionDrawer(CollectionDrawer):
    '''A Drawer for PriceCollection Class'''
    def __init__(self, method: str = 'line', date: 'str | list | slice' = slice(None), 
        asset: 'str | list | slice' = slice(None), name: str = None, 
        indicator: 'str | list | slice' = None, 
        title: str = 'Image', ax: plt.Axes = None, **kwargs):
        if isinstance(name, list):
            name += ['price']
        elif isinstance(name, str):
            name = [name, 'price']
        if isinstance(indicator, list):
            indicator += ['price']
        elif isinstance(indicator, str):
            indicator = [indicator, 'price']
        super().__init__(method, date, asset, name, indicator, title, ax, **kwargs)
