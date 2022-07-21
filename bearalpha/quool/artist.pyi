from bearalpha import *


class Drawer(Worker):

    def draw(
        self, 
        kind: str, 
        datetime: str = slice(None), 
        asset: str = slice(None), 
        indicator: str = slice(None), 
        **kwargs
    ) -> None: ...


class Printer(Worker):

    def display(
        self, 
        datetime: str = slice(None), 
        asset: str = slice(None),
        indicator: str = slice(None), 
        maxdisplay_length: int = 20, 
        maxdisplay_width: int = 12, 
        title: str = "Table"
    ) -> None: ...