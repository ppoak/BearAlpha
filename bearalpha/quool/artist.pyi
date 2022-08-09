from bearalpha import *


class Drawer(quool.base.Worker):

    def draw(
        self, 
        kind: str, 
        datetime: str = slice(None), 
        asset: str = slice(None), 
        indicator: str = slice(None), 
        **kwargs
    ) -> None: 
        """Draw a image of the given slice of data
        ------------------------------------------

        kind: str, the kind of the plot
        datetime: str, the slice of datetime, default to all time period
        asset: str, the slice of asset, default to all assets
        indicator: str, the slice of indicator, default to all indicators
        kwargs: dict, the kwargs for the plot function
        """


class Printer(quool.base.Worker):

    def display(
        self, 
        datetime: str = slice(None), 
        asset: str = slice(None),
        indicator: str = slice(None), 
        maxdisplay_length: int = 20, 
        maxdisplay_width: int = 12, 
        title: str = "Table"
    ) -> None:
        """Print the dataframe or series in a terminal
        ------------------------------------------

        datetime: str, the slice on the datetime dimension
        asset: str, the slice on the asset dimension
        indicator: str, the slice on the indicator dimension
        maxdisplay_length: int, the maximum display length of the table
        maxdisplay_width: int, the maximum display width of the table
        title: str, the title of the table
        """