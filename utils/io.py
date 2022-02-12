import re
import sys
import rich
import time
import smtplib
import pymysql
import datetime
import pandas as pd
from typing import Union
from email.mime.text import MIMEText
from rich.traceback import install
from rich.console import Console
from rich.progress import (track, Progress, SpinnerColumn,
                           TimeRemainingColumn, BarColumn)
import sqlalchemy

install(suppress=[rich, pymysql, sqlalchemy, pd], show_locals=False)
console = Console()
progress = Progress(
    SpinnerColumn(spinner_name='monkey'),
    "[progress.description]{task.description}",
    BarColumn(),
    "[progress.percentage]{task.percentage:>3.0f}%",
    "{task.completed} of {task.total}",
    TimeRemainingColumn()
)


def time2str(date: Union[str, datetime.date, datetime.datetime]) -> str:
    if isinstance(date, (datetime.datetime, datetime.date)):
        date = date.strftime(r'%Y-%m-%d')
    return date

def str2time(date: Union[str, datetime.date, datetime.datetime]) -> datetime.datetime:
    if isinstance(date, (str, datetime.date)):
        date = pd.to_datetime(date)
    return date
