import pandas as pd

from .artist import (
    Drawer,
    Printer,
)

from .analyst import (
    Regressor,
    Describer,
    Decompositer,
    SigTester,
)

from .fetcher import (
    Filer,
    Sqliter,
    Mysqler,
)

from .calculator import (
    Calculator
)
    
from .processor import (
    PreProcessor,
    Converter,
)

from .backtester import (
    Relocator,
    BackTrader,
    Factester,
)

from .evaluator import (
    Evaluator,
)

class DataFrame(pd.DataFrame):
    drawer: Drawer
    printer: Printer
    regressor: Regressor
    describer: Describer
    decompositer: Decompositer
    tester: SigTester
    filer: Filer
    sqliter: Sqliter
    mysqler: Mysqler
    calculator: Calculator
    converter: Converter
    preprocessor: PreProcessor
    backtrader: BackTrader
    relocator: Relocator
    factester: Factester
    evaluator: Evaluator


class Series(pd.Series):
    drawer: Drawer
    printer: Printer
    regressor: Regressor
    describer: Describer
    decompositer: Decompositer
    tester: SigTester
    filer: Filer
    sqliter: Sqliter
    mysqler: Mysqler
    calculator: Calculator
    converter: Converter
    preprocessor: PreProcessor
    backtrader: BackTrader
    relocator: Relocator
    factester: Factester
    evaluator: Evaluator

del pd
