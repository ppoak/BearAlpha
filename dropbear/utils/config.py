from utils import *
from pandas import Series
from factor.define import *

FACTORS = {
    "return_1m": {
        "table": "technical",
        "dataloader": return_1m,
        },
    "return_3m": {
        "table": "technical",
        "dataloader": return_3m,
        },
    "return_12m": {
        "table": "technical",
        "dataloader": return_12m,
        },
    "turnover_1m": {
        "table": "technical",
        "dataloader": turnover_1m,
        },
    "turnover_3m": {
        "table": "technical",
        "dataloader": turnover_3m,
        },
    "volatility_1m": {
        "table": "technical",
        "dataloader": volatility_1m,
        },
    "volatility_3m": {
        "table": "technical",
        "dataloader": volatility_3m,
        },
    "volatility_12m": {
        "table": "technical",
        "dataloader": volatility_12m},
    "ar": {
        "table": "technical",
        "dataloader": ar,
        },
    "br": {
        "table": "technical",
        "dataloader": br,
        },
    "bias_1m": {
        "table": "technical",
        "dataloader": bias_1m,
        },
    "davol_1m": {
        "table": "technical",
        "dataloader": davol_1m,
        },
}
