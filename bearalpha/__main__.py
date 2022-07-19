import argparse
import sqlalchemy
from .core import *
from .tools import *
from .database import *

local_market_daily_config = dict(
    loader = Local,
    path = Cache().get('local_market_daily_path'),
    table = "market_daily",
    database = sqlalchemy.engine.create_engine(Cache().get('local')),
    addindex = {
        "idx_market_daily_date": "date",
        "idx_market_daily_order_book_id": "order_book_id",
    },
)

local_plate_info_config = dict(
    loader = Local,
    path = Cache().get('local_plate_info_path'),
    table = "plate_info",
    database = sqlalchemy.engine.create_engine(Cache().get('local')),
    addindex = {
        "idx_plate_info_date": "date",
        "idx_plate_info_order_book_id": "order_book_id",
    },
)

local_index_market_daily_config = dict(
    loader = Local,
    path = Cache().get('local_index_market_daily_path'),
    table = "index_market_daily",
    database = sqlalchemy.engine.create_engine(Cache().get('local')),
    addindex = {
        "idx_index_market_daily_date": "date",
        "idx_index_market_daily_order_book_id": "order_book_id",
    },
)

local_derivative_indicator_config = dict(
    loader = Local,
    path = Cache().get('local_derivative_indicator_path'),
    table = "derivative_indicator",
    database = Cache().get('local'),
    addindex = {
        "idx_derivative_indicator_date": "date",
        "idx_derivative_indicator_order_book_id": "order_book_id",
    },
)

local_instruments_config = dict(
    loader = Local,
    path = Cache().get('local_instruments_path'),
    table = "instruments",
    database = Cache().get('local'),
    addindex = {
        "idx_instruments_order_book_id": "order_book_id",
    },
)

local_index_weights_config = dict(
    loader = Local,
    path = Cache().get('local_index_weights_path'),
    table = "index_weights",
    database = Cache().get('local'),
    addindex = {
        "idx_index_weights_date": "date",
        "idx_index_weights_index_id": "index_id",
        "idx_index_weights_order_book_id": "order_book_id",
    },
)

configs = dict(
    local_market_daily_config = local_market_daily_config,
    local_index_market_daily_config = local_index_market_daily_config,
    local_derivative_indicator_config = local_derivative_indicator_config,
    local_index_weights_config = local_index_weights_config,
    local_instruments_config = local_instruments_config,
    local_plate_info_config = local_plate_info_config,
)

def set(args):
    cache = Cache()
    cache.set(key=args.key, value=args.value, expire=args.expire)

def delete(args):
    cache = Cache()
    cache.delete(key=args.key)

def show(args):
    cache = Cache()
    keys = list(cache.iterkeys())
    if args.key is not None:
        if args.value:
            CONSOLE.print(f'[red]When assigned key, -v will be ignored')
        if not args.precise:
            available_keys = filter(lambda x: args.key in x, keys)
            for akey in available_keys:
                CONSOLE.rule(f'{akey}')
                CONSOLE.print(f'{cache.get(akey)}')
        else:
            CONSOLE.rule(f'{args.key}')
            CONSOLE.print(f'{cache.get(args.key)}')
    
    elif args.value:
        for key in keys:
            CONSOLE.rule(f'{key}')
            CONSOLE.print(f'{cache.get(key)}')
    else:
        CONSOLE.print(f'{keys}')

def clear(args):
    cache = Cache()
    cache.expire()
    
def load(args):
    config = configs[args.config]
    loader = config['loader'](config)
    loader()


def main():
    parser = argparse.ArgumentParser(description='BearAlpha Cli API')
    subparser = parser.add_subparsers()

    setter = subparser.add_parser('set', help='Set cache via quool Cli')
    setter.add_argument('-k', '--key', required=True, type=str, help='Key to the cache')
    setter.add_argument('-v', '--value', required=True, help='Value to the cache')
    setter.add_argument('-e', '--expire', default=None, type=int, help='Valid time, default to forever')
    setter.set_defaults(func=set)

    deleter = subparser.add_parser('delete', help='Delete cache via quool Cli')
    deleter.add_argument('-k', '--key', required=True, type=str, help='Key to the cache')
    deleter.set_defaults(func=delete)

    shower = subparser.add_parser('show', help='Show cahce keys')
    shower.add_argument('-k', '--key', default=None, help='Whether to show the value')
    shower.add_argument('-v', '--value', default=False, action='store_true', help='Whether to show the value')
    shower.add_argument('-p', '--precise', default=False, action='store_true', help='Precisely search for the key')
    shower.set_defaults(func=show)

    clearer = subparser.add_parser('clear', help='Clear expired keys')
    clearer.set_defaults(func=clear)

    loader = subparser.add_parser('load', help='Load data into local database according to configurations')
    loader.add_argument('-c', '--config', required=True, help='Configurations to use in loading data')
    loader.set_defaults(func=load)

    args = parser.parse_args()
    args.func(args)
