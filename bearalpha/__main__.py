import re
import importlib
import argparse
from .core import *
from .tools import *
from .database import *


def set(args):
    cache = Cache()
    cache.set(key=args.key, value=args.value, expire=args.expire)

def delete(args):
    cache = Cache()
    for k in cache.iterkeys():
        if re.match(args.key, k):
            cache.delete(key=k)

def show(args):
    cache = Cache()
    if args.key is not None:
        for akey in cache.iterkeys():
            if re.match(args.key, akey):
                CONSOLE.rule(f'{akey}')
                if args.value:
                    CONSOLE.print(f'{cache.get(akey)}')

    elif args.value:
        for akey in cache.iterkeys():
            CONSOLE.rule(f'{akey}')
            CONSOLE.print(f'{cache.get(akey)}')
    
    else:
        CONSOLE.print(f'{list(cache.iterkeys())}')

def clear(args):
    cache = Cache()
    cache.expire()
    
def store(args):
    path = args.path
    mod = importlib.import_module(path)
    for name, config in mod.__dict__.items():
        if 'baconfig' in name:
            Cache().set(name, config)

def load(args):
    config = Cache().get(args.config)
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
    shower.set_defaults(func=show)

    clearer = subparser.add_parser('clear', help='Clear expired keys')
    clearer.set_defaults(func=clear)

    storer = subparser.add_parser('store', help='Store config data into bearalpha cache')
    storer.add_argument('-p', '--path', required=True, help='Path to the configuration .py file')
    storer.set_defaults(func=store)
    
    loader = subparser.add_parser('load', help='Load data into local database according to configurations')
    loader.add_argument('-c', '--config', required=True, help='Configurations to use in loading data')
    loader.set_defaults(func=load)

    args = parser.parse_args()
    args.func(args)
