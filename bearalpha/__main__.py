import re
import os
import importlib
import argparse
from .oxygene import *
from .tools import *


def set(args):
    from diskcache import Cache
    cache = Cache(directory=os.path.join(os.path.split(os.path.abspath(__file__))[0], 'cache'))
    cache.set(key=args.key, value=args.value, expire=args.expire)

def delete(args):
    from diskcache import Cache
    cache = Cache(directory=os.path.join(os.path.split(os.path.abspath(__file__))[0], 'cache'))
    for k in cache.iterkeys():
        if re.match(args.key, k):
            cache.delete(key=k)

def show(args):
    from diskcache import Cache
    cache = Cache(directory=os.path.join(os.path.split(os.path.abspath(__file__))[0], 'cache'))
    if args.key is not None:
        for akey in cache.iterkeys():
            if re.match(args.key, akey):
                Console().rule(f'{akey}')
                if args.value:
                    Console().print(f'{cache.get(akey)}')

    elif args.value:
        for akey in cache.iterkeys():
            Console().rule(f'{akey}')
            Console().print(f'{cache.get(akey)}')
    
    else:
        Console().print(f'{list(cache.iterkeys())}')

def clear(args):
    from diskcache import Cache
    cache = Cache(directory=os.path.join(os.path.split(os.path.abspath(__file__))[0], 'cache'))
    cache.expire()
    
def store(args):
    path = args.path
    mod = importlib.import_module(path)
    for name, config in mod.__dict__.items():
        if ('baconfig' in name) or ('Loader' in name)\
            or ('loader' in name and name != '__loader__'):
                from diskcache import Cache
                cache = Cache(directory=os.path.join(os.path.split(os.path.abspath(__file__))[0], 'cache'))
                cache.set(name, config)

def load(args):
    from diskcache import Cache
    cache = Cache(directory=os.path.join(os.path.split(os.path.abspath(__file__))[0], 'cache'))
    config = cache.get(args.config)
    config['loader'](config)()

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

if __name__ == "__main__":
    main()