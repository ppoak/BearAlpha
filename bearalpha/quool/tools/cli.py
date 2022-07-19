from .core import Cache
from .io import CONSOLE


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
    