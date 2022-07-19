import argparse
from .tools.cli import set, delete, show, clear


def main():
    parser = argparse.ArgumentParser(description='Quool CLI API')
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

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
