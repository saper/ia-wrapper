"""Concurrently download metadata for items on Archive.org.

usage:
    ia mine [--cache | --output=<output.json>] [--workers=<count>] <itemlist.txt>
    ia mine --help

options:
    -h, --help
    -c, --cache                 Write item metadata to a file called <identifier>_meta.json
    -o, --output=<output.json>  Write all metadata to a single output file <itemlist>.json
    -w, --workers=<count>       The number of requests to run concurrently [default: 20]

"""
import sys
import json
from functools import partial

from docopt import docopt
from clint.textui import progress

from internetarchive import get_data_miner


# cache_metadata()
# ________________________________________________________________________________________
def cache_metadata(item, output=None):
    if output:
        with open(output, 'a+') as fp:
            json.dump(item._json, fp)
            fp.write('\n')
    else:
        with open('{0}_meta.json'.format(item.identifier), 'w') as fp:
            json.dump(item._json, fp)

# ia_mine()
# ________________________________________________________________________________________
def main(argv):
    args = docopt(__doc__, argv=argv)

    if args['<itemlist.txt>'] == '-':
        itemfile = sys.stdin
    else:
        itemfile = open(args['<itemlist.txt>'])
    with itemfile:
        identifiers = [i.strip() for i in itemfile]

    workers = int(args.get('--workers', 20)[0])

    callback_kwargs = None
    if args['--cache']:
        callback_func = cache_metadata
    elif args['--output']:
        callback_func = cache_metadata
        callback_kwargs = dict(output=args['--output'])
    else:
        callback_func = None

    miner = get_data_miner(identifiers, callback=callback_func,
                           callback_kwargs=callback_kwargs, max_workers=workers)
    miner.run()
    sys.exit(0)
