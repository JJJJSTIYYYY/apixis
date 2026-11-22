"""Isolate the no-listener query path in a disposable benchmark process."""
import argparse
import gc
import json
from pathlib import Path
import statistics
import sys
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--queries', type=int, default=1000000)
    parser.add_argument('--repeats', type=int, default=5)
    args = parser.parse_args()
    if min(args.queries, args.repeats) <= 0:
        parser.error('queries and repeats must be positive')
    sys.path.insert(0, str(args.source.resolve()))
    import apixis
    from apixis import ApixEventHandler, ApixEventRegistry, ApixHandlerRegistry

    assert Path(apixis.__file__).resolve().is_relative_to(args.source.resolve())

    async def noop(event):
        pass

    registry = ApixHandlerRegistry(ApixEventRegistry())
    handler = ApixEventHandler(noop, name='one')
    handler.subscribe = ['exact']
    handler.priority = 1
    registry.register_handler(handler)
    assert not registry._waiter_cache.positions
    original = ApixHandlerRegistry.get_handlers_for_event

    def ordinary_only(self, event_name):
        return [self.registry[name] for name in self.get_subscription_chain_for_event(event_name)]

    rows = []
    try:
        for label in ('current', 'ordinary-only', 'ordinary-only', 'current'):
            ApixHandlerRegistry.get_handlers_for_event = original if label == 'current' else ordinary_only
            for _ in range(10000):
                assert registry.get_handlers_for_event('exact') == [handler]
            samples = []
            for _ in range(args.repeats):
                gc.collect()
                started = time.perf_counter()
                for _ in range(args.queries):
                    assert len(registry.get_handlers_for_event('exact')) == 1
                samples.append(time.perf_counter() - started)
            rows.append({'label': label, 'seconds': samples})
    finally:
        ApixHandlerRegistry.get_handlers_for_event = original
    medians = {label: statistics.median(value for row in rows if row['label'] == label for value in row['seconds'])
               for label in ('current', 'ordinary-only')}
    result = {'queries': args.queries, 'rows': rows, 'medians': medians,
              'extra_nanoseconds_per_query': (medians['current'] - medians['ordinary-only']) / args.queries * 1e9,
              'note': 'Ordinary-only bypass is valid only for this verified no-listener probe and is never written to runtime source.'}
    args.output.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
