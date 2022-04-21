#! /usr/bin/env python3
'''
Simple program that runs a function that just waits some amount of time.
'''

# imports
from os import cpu_count
from time import sleep, time
from warnings import catch_warnings, simplefilter
import argparse
with catch_warnings():
    simplefilter("ignore")
    import ray

# parse user args
def parse_args():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-n', '--number', required=True, type=int, help="Number of Function Calls")
    parser.add_argument('-d', '--duration', required=True, type=int, help="Duration (in seconds) per Function Call")
    parser.add_argument('-t', '--threads', required=False, type=int, default=cpu_count(), help="Number of Threads")
    args = parser.parse_args()
    assert args.number > 0, "Invalid number: %d (must be positive)" % args.number
    assert args.duration > 0, "Invalid duration: %d (must be positive)" % args.duration
    assert 1 <= args.threads <= cpu_count(), "Invalid number of threads: %d (must be between 1 and %d)" % (args.threads, cpu_count())
    return args

# run in serial
def run_serial(number, duration):
    total = 0
    for _ in range(number):
        sleep(duration)
        total += duration
    return total

# worker function
@ray.remote
def worker(duration):
    sleep(duration)
    return duration

# run in parallel
def run_parallel(number, duration):
    futures = list()
    for _ in range(number):
        futures.append(worker.remote(duration))
    results = ray.get(futures)
    return sum(results)

# main content
if __name__ == "__main__":
    args = parse_args()
    if args.threads == 1:
        start_time = time()
        total = run_serial(args.number, args.duration)
        end_time = time()
    else:
        ray.init(num_cpus=args.threads)
        start_time = time()
        total = run_parallel(args.number, args.duration)
        end_time = time()
    print("Total (s): %s\nRuntime (s): %s" % (total, end_time-start_time))
