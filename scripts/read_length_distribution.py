#! /usr/bin/env python3
'''
Compute the mean and standard deviation of read alignment lengths in a SAM/BAM
'''

# useful constants
DEFAULT_BLOCK_SIZE = 100000

# imports
from os import cpu_count
from os.path import isfile
from time import time
from warnings import catch_warnings, simplefilter
import argparse
import pysam
with catch_warnings():
    simplefilter("ignore")
    import ray

# parse user args
def parse_args():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-i', '--input', required=True, type=str, help="Input SAM/BAM")
    parser.add_argument('-t', '--threads', required=False, type=int, default=cpu_count(), help="Number of Threads")
    parser.add_argument('-b', '--block_size', required=False, type=int, default=DEFAULT_BLOCK_SIZE, help="Block Size (i.e., number of reads per block)")
    parser.add_argument('-r', '--reps', required=False, type=int, default=1, help="Number of Reps")
    args = parser.parse_args()
    assert 1 <= args.threads <= cpu_count(), "Invalid number of threads: %d (must be between 1 and %d)" % (args.threads, cpu_count())
    assert args.block_size > 0, "Invalid block size: %d (must be positive)" % args.block_size
    assert args.reps > 0, "Invalid reps: %d (must be positive)" % args.reps
    return args

# open SAM/BAM file
def open_sam(fn):
    tmp = pysam.set_verbosity(0) # disable htslib verbosity to avoid "no index file" warning
    if fn.lower() == 'stdin':
        aln = pysam.AlignmentFile('-', 'r') # standard input --> SAM
    elif not isfile(fn):
        assert False, "File not found: %s" % fn
    elif fn.lower().endswith('.sam'):
        aln = pysam.AlignmentFile(fn, 'r')
    elif fn.lower().endswith('.bam'):
        aln = pysam.AlignmentFile(fn, 'rb')
    else:
        assert False, "Invalid input alignment file extension: %s" % fn
    pysam.set_verbosity(tmp) # re-enable htslib verbosity and finish up
    return aln

# compute stats single-threaded
def compute_stats(lengths):
    s = 0; ss = 0
    for curr_len in lengths:
        s += curr_len
        ss += (curr_len * curr_len)
    n = len(lengths)
    avg = s / n
    std = (ss / n) - (avg * avg)
    return (avg, std)

# worker function
@ray.remote
def worker(lengths):
    s = 0; ss = 0; n = 0
    for curr_len in lengths:
        if curr_len is None:
            break
        s += curr_len
        ss += (curr_len * curr_len)
    return (s, ss)

# compute stats in parallel
def compute_stats_parallel(lengths, threads, block_size):
    futures = list(); block = [None]*block_size
    for i, length in enumerate(lengths):
        ind = i % block_size
        if ind == 0 and block[0] is not None:
            futures.append(worker.remote(block))
            block = [None]*block_size
        block[ind] = length
    futures.append(worker.remote(block))
    results = ray.get(futures)
    n = len(lengths)
    s = sum(curr_s for curr_s, curr_ss in results)
    ss = sum(curr_ss for curr_s, curr_ss in results)
    avg = s / n
    std = (ss / n) - (avg * avg)
    return (avg, std)

# main content
if __name__ == "__main__":
    args = parse_args()
    sam = open_sam(args.input)
    lengths = [read.query_alignment_length for read in sam if not read.is_unmapped]*args.reps
    if args.threads == 1:
        start_time = time()
        avg, std = compute_stats(lengths)
        end_time = time()
    else:
        ray.init(num_cpus=args.threads)
        start_time = time()
        avg, std = compute_stats_parallel(lengths, args.threads, args.block_size)
        end_time = time()
    print("Number of Mapped Reads: %s\nAverage: %s\nStandard Deviation: %s\nRuntime (s): %s" % (len(lengths), avg, std, end_time-start_time))
