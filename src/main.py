import argparse
import importlib
import os
import os.path as path
import sys

import pyspark

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-j', '--job', type=str, required=True)
    parser.add_argument('-a', '--job-args', nargs='*')
    parser.add_argument('-n', '--job-name', type=str, required=True)
    args = parser.parse_args()
    
    sc = pyspark.SparkContext(appName=args.job_name)
    job_module = importlib.import_module('jobs.{}'.format(args.job))
    job_module.run(sc, args.job_args)

if __name__ == '__main__':
    main()
