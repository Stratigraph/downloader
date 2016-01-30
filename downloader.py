import requests
import subprocess
import glob
import os
import argparse
import json
import time
import multiprocessing
import random
import string


def get_id(N=30):
    return ''.join(random.SystemRandom().choice(string.ascii_uppercase + string.digits) for _ in range(N))


# define args
parser = argparse.ArgumentParser(description='Download GDELT projects data and filter them.')
parser.add_argument('--geo', type=bool, default=True)
parser.add_argument('--url', type=bool, default=True, help='include url to geo filtering')
args = parser.parse_args()

os.chdir("./data")
# subprocess.call("cat /dev/null > result.csv", shell=True)  # erase old result file
GDELT_DATA_ENDPOINT = 'http://data.gdeltproject.org/events/'  # page with gdelt data index
GDELT_GEO_FIELDNAMES = [0, 1, 7, 17, 30, 31, 32, 33, 34, 53, 54, 57]  # id's of columns for geo export
GDELT_HEADER = list(range(0, 58))  # id's of columns for geo export
gdelt_files = []

# download and process
for line in str(requests.get(GDELT_DATA_ENDPOINT + 'filesizes').text).splitlines():
    line = line.split(" ")
    if line[1] == 'GDELT.MASTERREDUCEDV2.1979-2013.zip': continue
    gdelt_files.append(line)

gdelt_files.reverse()


def process_file(file):
    global GDELT_GEO_FIELDNAMES, GDELT_GEO_FIELDNAMES, GDELT_HEADER

    process_id = get_id()

    os.mkdir(process_id)
    os.chdir(process_id)
    filename = file[1]
    print("start downloading: {}, with size of {} MB".format(filename, int(int(file[0]) / 1000 ** 2)))

    subprocess.call("wget %s" % GDELT_DATA_ENDPOINT + filename, shell=True, stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL)

    csv_before = glob.glob("*.csv") + glob.glob("*.CSV")
    subprocess.call("unzip %s" % filename, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    subprocess.call("rm %s" % filename, shell=True)

    filename = [x for x in list(glob.glob("*.csv") + glob.glob("*.CSV")) if x not in csv_before][0]

    if args.geo:
        GDELT_HEADER = GDELT_GEO_FIELDNAMES

    with open("../subresults/res_" + str(file[1][:-4]).lower(), "a") as result_file:
        with open(filename) as f:
            for line in f:
                line = line.replace("\n", '').split("\t")

                result_line = []
                for key in GDELT_HEADER:
                    if key < len(line):
                        result_line.append(line[key].lstrip().rstrip())
                    else:
                        result_line.append("")

                result_file.write(";".join(result_line) + "\n")

    subprocess.call("rm %s" % filename, shell=True)
    os.chdir("../")
    os.rmdir(process_id)

p = multiprocessing.Pool(1)
p.map(process_file, gdelt_files)