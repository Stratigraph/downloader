import requests
import subprocess
import glob
import os
import argparse
import json

# define args
parser = argparse.ArgumentParser(description='Download GDELT projects data and filter them.')
parser.add_argument('--geo', type=bool, default=False)
parser.add_argument('--url', type=bool, default=False, help='include url to geo filtering')
args = parser.parse_args()

os.chdir("./data")
subprocess.call("cat /dev/null > result.csv", shell=True)  # erase old result file
GDELT_DATA_ENDPOINT = 'http://data.gdeltproject.org/events/'  # page with gdelt data index
GDELT_GEO_FIELDNAMES = [0, 1, 7, 17, 30, 31, 32, 33, 34, 53, 54]  # id's of columns for geo export
GDELT_HEADER = list(range(0, 58))  # id's of columns for geo export
gdelt_files_downloaded = glob.glob("./*")
gdelt_files = []

# download and process
for line in str(requests.get(GDELT_DATA_ENDPOINT + 'filesizes').text).splitlines():
    line = line.split(" ")
    if line[1] == 'GDELT.MASTERREDUCEDV2.1979-2013.zip': continue
    gdelt_files.append(line)

for file in gdelt_files:
    filename = file[1]

    print("start downloading: {}, with size of {} MB".format(filename, int(int(file[0]) / 1000 ** 2)))

    if "./" + filename not in gdelt_files_downloaded:
        subprocess.call("wget %s" % GDELT_DATA_ENDPOINT + filename, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    csv_before = glob.glob("*.csv") + glob.glob("*.CSV")
    subprocess.call("unzip %s" % filename, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    subprocess.call("rm %s" % filename, shell=True)

    filename = [x for x in list(glob.glob("*.csv") + glob.glob("*.CSV")) if x not in csv_before][0]

    if args.geo:
        GDELT_HEADER = GDELT_GEO_FIELDNAMES
        if args.url: GDELT_HEADER.append(57)

    with open("result.csv", "a") as result_file:
        result_file.write(json.dumps(GDELT_HEADER) + '\n')
        with open(filename) as f:
            required_len = len(GDELT_HEADER)

            for line in f:
                line = line.replace("\n", '').split("\t")
                result_line = [(line[key] if key < len(line) else '') for key in GDELT_HEADER]

                if len(result_line) == required_len:
                    result_file.write(",".join(result_line) + "\n")

    subprocess.call("rm %s" % filename, shell=True)
