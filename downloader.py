import requests
import subprocess
import glob
import os
import argparse

# define args
parser = argparse.ArgumentParser(description='Download GDELT projects data and filter them.')
parser.add_argument('--geo', type=bool, default=False)
parser.add_argument('--url', type=bool, default=False, help='include url to geo filtering')
args = parser.parse_args()

os.chdir("./data")
subprocess.call("echo '' > result.csv", shell=True)  # erase old result file
GDELT_DATA_ENDPOINT = 'http://data.gdeltproject.org/events/'  # page with gdelt data index
GDELT_GEO_FIELDNAMES = [0, 1, 7, 17, 30, 31, 32, 33, 34, 53, 54]  # id's of columns for geo export
gdelt_files_downloaded = glob.glob("./*")
gdelt_files = []

# download and process
for line in str(requests.get(GDELT_DATA_ENDPOINT + 'filesizes').text).splitlines():
    if line[1] == 'GDELT.MASTERREDUCEDV2.1979-2013.zip': continue
    gdelt_files.append(line.split(" "))

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
        with open("result.csv", "a") as result_file:
            with open(filename) as f:
                required_len = len(GDELT_GEO_FIELDNAMES)

                for line in f:
                    line = line.split("\t")
                    result_line = [line[key] for key in GDELT_GEO_FIELDNAMES if len(line[key]) > 0]

                    if len(result_line) == required_len:
                        if args.url and 57 in line:
                            result_line.append(line[57])

                        result_file.write(",".join(result_line) + "\n")

    else:
        subprocess.call("cat %s >> result.csv" % filename, shell=True)

    subprocess.call("rm %s" % filename, shell=True)
