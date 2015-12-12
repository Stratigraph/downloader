import requests
import subprocess
import glob
import os

os.chdir("./data")

GDELT_DATA_ENDPOINT = 'http://data.gdeltproject.org/events/'

response = requests.get(GDELT_DATA_ENDPOINT + 'filesizes')
gdelt_files = []

for line in str(response.text).splitlines():
    gdelt_files.append(line.split(" "))

gdelt_files_downloaded = glob.glob("./*")

for file in gdelt_files:
    filename = file[1]
    if filename == 'GDELT.MASTERREDUCEDV2.1979-2013.zip': continue

    print("start downloading: {}, with size of {} MB".format(filename, int(int(file[0]) / 1000 ** 2)))

    if "./" + filename not in gdelt_files_downloaded:
        subprocess.check_output("wget %s" % GDELT_DATA_ENDPOINT + filename, shell=True)

    subprocess.check_output("unzip %s" % filename, shell=True)
    subprocess.check_output("rm %s" % filename, shell=True)