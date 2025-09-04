"""
This script is meant to be a program that transforms a big quantity of files
with a specific format into json files that contain the same data but
in a different format.

The original files are txt files in bz2 compression, and the resulting files
are json files.

I created this script while working at ALMA Observatory and I wanted to back
it up in my own files because is a big accomplishment (I think)

HOW TO RUN

python -u month_to_json.py ${basepath} ${lts_data} ${year} ${month} > ${log_file_name} 2>&1

basepath = String that is a path to where the TXT files are located.
lts_data = String that is a path to where the json files will be written.
year = a string of the format YYYY that corresponds to the year to be processed,
       the year is inside basepath as  ${basepath}/${year}
month = a string of the format mm that corresponds to the month to be processed,
        the month is inside basepath/year as  ${basepath}/${year}/${month}
log_file_name = Path to the file where the prints of this script will be written.
                The logs of the processes that are run with multiprocessing are
                located in a different path that these prints let you know where it is.

IMPORTANT NOTE:

This script can be run for several months by another script I have in my
other repository about learning bash-linux. The link is here:
https://github.com/rosita-hormann/learning-bash-linux/blob/master/bash-scripting/run_python_script_repetidely.sh
"""

import os
from pathlib import Path

import sys

from collections import Counter
from multiprocessing import Manager, Pool, Lock
from functools import partial

import bz2
import json

import logging

from datetime import datetime
import time

import math



# Create the logs lock as a global variable
log_lock = Lock()

def setup_logging(log_file_name):
    logging.basicConfig(
        filename=log_file_name,
        level=logging.INFO,
        format="%(asctime)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def list_txt_files(directory):
    print('I am in list_txt_files')
    return [str(file) for file in Path(directory).rglob("*.txt.bz2")]

def save_json(data, file_path):

    # Convert to Path object
    file_path = Path(file_path)
    
    # Create parent directories if they don't exist
    file_path.parent.mkdir(parents=True, exist_ok=True)

    with open(file_path, 'w') as file:
        json.dump(data, file)


def process_day(day_path):
    print('I am in process_day')
    shared_list = []

    list_mpoints_temp = list_txt_files(day_path)
    #print('Finished list_txt_files. list_mpoints_temp', list_mpoints_temp)
    for mpoint_dir in list_mpoints_temp:

        # Extract the file name without the extension
        file_name = os.path.splitext(os.path.basename(mpoint_dir))[0]

        # Extract the parent directory name
        parent_dir = os.path.basename(os.path.dirname(mpoint_dir))

        first_half = parent_dir.replace('_', '/')
        second_half = file_name[0:-4]

        # Combine the parent directory and file name to obtain mpoint_name
        mpoint_name = f"{first_half}:{second_half}"

        shared_list.append(mpoint_name)
    return shared_list



def process_mpoint(mpoint, list_day_paths, lts_path, month_path):
    # list_day_paths = [os.path.join(month_path, d) for d in os.listdir(month_path) if os.path.isdir(os.path.join(month_path, d))]
    #logging.info("mpoint: " + mpoint)
     
    main_path, variable = mpoint.split(":")
    main_path = main_path.replace("/", "_")
    mpoint_base_filename = main_path + '/' + variable + '.txt.bz2'

    json_data = {
        'name': mpoint,
        'values': []
    }

    data_type = ''

    for path_day in list_day_paths:
        file_path = path_day + '/' + mpoint_base_filename
        if os.path.exists(file_path):
            
            with bz2.open(file_path, mode='r') as file:
                i = 0
                try:
                    for line in file:
                        decoded_line = line.decode("utf-8")
                        #date_str,data = decoded_line.split(' ', 1)
                        parts = decoded_line.split(' ', 1)
                        date_str = parts[0]
                        data = parts[1] if len(parts) > 1 else ''
                        if data == '':
                            with log_lock:
                                logging.error('Bad line. File: ' + file_path + " , line: " +  decoded_line)
                            #print('Bad line:', decoded_line)
                            continue
                        
                        #dt = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S.%f")
                        try:
                            dt = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S.%f")
                        except ValueError as e:
                            with log_lock:
                                logging.error('Bad date: ' + date_str + ' , file: ' + file_path)
                                continue
                        timestamp_ms = int(dt.timestamp() * 1000)

                        data_split = data.split(' ')
                        
                        try:
                            if len(data_split) == 1:
                                data_type = 'number'
                                data = float(data)
                                if math.isnan(data) or math.isinf(data):
                                    data = str(data)
                            else:
                                data_type = 'array'
                                new_data = []
                                for d in data_split:
                                    new_d = float(d)
                                    if math.isnan(new_d) or math.isinf(new_d):
                                        new_d = 0
                                    new_data.append(new_d)
                                    
                                data = new_data
                        except ValueError as e:
                            logging.error(f'Error parsing data as float/array: {repr(decoded_line)}, file: {file_path}')
                            continue

                        i+=1
                        monitoring_point = [timestamp_ms, data]
                        json_data['values'].append(monitoring_point)
                        # Open file an process it
                    

                except EOFError:
                    # Log to file using the global lock
                    with log_lock:
                        logging.error('Error reading EOF: ' + file_path)
        
        # List txt.bz2 files inside

    json_data['data_type'] = data_type
    

    # Write Json File
    my_month_path = month_path[-8:]
    json_filename = lts_path + my_month_path + main_path + '/' + variable + '.json'
    save_json(data=json_data, file_path=json_filename)

    # Log to file using the global lock
    with log_lock:
        logging.info("Written: " +  mpoint) # LOGS

def process_directory(day_dir):
    print(f"process_directory: {day_dir}")

def main(basepath, lts_path, year, month, n_processes):

    my_month_path = Path(basepath + year + '/' + month + '/')


    print("n_processes", n_processes)
    
    
    mpoints_names = set()

    # Measuring time, temporarly
    start = time.time()
    for day in my_month_path.iterdir():
        print('day', day)
        mpoints_names.update(process_day(day))
        print('dayyyyyyyyyyyyy')
    end = time.time()
    print('FOR my_month_path.iterdir(): time elapsed:', end - start, ' seconds')
    print('-----------------------------------------------')

    #"""
    month_path_string = basepath + year + '/' + month + '/'
    list_day_paths = [os.path.join(month_path_string, d) for d in os.listdir(month_path_string) if os.path.isdir(os.path.join(month_path_string, d))]

    with Pool(processes=n_processes) as pool:
        print("I am in a Pool")
        process_func = partial(process_mpoint, list_day_paths=list_day_paths, lts_path=lts_path, month_path=month_path_string)

        results = [pool.apply_async(process_func, (mpoint,)) for mpoint in mpoints_names]

        # Wait for all processes to complete
        [result.get() for result in results]
        with log_lock:
            logging.info('- - - - - - - - - - - - - - - - -')
            logging.info('Finished!')
    #"""
    print('finish main')
        

    


if __name__ == "__main__":
    
    basepath = sys.argv[1]
    lts_path = sys.argv[2]
    year = sys.argv[3]
    month = sys.argv[4]
    n_processes = 20

    print('basepath', basepath)
    print('lts_path', lts_path)
    print('year', year)
    print('month', month)

    month_path_string = year + '/' + month
    log_file_name = 'month_to_json_' + month_path_string.replace('/', '_') + '.log'
    log_file_name = 'logs/to_coldstorage/' + log_file_name

    setup_logging(log_file_name)

    print('Logging setted up')

    print('Starting')

    start = time.time()

    main(basepath, lts_path, year, month, n_processes)
    
    print('Ending')
    end = time.time()

    logging.info('- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - ')
    logging.info('MAIN finished. Time elapsed ' + str(end - start) + ' seconds')
