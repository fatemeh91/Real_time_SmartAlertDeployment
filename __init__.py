import datetime
import logging
import os
import requests
import pytz
import random
import threading
import traceback
import azure.functions as func  # type: ignore
import pdb
import sys
import subprocess

def source_env_vars(script_path):
    command = f'source {script_path} && env'
    proc = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True, executable='/bin/bash')
    for line in proc.stdout:
        (key, _, value) = line.decode('utf-8').partition('=')
        os.environ[key] = value.strip()

# Source the environment variables
source_env_vars('./env_vars.sh')

sys.path.append(os.path.abspath('/Users/fa/Documents/Lab_Prediction/lab-prediction-serve-main'))

from utils import utils, cosmos

LOCAL_DEV = True

def main(timerevent: func.TimerRequest) -> None:
    pacific_timezone = pytz.timezone("America/Los_Angeles")
    timestamp = datetime.datetime.now(pacific_timezone).strftime("%Y-%m-%dT%H:%M:%Z")

    if timerevent.past_due:
        logging.info("The timer is past due!")
    
    patient_list = utils.get_patients_in_unit(unit_ids=['2000233', '2000327', 
'2000252',
'2000263',
'2000262',
'2000258',
'2000261',
'2000250',
'2000273',
'2000241',
'2000251',
'2000253',
'2000224',
'2000231',
'2000237',
'2000211',
'120201004',
'120201003',
'120201002',
'120201001',
'110100003',
'110100004',
'110100005',
'110100007',
'110100008',
'110100009',
'110100011',
'110100012',
'110100013',
'110100015',
'110100016',
'110100017'
], unit_names=[
    "H1",
    "G1",
    "EGR",
    "E3",
    "E2-ICU",
    "E2-AAU",
    "E1",
    "DGR",
    "D3",
    "D2",
    "D1",
    "C3",
    "C2",
    "B3",
    "B2",
    "B1",
    "3 WEST PLEASANTON",
    "2 WEST PLEASANTON",
    "2 NORTH PLEASANTON",
    "1 WEST PLEASANTON",
    "J5",
    "J6",
    "J7",
    "K5",
    "K6",
    "K7",
    "L5",
    "L6",
    "L7",
    "M5",
    "M6",
    "M7"
])
    def process_patient_list(patient_id_list):
        for patient in patient_id_list:
            if LOCAL_DEV:
                function_url = "http://localhost:7071/api/ComponentInferenceHTTPTrigger" # the defult of function URL locally
            else: 
                function_url = "https://deployrdb.azurewebsites.net/api/ComponentInferenceHttpTrigger?code=Qq7-V_8Zu4r8UANItbryAifJS--yrUMr4vlnJBH2HU50AzFu5Kokhw=="
            
            # Define the request headers and parameters
            headers = {"Content-Type": "application/json; charset=utf-8"}
            # Add some filtering logic HERE, should have prior values
            patient_identifiers = utils.get_patient_identifiers(patient["CSN"],"CSN")
            patient.update(patient_identifiers)

            patient_identifiers = utils.get_patients_EPIs(patient["MRN"],"MRN")
            patient.update(patient_identifiers)


            culture_order=utils.get_anycultureorder
            last_value = utils.get_last_lab_results_numerical(
                patient["FHIR STU3"], lab_base_names=base_names
            )  # Need it!

            if (
                last_value is not None
            ):  # Only trigger for cases where all previous measurements exist

                #  count how many dose not have lab values
                Caount_dict = {
                "FHIR STU3":patient["FHIR STU3"],
                "FHIR R4": patient["FHIR"],
                "MRN":patient['SHCMRN'],
                "model": "ComponentInferenceHttpTrigger/20240229_combined.pkl",
                }
                cosmos.cosmoswrite(patient=Caount_dict,
                                container_id='20240715_Stability_Models_CountAnalysis',
                                partition_key= "ComponentInferenceHttpTrigger/20240229_combined.pkl") # count how many dose have lab values

        
                params = {
                    "FHIR": patient["FHIR"], "CSN": patient["CSN"], "BeddingUnit": patient["BeddingUnit"], **last_value
                }
                for retry in range(6):
                    # Send the HTTP GET request to your Azure Function
                    try:
                        response = requests.get(
                            function_url, headers=headers, params=params, timeout=900 #Timeout set as 15mins
                        )
                        if response.status_code == 200: #If succeed, just break
                            break
                        else:
                            if retry==5:
                                error=patient['SHCMRN']
                                Count_dict = {
                                        "API":"https://deployrdb.azurewebsites.net/api/ComponentInferenceHttpTrigger?code=Qq7-V_8Zu4r8UANItbryAifJS--yrUMr4vlnJBH2HU50AzFu5Kokhw==",
                                        "Error": error,
                                        "Time": timestamp,
                                        "model": "ComponentInferenceHttpTrigger/20240229_combined.pkl",
                                    }
                                cosmos.cosmoswrite(
                                        patient=Count_dict,
                                        container_id='20240722_Stability_Models_APIErrors',
                                        partition_key="ComponentInferenceHttpTrigger/20240229_combined.pkl"
                                    )
                    except:
                        error_dict = {
                            "FHIR R4": patient["FHIR"],
                            "Error": traceback.format_exc(),
                            "model": model_path,
                        }
                        cosmos.cosmoswrite(patient=error_dict,
                                container_id='20240708_Stability_Models_Error', 
                                partition_key= model_path) # could not call the HTTP reqeust for second module 
                #if LOCAL_DEV: #Only test on one patient for local debug
                #    break
            else:
                error_dict = {
                    "FHIR STU3": patient["FHIR STU3"],
                    "FHIR R4": patient["FHIR"],
                    "MRN": patient['SHCMRN'],
                    "model": model_path,
                }
                cosmos.cosmoswrite(patient=error_dict,
                           container_id='20240708_Stability_Models_NoLastValue',
                           partition_key= model_path) # number of patients that does not have last values 
            
    num_threads =1 
    def shuffle_split(id_list, num_chunks):
        random.shuffle(id_list)
        chunks = []
        len_per_chunk = 1+len(id_list)//num_chunks
        for i in range(num_chunks):
            chunks.append(id_list[len_per_chunk*i:min(len(id_list), len_per_chunk*(i+1))])
        return chunks
    
    patient_chunks = shuffle_split(patient_list, num_threads)


    threads = []
    for i in range(num_threads):
        thread = threading.Thread(target=process_patient_list, args=(patient_chunks[i],), name=f"Thread-{i+1}")
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    logging.info("Python timer trigger function ran at %s", timestamp)
    import json
    # Function to count occurrences of the word "MRN"
    def count_mrn(file_path):
        count = 0
        with open(file_path, 'r') as file:
            for line in file:
                # Parse the JSON from each line
                data = json.loads(line)
                # Convert to string and count occurrences of "MRN"
                count += str(data).count("MRN")
        return count

    # Assuming the file path is 'data.txt'
    file_path = '20240708_Stability_Models.txt'
    mrn_count = count_mrn(file_path)
    print(f'The word "MRN" appears {mrn_count} times in the 20240708_Stability_Models.')


    # Assuming the file path is 'data.txt'
    file_path = '20240708_Stability_Models_NoLastValue.txt'
    mrn_count = count_mrn(file_path)
    print(f'The word "MRN" appears {mrn_count} times in the 20240708_Stability_Models_NoLastValue.')
    

    # Assuming the file path is 'data.txt'
    file_path = '20240715_Stability_Models_CountAnalysis.txt'
    mrn_count = count_mrn(file_path)
    print(f'The word "MRN" appears {mrn_count} times in the 20240715_Stability_Models_CountAnalysis.')
    

    # Assuming the file path is 'data.txt'
    file_path = '20240708_Stability_Models_Error.txt'
    mrn_count = count_mrn(file_path)
    print(f'The word "MRN" appears {mrn_count} times in the 20240708_Stability_Models_Error.')
    # delete files 
    import os
    file_paths = ['20240708_Stability_Models.txt','20240708_Stability_Models_NoLastValue.txt','20240715_Stability_Models_CountAnalysis.txt','20240708_Stability_Models_Error.txt']
    for file_path in file_paths:
        if os.path.exists(file_path):
            os.remove(file_path)
        else:
            print(f"The file {file_path} does not exist.")





