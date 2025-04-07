import os
import requests
from requests.auth import HTTPBasicAuth
import json


def get_patient_identifiers(patient_id, patient_id_type):
    """
    Populates patient_identifier attribute with EPIC and FHIR identifiers.
    The Rest APIs we'll be using to pull in patient features will require
    different forms of identification for a particular patient.  Here we
    ensure we have all of them.
    Args:
        patient_csn: CSN at index time for patient in question
    """
    """
    if self.from_fhir:
        id_ = self.patient_dict['FHIR STU3']
        id_type = 'FHIR STU3'
    else:
        id_ = self.csn
        id_type = "CSN"
    """
    ReqPatientIDs = requests.get(
        (
            f"{os.environ['EPIC_ENV']}api/epic/2010/Common/Patient/"
            "GETPATIENTIDENTIFIERS/Patient/Identifiers"
        ),
        params={"PatientID": patient_id, "PatientIDType": patient_id_type},
        headers={
            "Content-Type": "application/json; charset=utf-8",
            "Epic-Client-ID": os.environ["EPIC_CLIENT_ID"],
        },
        auth=HTTPBasicAuth(os.environ["secretID"], os.environ["secretpass"]),
    )
    patient_json = json.loads(ReqPatientIDs.text)
    patient_dict = {}
    for id_load in patient_json["Identifiers"]:
        patient_dict[id_load["IDType"]] = id_load["ID"]
    return patient_dict


def get_patients_in_unit(unit_ids=[], unit_names=[]):
    """
    Returns a list of patient in certain units, all POC
    """
    if not isinstance(unit_ids, list):
        unit_ids = [unit_ids]
        unit_names = [unit_names]
    
    PatientList = []
    def get_patients_in_single_unit(unit_id, unit_name):
        UnitJSONresp = requests.get(
            f"{os.environ['EPIC_ENV']}api/epic/2012/Access/Patient/GETCENSUSBYUNIT/UnitCensus",
            params={"UnitID": unit_id},
            headers={
                "Content-Type": "application/json; charset=utf-8",
                "Epic-Client-ID": os.environ["EPIC_CLIENT_ID"],
            },
            auth=HTTPBasicAuth(os.environ["secretID"], os.environ["secretpass"]),
        )
        UnitJSON = json.loads(UnitJSONresp.text)
        for key in UnitJSON["BeddedPatients"]:
            Patient = {}
            Patient["BeddingUnit"] = unit_name
            Patient["Name"] = key["FirstName"]
            for value in key["PatientIDs"]:
                Patient[value["Type"]] = value["ID"]
            for attri in key["ContactIDs"]:
                Patient[attri["Type"]] = attri["ID"]
            PatientList.append(Patient)
    for idx in range(len(unit_ids)):
        get_patients_in_single_unit(unit_ids[idx], unit_names[idx])
    

    print('**************PatientList*****************',len(PatientList))
    return PatientList

def get_last_lab_results_numerical(
    fhir_stu3, look_back=3, lab_base_names=["PLT", "HCT", "WBC", "HGB"]
):
    """
    Pulls lab results data for desired base_name component using
    `GETPATIENTRESULTCOMPONENTS` API. Populates the patient_dict attribute in bag of words fashion.
    """
    results = {}
    for base_name in lab_base_names:
        lab_result_packet = {
            "PatientID": fhir_stu3,
            "PatientIDType": "FHIR STU3",
            "UserID": os.environ["secretID"][4:],
            "UserIDType": "External",
            "NumberDaysToLookBack": look_back,
            "MaxNumberOfResults": 1,  # Get most recent
            "FromInstant": "",
            "ComponentTypes": [{"Value": base_name.upper(), "Type": "base-name"}],
        }

        lab_component_packet = json.dumps(lab_result_packet)
        lab_component_response = requests.post(
            (
                f'{os.environ["EPIC_ENV"]}api/epic/2014/Results/Utility/'
                'GETPATIENTRESULTCOMPONENTS/ResultComponents'
            ),
            headers={
                "Content-Type": "application/json; charset=utf-8",
                "Epic-Client-ID": os.environ["EPIC_CLIENT_ID"],
            },
            auth=HTTPBasicAuth(os.environ["secretID"], os.environ["secretpass"]),
            data=lab_component_packet,
        )
        numeric = "0123456789-."
        lab_response = json.loads(lab_component_response.text)
        # print(lab_response)
        if (
            "ResultComponents" in lab_response and lab_response["ResultComponents"]
        ):  # not none
            for i in range(len(lab_response["ResultComponents"])):
                if lab_response["ResultComponents"][i]["Value"] is None:
                    continue
                value = lab_response["ResultComponents"][i]["Value"][0]
                # Convert to numeric
                num_value = ""
                for i, c in enumerate(value):
                    if c in numeric:
                        num_value += c
                try:
                    value = float(num_value)
                    results[f'last_{base_name}'] = value
                except:
                    # Log parsing error in patient dictionary
                    return None
        else:
            pass#return None
    if len(results) == 0: #No previous measure for any component
        return None
    return results

