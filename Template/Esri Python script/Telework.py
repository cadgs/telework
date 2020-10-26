import json
import time
import requests
import csv
import keyring
import pandas as pd
import traceback

from datetime import datetime as dt
from Logger import Logger

logger = Logger()

DEFAULT_BATCH_SIZE = 150
HEADER = [
    "Employee_Number",
    "Commute_Miles",
    "Commute_Minutes",
    "Work_Latitude",
    "Work_Longitude",
    "Home_Latitude",
    "Home_Longitude",
    "Flagged",
]
EMPLOYEE_ADDRESS_TYPE_FIELD = "Employee_Address_Type"
WORK_ADDRESS_TYPE = "WORK"
HOME_ADDRESS_TYPE = "HOME"


def read_config(file):
    """
    Reads the given file, converts it to a json (dict) object, and returns it
    """
    logger.info("Reading config file...")
    try:
        with open(file, "r") as configFile:
            jsonConfig = json.load(configFile)
            return jsonConfig
    except Exception as e:
        logger.critical("An error occured while reading from the config file.", e)
        logger.debug(traceback.format_exc())


def generate_token(username=None, password=None):
    """
    Gets a token from AGOL using the given username and password
    """
    logger.info("Generating Token.")
    url = "https://www.arcgis.com/sharing/rest/generateToken?"

    payload = f"f=json&username={username}&password={password}&referer=https://arcgis.com&expiration=120"
    headers = {"Content-Type": "application/x-www-form-urlencoded"}

    try:
        res_json = requests.post(url, headers=headers, data=payload).json()
    except Exception as e:
        logger.critical("An error occured when trying to generate a token.", e)
        return

    if "error" in res_json:
        error = res_json["error"]
        statusCode = error["code"]
        message = error["message"]
        logger.critical(
            f"Unable to generate a token. Received status code of {statusCode} with reason: {message}"
        )
        return

    return res_json["token"]


def geocode_addresses(records, token):
    """
    Geocodes addresses from the given records and returns their location data
    """
    logger.info("Geocoding addresses...")
    addresses = {"records": records}
    payload = {
        "f": "json",
        "addresses": json.dumps(addresses),
        "token": token,
        "category": "Address",
        "sourceCountry": "USA",
    }
    try:
        res_json = requests.get(
            "https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/geocodeAddresses",
            params=payload,
        ).json()
    except Exception as e:
        logger.critical("An error occurred while geocoding the addresses.", e)
        logger.debug(traceback.format_exc())
        logger.debug(payload)
        return None

    if "locations" in res_json:
        locations = res_json["locations"]
        location_count = len(locations)

        if location_count == 0:
            logger.critical(
                f"Unable to geocode addresses. See response for more info: {res_json}"
            )
            return None

        if location_count == 2:
            first_location = locations[0]["attributes"]
            first_location_id = first_location["ResultID"]
            second_location = locations[1]["attributes"]
            second_location_id = second_location["ResultID"]
            if first_location_id > second_location_id:
                first_location[EMPLOYEE_ADDRESS_TYPE_FIELD] = HOME_ADDRESS_TYPE
                second_location[EMPLOYEE_ADDRESS_TYPE_FIELD] = WORK_ADDRESS_TYPE
                first_location["ResultID"] = second_location_id
            else:
                first_location[EMPLOYEE_ADDRESS_TYPE_FIELD] = WORK_ADDRESS_TYPE
                second_location[EMPLOYEE_ADDRESS_TYPE_FIELD] = HOME_ADDRESS_TYPE
                second_location["ResultID"] = first_location_id

        return locations
    else:
        logger.critical(
            f"Unable to geocode addresses. See response for more info: {res_json}"
        )
        return None


def get_suggested_batch_size():
    """
    Retrieves the suggested batch geocode size from the Worl Geocoder Service
    """
    logger.info("Getting suggested batch geocode size...")
    try:
        res_json = requests.get(
            "https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer?f=json"
        ).json()
        if "SuggestedBatchSize" in res_json:
            return res_json["SuggestedBatchSize"]
        else:
            return DEFAULT_BATCH_SIZE
    except Exception as e:
        logger.error(
            f"An error occurred while retrieving the suggested batch size, using default ({DEFAULT_BATCH_SIZE}).",
            e,
        )
        logger.debug(traceback.format_exc())
        return DEFAULT_BATCH_SIZE


def locations_to_feature_collection(locations):
    logger.info("Converting locations to a feature collection...")
    feature_collection = {
        "layerDefinition": {
            "geometryType": "esriGeometryPoint",
            "fields": [
                {"name": "employee_number", "type": "esriFieldTypeOID"},
                {"name": EMPLOYEE_ADDRESS_TYPE_FIELD, "type": "esriFieldTypeString"},
                {"name": "Lat", "type": "esriFieldTypeDouble"},
                {"name": "Lon", "type": "esriFieldTypeDouble"},
            ],
        },
        "featureSet": {
            "geometryType": "esriGeometryPoint",
            "spatialReference": {"wkid": 4326},
            "features": [],
        },
    }

    features = feature_collection["featureSet"]["features"]
    for location in locations:
        lat = location["location"]["y"]
        lon = location["location"]["x"]
        features.append(
            {
                "geometry": {"x": lon, "y": lat,},
                "attributes": {
                    "employee_number": location["attributes"]["ResultID"],
                    EMPLOYEE_ADDRESS_TYPE_FIELD: location["attributes"][
                        EMPLOYEE_ADDRESS_TYPE_FIELD
                    ],
                    "Lat": lat,
                    "Lon": lon,
                },
            }
        )

    return feature_collection


def match_work_and_home(locations):
    """
    Matches the coordinates of locations to home and work addresses of the employees
    """
    logger.info("Matching work and home coordinates...")
    found_employees = []
    origins = []
    destinations = []
    flagged_employees = []
    for location in locations:
        employee = None
        try:
            employee = location["attributes"]["ResultID"]
            if employee in found_employees or employee in flagged_employees:
                continue

            employee_address_type = location["attributes"][EMPLOYEE_ADDRESS_TYPE_FIELD]
            address = location["address"]
            matching_location = next(
                (
                    l
                    for l in locations
                    if l["attributes"]["ResultID"] == employee
                    and l["address"] != address
                    and l["attributes"][EMPLOYEE_ADDRESS_TYPE_FIELD]
                    != employee_address_type
                ),
                None,
            )

            if not matching_location:
                if employee not in flagged_employees:
                    flagged_employees.append(employee)
                logger.warn(
                    f"Could not find matching location or the home and work addresses are the same for employee number {employee}"
                )
                continue

            if location["score"] == 0 or matching_location["score"] == 0:
                if employee not in flagged_employees:
                    flagged_employees.append(employee)
            else:
                origins.append(location)
                destinations.append(matching_location)

            found_employees.append(employee)
        except Exception as e:
            if employee:
                logger.error(
                    f"An error occurred while matching home and work address for employee number {employee}.",
                    e,
                )
                if employee not in flagged_employees:
                    flagged_employees.append(employee)
            else:
                logger.error(
                    f"An error occurred while matching home and work address.", e
                )
            logger.debug(traceback.format_exc())

    return origins, destinations, flagged_employees


def read_worker_info(
    worker_info_csv,
    employee_number_field,
    work_address_field,
    work_city_field,
    work_state_field,
    work_zip_field,
    home_address_field,
    home_city_field,
    home_state_field,
    home_zip_field,
):
    """
    Reads a given CSV for info on employees and geocodes them, returns the resulting locations of the geocodes
    """
    worker_info = pd.read_csv(worker_info_csv)
    records_list = []
    for _, worker in worker_info.iterrows():
        employee = None
        records = []
        try:
            employee = worker[employee_number_field]
            work_address = worker[work_address_field]
            work_city = worker[work_city_field]
            work_state = worker[work_state_field]
            work_zip = worker[work_zip_field]
            home_address = worker[home_address_field]
            home_city = worker[home_city_field]
            home_state = worker[home_state_field]
            home_zip = worker[home_zip_field]

            records.append(
                {
                    "attributes": {
                        "OBJECTID": employee,
                        "Address": work_address,
                        "City": work_city,
                        "Region": work_state,
                        "Postal": work_zip,
                    }
                }
            )
            records.append(
                {
                    "attributes": {
                        "OBJECTID": employee + 1,
                        "Address": home_address,
                        "City": home_city,
                        "Region": home_state,
                        "Postal": home_zip,
                    }
                }
            )
            records_list.append(records)
        except Exception as e:
            if employee:
                logger.error(
                    f"An error occurred while gathering data for employee number {employee}.",
                    e,
                )
            else:
                logger.error("An error occurred while gathering data for a worker.", e)
            logger.debug(traceback.format_exc())

    return records_list


def get_travel_mode(travel_mode_name, token):
    """
    Returns the json of a given travel mode
    """
    logger.info(f"Getting travel mode: {travel_mode_name}...")
    try:
        res_json = requests.get(
            f"https://route.arcgis.com/arcgis/rest/services/World/Utilities/GPServer/GetTravelModes/execute?f=json&token={token}"
        ).json()
    except Exception as e:
        logger.critical(
            "An error occurred while getting travel modes. Cannot calculate commute times and distances.",
            e,
        )
        logger.debug(traceback.format_exc())
        return

    travel_modes = next(
        (
            result
            for result in res_json["results"]
            if result["paramName"] == "supportedTravelModes"
        ),
        None,
    )
    if not travel_modes:
        logger.critical("Could not get travel modes from AGOL.")
        return

    travel_mode_json = next(
        (
            travel_mode["attributes"]["TravelMode"]
            for travel_mode in travel_modes["value"]["features"]
            if travel_mode["attributes"]["Name"] == travel_mode_name
        ),
        None,
    )

    return travel_mode_json


def calculate_commute(
    analysis_url, origin_fc, dest_fc, travel_mode, token, username, password
):
    """
    Runs the connect origins to destinations analysis tool using the given origins and destinations and returns the route features
    """
    start_time = dt.now()
    logger.info(f"Calculating commute times and distances...")
    logger.debug(f"Start time: {start_time}")
    try:
        analysis_url = f"{analysis_url}ConnectOriginsToDestinations"
        payload = {
            "originsLayer": json.dumps(origin_fc),
            "destinationsLayer": json.dumps(dest_fc),
            "measurementType": travel_mode,
            "originsLayerRouteIDField": "employee_number",
            "destinationsLayerRouteIDField": "employee_number",
            "f": "json",
            "token": token,
        }
        res_json = requests.post(f"{analysis_url}/submitJob", data=payload).json()

        if "jobId" in res_json:
            job_id = res_json["jobId"]
        else:
            logger.critical(
                f"Something caused the analysis tool to fail. See the response for more details: {res_json}"
            )
            return

        job_status = "esriJobSubmitted"
        while job_status == "esriJobSubmitted" or job_status == "esriJobExecuting":
            status = requests.get(
                f"{analysis_url}/jobs/{job_id}?f=json&token={token}"
            ).json()
            if "jobStatus" in status:
                job_status = status["jobStatus"]
                if job_status == "esriJobSucceeded":
                    routes_url = status["results"]["routesLayer"]["paramUrl"]
                    routes_json = requests.get(
                        f"{analysis_url}/jobs/{job_id}/{routes_url}?f=json&token={token}"
                    ).json()
                    return routes_json["value"]["featureSet"]["features"]
                elif job_status == "esriJobFailed":
                    logger.critical(
                        f"Something caused the analysis tool to fail. See the response for more details: {status}"
                    )
                    return
                else:
                    time.sleep(3)
            elif "error" in status:
                if "code" in status["error"] and status["error"]["code"] == 498:
                    token = generate_token(username, password)
                    time.sleep(3)
                else:
                    logger.critical(
                        f"Something caused the analysis tool to fail. See the response for more details: {status}"
                    )
                    return
    except Exception as e:
        logger.critical("An error occurred while calculating the commutes.", e)
        logger.debug(traceback.format_exc())
        return
    finally:
        end_time = dt.now()
        total_time = end_time - start_time
        logger.debug(f"Finished calculating commute at {end_time}")
        logger.debug(f"Total time - {total_time}")


def write_output(features, flagged_employees, header, output_file):
    """
    Writes out the given route features and flagged employees to a given output file
    """
    logger.info("Writing output...")
    rows = []
    for feature in features:
        attributes = feature["attributes"]
        employee_number = attributes["RouteName"]
        total_miles = attributes["Total_Miles"]
        total_minutes = attributes["Total_Minutes"]
        from_address_type = attributes[f"From_{EMPLOYEE_ADDRESS_TYPE_FIELD}"]
        if from_address_type == WORK_ADDRESS_TYPE:
            work_lat = attributes["From_Lat"]
            work_lon = attributes["From_Lon"]
            home_lat = attributes["To_Lat"]
            home_lon = attributes["To_Lon"]
        else:
            work_lat = attributes["To_Lat"]
            work_lon = attributes["To_Lon"]
            home_lat = attributes["From_Lat"]
            home_lon = attributes["From_Lon"]

        rows.append(
            [
                employee_number,
                total_miles,
                total_minutes,
                work_lat,
                work_lon,
                home_lat,
                home_lon,
                False,
            ]
        )

    for emp in flagged_employees:
        rows.append([emp, 0, 0, 0, 0, 0, 0, True])

    try:
        with open(output_file, "w", newline="") as output_csv:
            emp_writer = csv.writer(output_csv, delimiter=",")
            emp_writer.writerow(header)
            emp_writer.writerows(rows)
    except Exception as e:
        logger.critical("An error occurred while writing the employees to the csv.", e)
        logger.debug(traceback.format_exc())


def get_analysis_url(token):
    """
    Retrieves the analysis URL from AGOL with the given token.
    """
    logger.info("Getting analysis URL...")
    try:
        res_json = requests.get(
            f"https://www.arcgis.com/sharing/rest/portals/self?f=json&token={token}"
        ).json()
        if (
            "helperServices" in res_json
            and "analysis" in res_json["helperServices"]
            and "url" in res_json["helperServices"]["analysis"]
        ):
            return res_json["helperServices"]["analysis"]["url"] + "/"
        else:
            logger.critical("Unable to retrieve analysis URL. Cannot run analysis.")
    except Exception as e:
        logger.critical("Unable to retrieve analysis URL. Cannot run analysis.", e)
        logger.debug(traceback.format_exc())


def main():
    """
    Writes the output of commute times and distances for employees from a provided CSV
    """
    logger.info("Starting...")
    config_values = read_config("config.json")
    if not config_values:
        return

    try:
        logger.setLevel(config_values["log_level"])
        profile = config_values["profile"]
        username = config_values["username"]
        worker_info_csv = config_values["worker_info_csv"]
        employee_number_field = config_values["employee_number_field"]
        work_address_field = config_values["work_address_field"]
        work_city_field = config_values["work_city_field"]
        work_state_field = config_values["work_state_field"]
        work_zip_field = config_values["work_zip_field"]
        home_address_field = config_values["home_address_field"]
        home_city_field = config_values["home_city_field"]
        home_state_field = config_values["home_state_field"]
        home_zip_field = config_values["home_zip_field"]
        csv_out = config_values["csv_out"]
    except KeyError as e:
        logger.critical(f"Required config values are missing: {e}")
        return

    records_list = read_worker_info(
        worker_info_csv,
        employee_number_field,
        work_address_field,
        work_city_field,
        work_state_field,
        work_zip_field,
        home_address_field,
        home_city_field,
        home_state_field,
        home_zip_field,
    )

    if not records_list:
        return

    password = keyring.get_password(profile, username)

    token = generate_token(username, password)

    locations = []
    for records in records_list:
        locations.extend(geocode_addresses(records, token))

    if not locations:
        return

    origins, destinations, flagged_employees = match_work_and_home(locations)

    origin_fc = locations_to_feature_collection(origins)
    dest_fc = locations_to_feature_collection(destinations)

    analysis_url = get_analysis_url(token)
    if not analysis_url:
        return

    travel_mode = get_travel_mode("Driving Distance", token)
    if not travel_mode:
        return

    features = calculate_commute(
        analysis_url, origin_fc, dest_fc, travel_mode, token, username, password
    )
    if not features:
        return

    write_output(features, flagged_employees, HEADER, csv_out)

    logger.info("Finished.")


if __name__ == "__main__":
    main()
