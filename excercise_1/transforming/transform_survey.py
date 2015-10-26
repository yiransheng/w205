import re
from pyspark import SparkContext

import utils

orig_headers = [
    "Provider Number",
    "Hospital Name",
    "Address",
    "City",
    "State",
    "ZIP Code",
    "County Name",
    "Communication with Nurses Achievement Points",
    "Communication with Nurses Improvement Points",
    "Communication with Nurses Dimension Score",
    "Communication with Doctors Achievement Points",
    "Communication with Doctors Improvement Points",
    "Communication with Doctors Dimension Score",
    "Responsiveness of Hospital Staff Achievement Points",
    "Responsiveness of Hospital Staff Improvement Points",
    "Responsiveness of Hospital Staff Dimension Score",
    "Pain Management Achievement Points",
    "Pain Management Improvement Points",
    "Pain Management Dimension Score",
    "Communication about Medicines Achievement Points",
    "Communication about Medicines Improvement Points",
    "Communication about Medicines Dimension Score",
    "Cleanliness and Quietness of Hospital Environment Achievement Points",
    "Cleanliness and Quietness of Hospital Environment Improvement Points",
    "Cleanliness and Quietness of Hospital Environment Dimension Score",
    "Discharge Information Achievement Points",
    "Discharge Information Improvement Points",
    "Discharge Information Dimension Score",
    "Overall Rating of Hospital Achievement Points",
    "Overall Rating of Hospital Improvement Points",
    "Overall Rating of Hospital Dimension Score",
    "HCAHPS Base Score",
    "HCAHPS Consistency Score"
]

new_headers = [
    "Provider Number",
    "Communication with Nurses Achievement Points",
    "Communication with Nurses Improvement Points",
    "Communication with Nurses Dimension Score",
    "Communication with Doctors Achievement Points",
    "Communication with Doctors Improvement Points",
    "Communication with Doctors Dimension Score",
    "Responsiveness of Hospital Staff Achievement Points",
    "Responsiveness of Hospital Staff Improvement Points",
    "Responsiveness of Hospital Staff Dimension Score",
    "Pain Management Achievement Points",
    "Pain Management Improvement Points",
    "Pain Management Dimension Score",
    "Communication about Medicines Achievement Points",
    "Communication about Medicines Improvement Points",
    "Communication about Medicines Dimension Score",
    "Cleanliness and Quietness of Hospital Environment Achievement Points",
    "Cleanliness and Quietness of Hospital Environment Improvement Points",
    "Cleanliness and Quietness of Hospital Environment Dimension Score",
    "Discharge Information Achievement Points",
    "Discharge Information Improvement Points",
    "Discharge Information Dimension Score",
    "Overall Rating of Hospital Achievement Points",
    "Overall Rating of Hospital Improvement Points",
    "Overall Rating of Hospital Dimension Score",
    "HCAHPS Base Score",
    "HCAHPS Consistency Score"
]

def transform_row(line_parts):
    new_headers = utils.map_headers(new_headers, orig_headers)
    new_parts = []
    for (key, index), transform in zip(new_headers.iteritems(), transform_fns):
        if index > -1:
            value = line_parts[index]
            value = transform(value)
        else:
            value = utils.NULL_FMT

        new_parts.append(value)

    return new_parts

def parse_point(s):
    '''
    parse string "8 out 9" into point score 8
    '''
    s = s.strip()
    m = re.search(r'(\d+)\s+out\s+of', s)
    if m is not None:
        pt = int( m.group(1) )
    else:
        pt = utils.NULL_FMT

    return pt

transforms_fns = [
    lambda x: x,
    parse_point,
    parse_point,
    parse_point,
    parse_point,
    parse_point,
    parse_point,
    parse_point,
    parse_point,
    parse_point,
    parse_point,
    parse_point,
    parse_point,
    parse_point,
    parse_point,
    parse_point,
    parse_point,
    parse_point,
    parse_point,
    parse_point,
    parse_point,
    parse_point,
    parse_point,
    parse_point,
    parse_point,
    utils.convert_int,
    utils.convert_int
]

def main():
    sc = SparkContext( appName="Transforming Eff Care" )
    src = sc.textFile(utils.data_home + "/surveys_responses.csv")

    transformed = src.map(to_row_sep).map(transform_row).map(to_row_string)

    transformed.saveAsTextFile(utils.data_home + "/surveys_data")

if __name__ == '__main__':
    main()

