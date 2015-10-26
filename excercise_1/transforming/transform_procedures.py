import csv

from collections import OrderedDict
from functools import partial

from pyspark import SparkContext

try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO

import utils

def transform_row(line_parts, new_headers, orig_headers):
    '''
    transform a single csv line
    input is a list of cell values :: [str]
    output is a list of transformed values
    '''
    assert( len(orig_headers) == len(line_parts) )
    new_line_parts = []
    for (key, index) in new_headers.iteritems():
        if index > -1:
            value = line_parts[index]
        else:
            value = utils.NULL_FMT
        if key == "Score" or key == "Sample":
            value = utils.convert_int(value) # convert score to int type
        if key == "Lower Estimate" or key == "Higher Estimate":
            value = utils.convert_float(value) # convert score to int type

        new_line_parts.append(value)

    return new_line_parts

eff_care_headers = [
  "Provider ID",
  "Hospital Name",
  "Address",
  "City",
  "State",
  "ZIP Code",
  "County Name",
  "Phone Number",
  "Condition",
  "Measure ID",
  "Measure Name",
  "Score",
  "Sample",
  "Footnote",
  "Measure Start Date",
  "Measure End Date"
]

readmission_headers = [
  "Provider ID",
  "Hospital Name",
  "Address",
  "City",
  "State",
  "ZIP Code",
  "County Name",
  "Phone Number",
  "Measure Name",
  "Measure ID",
  "Compared to National",
  "Denominator",
  "Score",
  "Lower Estimate",
  "Higher Estimate",
  "Footnote",
  "Measure Start Date",
  "Measure End Date"
]

proc_headers = [
  "Provider ID",
  "Measure ID",
  "Score",
  "Sample", # only for effective care procs
  "Condition", # only for effective care procs
  "Compared to National", # only for readmissions procs
  "Denominator", # only for readmissino procs
  "Lower Estimate", # only for readmission procs
  "Higher Estimate", # only for readmission procs
  "Footnote"
]

def main():
    sc = SparkContext( appName="Transforming Eff Care" )
    src_effcare = sc.textFile(utils.data_home + "/effective_care.csv")
    src_readmission = sc.textFile(utils.data_home + "/readmissions.csv")

    transform1 = partial(transform_row,
                        orig_headers=eff_care_headers,
                        new_headers=utils.map_headers(proc_headers, eff_care_headers))
    transform2 = partial(transform_row,
                        orig_headers=readmission_headers,
                        new_headers=utils.map_headers(proc_headers, readmission_headers))


    transformed_eff = src_effcare.map(utils.to_row_sep).map(transform1).map(utils.to_row_string)
    transformed_readmission = src_readmission.map(utils.to_row_sep).map(transform2).map(utils.to_row_string)

    (transformed_eff + transformed_readmission) \
        .saveAsTextFile(utils.data_home + "/procedures_data")



if __name__ == "__main__":
    main()
