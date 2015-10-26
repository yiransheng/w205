from time import strptime, strftime
from pyspark import SparkContext

import utils


def transform_row(line_parts):
    line_parts[5] = strftime('%Y-%m-%d', strptime(line_parts[5], '%Y-%m-%d %H:%M:%S'))
    line_parts[3] = strftime('%Y-%m-%d', strptime(line_parts[3], '%Y-%m-%d %H:%M:%S'))
    return line_parts

def main():
    sc = SparkContext( appName="Transforming Eff Care" )
    src = sc.textFile(utils.data_home + "/measure_dates.csv")

    transformed = src.map(utils.to_row_sep).map(transform_row).map(utils.to_row_string)

    transformed.saveAsTextFile(utils.data_home + "/measures_data")

if __name__ == '__main__':
    main()


