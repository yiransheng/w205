import re

from pyspark import SparkContext

try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO

import utils

def main():
    sc = SparkContext( appName="Transforming Eff Care" )
    src = sc.textFile(utils.data_home + "/hospitals.csv")

    rm_quotes = src.map(utils.to_row_sep).map(utils.to_row_string)

    rm_quotes.saveAsTextFile(utils.data_home + "/hospitals_data")

if __name__ == '__main__':
    main()

