import csv

from collections import OrderedDict

# Hive default Text Format uses LazySimpleSerDe which interprets the string \N as NULL when importing.
NULL_FMT = "\N"

data_home = "hdfs:///user/root/hospital_compare"

def convert_int(s):
    try:
        return int(s)
    except ValueError:
        return NULL_FMT

def convert_float(s):
    try:
        return float(s)
    except ValueError:
        return NULL_FMT


def header_fmt_snake_case(s):
    '''
    snake_case identifier formatting function:
    removing spaces in column name and convert to lower case
    '''
    return re.sub(r'\s+', '_', s.lower())

def to_row_sep(line,d=','):
    for r in csv.reader([line.encode('utf-8')], delimiter=d, quoting=csv.QUOTE_ALL):
        return r


def escape_part(part):
    return part.replace(',', "\\,").replace('\n', ' ')

def to_row_string(line_parts):
    line_parts = [escape_part(p) for p in line_parts]
    return ','.join(line_parts)

def map_headers(new_headers, orig_headers):
    '''
    Produces a ordered dict that maps header name to indices in original file
    if no header exist in original file, index is set to -1
    '''
    origdict = dict(zip(orig_headers, range( len(orig_headers) )))
    new_header_pairs = [
        (h, origdict[h]) if h in origdict else (h, -1) \
        for h in new_headers
    ]
    return OrderedDict(new_header_pairs)


