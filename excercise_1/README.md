# MIDS W205 Exercise 1

Note: This exercise assumes running under user `root`. 

## Loading Data into HDFS

[`/loading_and_modeling/load_data_lake.sh`](./loading_and_modeling/load_data_lake.sh) downloads, unzips raw data and copies from local to HDFS location: `"/user/$USER/hospital_compare"`. 

Files copied are:
```
63280769   2015-10-17 06:22 hospital_compare/effective_care.csv
826758     2015-10-17 06:22 hospital_compare/hospitals.csv
13146      2015-10-17 06:22 hospital_compare/measure_dates.csv
19936145   2015-10-17 06:22 hospital_compare/readmissions.csv
1348499    2015-10-17 06:22 hospital_compare/surveys_responses.csv
```

## Transforming Data

The following scripts in `/transforming` can be used to transform raw csv files into header-less and quote-less csv files which conforms to our Hive schema sepcified in DDLs. 

```
transform_hospitals.py
transform_measures.py
transform_procedures.py
transform_surveys.py
```

Script should be run using `pyspark`:

```
> spark-submit --master yarn-client --py-files="utils.py" <transform_script>
```

`<transform_script>` is one of the scripts listed above. 

These scripts performs the following operations:

* Remove quotes and escape "," to "\," from raw csv files (Hive does not support quoted csv fields, and will treak quote chars as part of data when using csv for external tables)

* Parse numeric values such as survey response score "2 out of 9" from string to int / float types 
    - In case of missing values or parse errors, we write "\N", which is Hive's default NULL value 

* Combine `effective_care.csv` and `readmissions.csv` into a single table `hospital_compare/procedures_data` (`transform_procedures.py`)

Once all scripts have been run successfully, the following data dirs should be created:

```
hospital_comapre/hospitals_data
hospital_comapre/measures_data
hospital_comapre/procedures_data
hospital_comapre/surveys_data
```

## Hive DDLs

The following `Hive` DDLs defines external tables, they can be found in `/loading_and_modeling/hive_base_ddl.sql`. Initilize tables:

```
hive –f ./loading_and_modeling/hive_base_ddl.sql
```

(Note: all CHAR filed length are based on information provided in `Hospital.pdf` from downloaded data source)

### Hospitals Table

```
CREATE EXTERNAL TABLE hospitals (
  PROVIDER_ID CHAR(8),
  NAME CHAR(52),
  ADDRESS CHAR(52),
  CITY CHAR(22),
  STATE CHAR(4),
  ZIP_CODE CHAR(7),
  COUNTY_NAME CHAR(22),
  PHONE CHAR(12),
  HOSPITAL_TYPE CHAR(38),
  OWNERSHIP CHAR(45),
  EMERGENCY_SERVICES CHAR(5)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
ESCAPED BY '\\'
STORED AS TEXTFILE
LOCATION '/user/root/hospital_compare/hospitals_data';
```

### Procedures Table

```
CREATE EXTERNAL TABLE procedures (
  PROVIDER_ID CHAR(8),
  MEASURE_ID STRING,
  SCORE INT,
  SAMPLE INT,
  CONDITION CHAR(37),
  COMP_TO_NATIONAL CHAR(37),
  DENOMINATOR CHAR(15),
  LOWER_ESTI FLOAT,
  HIGHER_ESTI FLOAT,
  FOOTNOTE STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',' 
ESCAPED BY '\\'
STORED AS TEXTFILE
LOCATION '/user/root/hospital_compare/procedures_data';
```

### Measures Table

```
CREATE EXTERNAL TABLE measures (
  MEASURE_NAME STRING,
  MEASURE_ID STRING,
  MEASURE_START_QUARTER CHAR(50),
  MEASURE_START_DATE DATE,
  MEASURE_END_QUARTER CHAR(50),
  MEASURE_END_DATE DATE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
ESCAPED BY '\\'
STORED AS TEXTFILE
LOCATION '/user/root/hospital_compare/measures_data';
```

### Surveys Table

```
CREATE EXTERNAL TABLE surveys (
  PROVIDER_ID CHAR(8),
  COMMUNICATION_WITH_NURSES_ACHIEVEMENT_POINTS INT,
  COMMUNICATION_WITH_NURSES_IMPROVEMENT_POINTS INT,
  COMMUNICATION_WITH_NURSES_DIMENSION_SCORE INT,
  COMMUNICATION_WITH_DOCTORS_ACHIEVEMENT_POINTS INT,
  COMMUNICATION_WITH_DOCTORS_IMPROVEMENT_POINTS INT,
  COMMUNICATION_WITH_DOCTORS_DIMENSION_SCORE INT,
  RESPONSIVENESS_OF_HOSPITAL_STAFF_ACHIEVEMENT_POINTS INT,
  RESPONSIVENESS_OF_HOSPITAL_STAFF_IMPROVEMENT_POINTS INT,
  RESPONSIVENESS_OF_HOSPITAL_STAFF_DIMENSION_SCORE INT,
  PAIN_MANAGEMENT_ACHIEVEMENT_POINTS INT,
  PAIN_MANAGEMENT_IMPROVEMENT_POINTS INT,
  PAIN_MANAGEMENT_DIMENSION_SCORE INT,
  COMMUNICATION_ABOUT_MEDICINES_ACHIEVEMENT_POINTS INT,
  COMMUNICATION_ABOUT_MEDICINES_IMPROVEMENT_POINTS INT,
  COMMUNICATION_ABOUT_MEDICINES_DIMENSION_SCORE INT,
  CLEANLINESS_AND_QUIETNESS_OF_HOSPITAL_ENVIRONMENT_ACHIEVEMENT_POINTS INT,
  CLEANLINESS_AND_QUIETNESS_OF_HOSPITAL_ENVIRONMENT_IMPROVEMENT_POINTS INT,
  CLEANLINESS_AND_QUIETNESS_OF_HOSPITAL_ENVIRONMENT_DIMENSION_SCORE INT,
  DISCHARGE_INFORMATION_ACHIEVEMENT_POINTS INT,
  DISCHARGE_INFORMATION_IMPROVEMENT_POINTS INT,
  DISCHARGE_INFORMATION_DIMENSION_SCORE INT,
  OVERALL_RATING_OF_HOSPITAL_ACHIEVEMENT_POINTS INT,
  OVERALL_RATING_OF_HOSPITAL_IMPROVEMENT_POINTS INT,
  OVERALL_RATING_OF_HOSPITAL_DIMENSION_SCORE INT,
  HCAHPS_BASE_SCORE INT,
  HCAHPS_CONSISTENCY_SCORE INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
ESCAPED BY '\\'
STORED AS TEXTFILE
LOCATION '/user/root/hospital_compare/surveys_data';
```

## Investigations

* What hospitals are models of high-quality care—that is, which hospitals have the most consistently high scores for a variety of procedures?

__Script__: `/investigations/best_hospitals/best_hospitals.sql`

__Results:__

```
511313  BOONE MEMORIAL HOSPITAL                             125.33333333333333
331316  COMMUNITY MEMORIAL HOSPITAL, INC                    127.66666666666667
261317  MERCY HOSPITAL CASSVILLE                            128.0
400032  HOSPITAL HERMANOS MELENDEZ INC                      130.21052631578948
051318  REDWOOD MEMORIAL HOSPITAL                           130.33333333333334
310002  NEWARK BETH ISRAEL MEDICAL CENTER                   134.81578947368422
451330  MEDINA REGIONAL HOSPITAL                            136.0
400079  HOSP COMUNITARIO BUEN SAMARITANO                    167.25
051335  BEAR VALLEY COMMUNITY HOSPITAL                      175.33333333333334
450348  FALLS COMMUNITY HOSPITAL AND CLINIC                 235.58333333333334

```

*  What states are models of high-quality care?

__Script__: `/investigations/best_states/best_states.sql`

__Results:__

```

```

*  Which procedures have the greatest variability between hospitals?

__Script__: `/investigations/hospital_variability/hospital_variability.sql`

__Results:__

```

```

* Are average scores for hospital quality or procedural variability correlated with patient survey responses?

__Script__: `/investigations/hospitals_and_patients/hospitals_and_patients.sql`

__Results:__
















