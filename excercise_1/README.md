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
DC  100.98529411764706
MD  100.37987987987988
DE  99.98744769874477
NY  96.80901713255184
NJ  96.39256535947712
NV  96.01920768307323
NH  95.55102040816327
VI  95.45833333333333
CA  95.29843735020611
CT  95.15523809523809
```

*  Which procedures have the greatest variability between hospitals?

__Script__: `/investigations/hospital_variability/hospital_variability.sql`

__Results:__

(Variability measured by standard deviation of score, third column below)

```
CAC_1	Relievers for Inpatient Asthma	0.14210359538237247
CAC_2	Systemic Corticosteroids for Inpatient Asthma	1.283411995128645
OP_22	Patient left without being seen	1.716651736642136
AMI_2	Aspirin Prescribed at Discharge	3.805963814902322
STK_2	Discharged on Antithrombotic Therapy	3.882239441173123
OP_7	Prophylactic Antibiotic Selection for Surgical Patients	3.915771074179818
SCIP_VTE_2	Surgery Patients Who Received Appropriate Venous Thromboembolism Prophylaxis Within 24 Hours Prior to Surgery to 24 Hours After Surgery	4.6923485225052755
OP_6	Timing of Antibiotic Prophylaxis	5.069025482798145
OP_4	Aspirin at Arrival	5.206198494502723
STK_5	Antithrombotic Therapy By End of Hospital Day 2	5.370756360124224
VTE_4	Venous Thromboembolism Patients Receiving Unfractionated Heparin with Dosages/Platelet Count Monitoring by Protocol or Nomogram	5.590660865039616
AMI_10	Statin Prescribed at Discharge	5.620962943158109
STK_10	Assessed for Rehabilitation	5.668900431041956
OP_5	Median Time to ECG	6.109381800089672
STK_3	Anticoagulation Therapy for Atrial Fibrillation/Flutter	6.2262197795529595
HF_3	ACEI or ARB for LVSD	6.460854228892995
AMI_8a	Primary PCI Received Within 90 Minutes of Hospital Arrival	6.694532539262592
PC_01	Elective Delivery	6.958257962124783
VTE_2	Intensive Care Unit Venous Thromboembolism Prophylaxis	7.478245376071566
```

* Are average scores for hospital quality or procedural variability correlated with patient survey responses?

__Script__: `/investigations/hospitals_and_patients/hospitals_and_patients.sql`

__Results:__
```
# Hospital ID    Hospital Name    Quality Score    Survey Score
450348  	FALLS COMMUNITY HOSPITAL AND CLINIC                 	235.58333333333334	62.0
310002  	NEWARK BETH ISRAEL MEDICAL CENTER                   	134.81578947368422	0.0
140300  	PROVIDENT HOSPITAL OF CHICAGO                       	123.66666666666667	34.0
330202  	KINGS COUNTY HOSPITAL CENTER                        	123.1951219512195	0.0
450289  	HARRIS HEALTH SYSTEM                                	121.15384615384616	8.0
330182  	ST FRANCIS HOSPITAL, ROSLYN                         	119.29729729729729	42.0
050060  	COMMUNITY REGIONAL MEDICAL CENTER                   	119.17948717948718	14.0
140068  	ROSELAND COMMUNITY HOSPITAL                         	117.80952380952381	0.0
320001  	UNM HOSPITAL                                        	115.525	6.0
330306  	LUTHERAN MEDICAL CENTER                             	115.25641025641026	4.0
050138  	KAISER FOUNDATION HOSPITAL - LOS ANGELES            	115.2	22.0
110079  	GRADY MEMORIAL HOSPITAL                             	115.16216216216216	16.0
010087  	UNIVERSITY OF SOUTH ALABAMA MEDICAL CENTER          	115.11428571428571	27.0
290022  	DESERT SPRINGS HOSPITAL                             	112.86842105263158	8.0
050292  	RIVERSIDE COUNTY REGIONAL MEDICAL CENTER            	112.75	5.0
450213  	UNIVERSITY HEALTH SYSTEM                            	112.41463414634147	12.0
050492  	CLOVIS COMMUNITY MEDICAL CENTER                     	112.39473684210526	29.0
050373  	LAC+USC MEDICAL CENTER                              	112.27027027027027	8.0
330141  	BROOKHAVEN MEMORIAL HOSPITAL MEDICAL CENTER         	112.225	7.0
050315  	KERN MEDICAL CENTER                                 	112.17647058823529	18.0


```















