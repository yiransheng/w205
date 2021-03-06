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
