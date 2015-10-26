SELECT hospitals.STATE, AVG(procedures.SCORE) AS STATE_AVG_SCORE
  FROM hospitals JOIN procedures
    ON (hospitals.PROVIDER_ID = procedures.PROVIDER_ID)
  WHERE procedures.SCORE IS NOT NULL
  GROUP BY hospitals.STATE
  ORDER BY STATE_AVG_SCORE;
  
