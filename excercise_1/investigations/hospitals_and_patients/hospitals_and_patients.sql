SELECT hospitals.PROVIDER_ID, hospitals.NAME, best.AVG_SCORE, surveys_hospital.AVG_SURVEY_SCORE
  FROM hospitals 
    JOIN (
      SELECT PROVIDER_ID, AVG(SCORE) AS AVG_SCORE FROM procedures
        WHERE SCORE IS NOT NULL
        GROUP BY PROVIDER_ID
    ) AS best ON (hospitals.PROVIDER_ID = best.PROVIDER_ID)
    JOIN (
      SELECT PROVIDER_ID, AVG(HCAHPS_BASE_SCORE) AS AVG_SURVEY_SCORE FROM surveys
        WHERE HCAHPS_BASE_SCORE IS NOT NULL
        GROUP BY PROVIDER_ID
    ) AS surveys_hospital ON (hospitals.PROVIDER_ID = surveys_hospital.PROVIDER_ID)
  ORDER BY best.AVG_SCORE;
  
  
