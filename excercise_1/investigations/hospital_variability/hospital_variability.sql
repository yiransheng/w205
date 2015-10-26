SELECT measures.MEASURE_ID, measures.MEASURE_NAME, measure_var.SCORE_STDDEV
  FROM measures JOIN (
    SELECT MEASURE_ID, STDDEV_POP(SCORE) AS SCORE_STDDEV FROM procedures
      WHERE SCORE IS NOT NULL
      GROUP BY MEASURE_ID
  ) AS measure_var ON (measures.MEASURE_ID = measure_var.MEASURE_ID)
  ORDER BY measure_var.SCORE_STDDEV;
