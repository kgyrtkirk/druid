SELECT col7 , col5, LAG(col5) OVER(PARTITION BY col7 ORDER BY col5) LAG_col5
FROM "allTypsUniq.parquet"
WHERE
    col5 >= TIMESTAMP_TO_MILLIS(TIME_PARSE('1947-07-02 00:28:02.418', 'yyyy-MM-dd HH:mm:ss.SSS')) and
    col5 < TIMESTAMP_TO_MILLIS(TIME_PARSE('2011-06-02 00:28:02.218', 'yyyy-MM-dd HH:mm:ss.SSS'))