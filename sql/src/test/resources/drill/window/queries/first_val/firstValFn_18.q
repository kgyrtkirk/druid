SELECT col7 , col4, FIRST_VALUE(col4) OVER(PARTITION BY col7 ORDER BY col4) FIRST_VALUE_col4 FROM "allTypsUniq.parquet" WHERE col4 IN ('20:20:20.300' , '19:24:45.200' , '23:45:35.120' , '23:23:30.222' , '16:35:45.100' , '10:59:58.119' , '15:20:30.230')