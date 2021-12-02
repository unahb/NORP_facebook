First download the facebook data for good dataset from 
https://data.humdata.org/dataset/united-states-high-resolution-population-density-maps-demographic-estimates.

Execute create_facebook_table.py to create the facebook table in mysql.

Move a file from the downloaded set into a directory, and split it using
`split -l 10000 -a 5 FILENAME`. Remove this file from the directory.

Change the file pointer in python_scripts/testing.py so and the column name
so that the updates propagate to the correct place in mysql.

Once the data is inserted into MYSQL, the zipcode can be updated with 
lat_long.py (use the run.sh script to call lat_long.py).

The following query is of interest:

```
SELECT survey.EIN, SUM(facebook.POPULATION), COUNT(*)
FROM facebook, survey
WHERE (
111.111 *
    DEGREES(ACOS(LEAST(1.0, COS(RADIANS(survey.LATITUDE))
         * COS(RADIANS(facebook.LATITUDE))
         * COS(RADIANS(survey.LONGITUDE - facebook.LONGITUDE))
         + SIN(RADIANS(survey.LATITUDE))
         * SIN(RADIANS(facebook.LATITUDE))))) < 5
       AND facebook.POPULATION IS NOT NULL)
GROUP BY EIN;
```
where 5 represents the distance in km and can be adjusted as required. 
