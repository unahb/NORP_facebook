First download the facebook data for good dataset from 
https://data.humdata.org/dataset/united-states-high-resolution-population-density-maps-demographic-estimates

The files from the facebook data for good are far too large to read into memory
and process, so the solution is to split them. 

Use the split command (https://linux.die.net/man/1/split) 
`split -l 10000 -a 5 FILENAME` and then change the file in testing and 
calling testing.py on the directory that contains all the subfiles. 

Once the data is inserted into MYSQL, the zipcode can be updated with 
lat_long.py