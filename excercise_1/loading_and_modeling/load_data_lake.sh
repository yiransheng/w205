wget -O Hospital_Revised_Flatfiles.zip https://data.medicare.gov/views/bg9k-emty/files/Nqcy71p9Ss2RSBWDmP77H1DQXcyacr2khotGbDHHW_s?content_type=application%2Fzip%3B%20charset%3Dbinary&filename=Hospital_Revised_Flatfiles.zip 

mkdir tmp
unzip Hospital_Revised_Flatfiles.zip -d tmp
cd tmp && ls *.csv -l

# hospital data 
tail -n +2 Hospital\ General\ Information.csv > ../hospitals.csv
# procedure data
tail -n +2 Timely\ and\ Effective\ Care\ -\ Hospital.csv > ../effective_care.csv
tail -n +2 Readmissions\ and\ Deaths\ -\ Hospital.csv > ../readmissions.csv
tail -n +2 Measure\ Dates.csv > ../measure_dates.csv
# survey data
tail -n +2 hvbp_hcahps_05_28_2015.csv >../surveys_responses.csv

cd ..

HDFS_CSV_DEST="/user/$USER/hospital_compare"
hdfs dfs -mkdir -p $HDFS_CSV_DEST 

hdfs dfs -copyFromLocal *.csv $HDFS_CSV_DEST

# for the lazy, set permissive access
# hdfs dfs -chmod -R 1777 $HDFS_CSV_DEST  
