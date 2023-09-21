rm -f /home/hadoop/landing/*

wget -P /home/hadoop/landing https://data-engineer-edvai.s3.amazonaws.com/CarRentalData.csv

wget -P /home/hadoop/landing -O /home/hadoop/landing/geo_ref.csv https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/georef-united-states-of-america-state/exports/csv?lang=en&timezone=America%2FArgentina%2FBuenos_Aires&use_labels=true&delimiter=%3B


/home/hadoop/hadoop/bin/hdfs dfs -rm -f /ingest/*

/home/hadoop/hadoop/bin/hdfs dfs -put /home/hadoop/landing/* /ingest

