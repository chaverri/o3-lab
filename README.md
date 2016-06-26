./gradlew clean uberjar
spark-submit --class CsvAnalyzer --master local[2] build/libs/o3-lab-1.0-SNAPSHOT.jar ZipData.csv
