## How to run it

1. ./gradlew clean uberjar
2. spark-submit --class CsvAnalyzer --master local[2] build/libs/o3-lab-1.0-SNAPSHOT.jar ZipData.csv MM/dd/yy
