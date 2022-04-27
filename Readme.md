## Airline Flights Data Analysis: Find all the information about cancelled and delayed. 

### How to run program In Local Standalone Mode:


1. First Clone this repository ``git clone https://github.com/adnanrahin/Spark-Flights-Data-Analysis.git``
2. Download the Data set from Kaggle: [Data Source](https://www.kaggle.com/usdot/flight-delays)
3. After Downloading the dataset go to root of this repository ``~/Spark-Flight-Data-Analysis``
4. Do ``mvn clean install``.
5. If you are using Intellij or Eclipse pass an arguments variable where downloaded datasets are.
6. If you are not running spark standalone cluster simply Compile and Run Should Spin up the program.

### Launching Applications with spark-submit
```
./bin/spark-submit \
  --class <main-class> \
  --master <master-url> \
  --deploy-mode <deploy-mode> \
  --conf <key>=<value> \
  ... # other options
  <application-jar> \
  [application-arguments]
  
  spark-submit \
  --class org.flight.analysis.FlightDelaysAndCancellations \
  --master spark://ip_address(master):7077 \
  --deploy-mode cluster \
  --executor-memory 32G \
  --total-executor-cores 24 \
  --driver-memory 32G \
  --driver-cores 24 \
  spark-flights-data-analysis\target\spark-flights-data-analysis-1.0-SNAPSHOT.jar
  spark-flights-data-analysis\2015_flights_data\
```
### Running the program in Docker Container:


1. First Clone this repository ``git clone https://github.com/adnanrahin/Spark-Flights-Data-Analysis.git``
2. Download the Data set from Kaggle: [Data Source](https://www.kaggle.com/usdot/flight-delays)
3. 4. Do ``mvn clean install``.
4. Put data-set to the directory name ``2015_flights_data``
5. Build the Docker image: ``docker build -t cluster-apache-spark:3.0.2 .`
6. Run `docker-compose up -d` to start the docker container.
7. Once docker-compose is up and running, it will spin up the one master node and two worker node with bellow name
```
** Master: spark-flights-data-analysis_spark-master_1
** Worker: spark-flights-data-analysis_spark-worker-a_1
** Worker: spark-flights-data-analysis_spark-worker-c_1
```
8. Run this command to start interactive docker container mode: ``docker exec -it spark-flights-data-analysis_spark-master_1 /bin/bash``
9. Run the program in the interactive shell: 
```
./opt/spark/bin/spark-submit \
  --class org.flight.analysis.FlightDelaysAndCancellations \
  --master spark://spark-master:7077 \
  --deploy-mode cluster \
  --executor-memory 32G \
  --total-executor-cores 24 \
  --driver-memory 32G \
  --driver-cores 24 \
  /opt/spark-apps/spark-flights-data-analysis-1.0-SNAPSHOT.jar
  /opt/spark-data/
```


#### 1. Find all the flight that cancelled in 2015
```
+-------+----------+-------------+------------------+-------------------+
|airline|tailNumber|originAirport|destinationAirport|cancellationsReason|
+-------+----------+-------------+------------------+-------------------+
|     AS|    N431AS|          ANC|               SEA|                  A|
|     AA|    N3BDAA|          PHX|               DFW|                  B|
|     OO|    N746SK|          MAF|               IAH|                  B|
|     MQ|    N660MQ|          SGF|               DFW|                  B|
|     OO|    N583SW|          RDD|               SFO|                  A|
+-------+----------+-------------+------------------+-------------------+
only showing top 5 rows
```

#### 2. Find total of number flights cancelled byt Airline Name
```
+----------------------------+----------------------------------+
|Airline Names               |Total Number Of Flight's Cancelled|
+----------------------------+----------------------------------+
|United Air Lines Inc.       |6573                              |
|Virgin America              |534                               |
|US Airways Inc.             |4067                              |
|American Eagle Airlines Inc.|15025                             |
|Southwest Airlines Co.      |16043                             |
|Delta Air Lines Inc.        |3824                              |
|Skywest Airlines Inc.       |9960                              |
|Spirit Air Lines            |2004                              |
|Alaska Airlines Inc.        |669                               |
|American Airlines Inc.      |10919                             |
|Hawaiian Airlines Inc.      |171                               |
|Atlantic Southeast Airlines |15231                             |
|JetBlue Airways             |4276                              |
|Frontier Airlines Inc.      |588                               |
+----------------------------+----------------------------------+
```

#### 3. Find total number flights Depart from Airport by Airport IATA CODE
```
+---------------------------------------+------------------------+
|Airport Name                           |Total Number of Flight's|
+---------------------------------------+------------------------+
|LaGuardia Airport (Marine Air Terminal)|95074                   |
+---------------------------------------+------------------------+
```

#### 4. Find most cancelled Airline Name and total Number of Cancelled Flights
```
+----------------------+------------------------+
|Airline Name          |Total Number of Flight's|
+----------------------+------------------------+
|Southwest Airlines Co.|16043                   |
+----------------------+------------------------+
```

#### 5. Find Average delayed by Airliner
```
+----------------------------+------------------+
|Airline Name                |Average Delay     |
+----------------------------+------------------+
|US Airways Inc.             |3.553065019933418 |
|American Eagle Airlines Inc.|6.197287621554539 |
|Atlantic Southeast Airlines |6.3779928369489856|
|Hawaiian Airlines Inc.      |6.511530728899752 |
|Skywest Airlines Inc.       |6.522445811066178 |
|Southwest Airlines Co.      |6.575752200171454 |
|JetBlue Airways             |6.608200264868403 |
|Alaska Airlines Inc.        |6.608791285524754 |
|Delta Air Lines Inc.        |6.629987489349894 |
|United Air Lines Inc.       |6.6460316213296675|
|Spirit Air Lines            |6.72094474539545  |
|Virgin America              |6.727663804200818 |
|Frontier Airlines Inc.      |6.748626008332595 |
|American Airlines Inc.      |7.330276268590967 |
+----------------------------+------------------+
```

#### 6. Find the total distance flown each airline in 2015

```
+----------------------------+--------------+
|Airline Names               |Total Distance|
+----------------------------+--------------+
|Southwest Airlines Co.      |924523336     |
|American Airlines Inc.      |745699296     |
|Delta Air Lines Inc.        |744626128     |
|United Air Lines Inc.       |647702424     |
|Skywest Airlines Inc.       |288044533     |
|JetBlue Airways             |279708698     |
|Atlantic Southeast Airlines |257284318     |
|Alaska Airlines Inc.        |206001530     |
|US Airways Inc.             |178393656     |
|American Eagle Airlines Inc.|118297439     |
|Spirit Air Lines            |113749628     |
|Frontier Airlines Inc.      |87302103      |
|Virgin America              |86275456      |
|Hawaiian Airlines Inc.      |48160938      |
+----------------------------+--------------+
```
#### Find Destination and origin airports that has maximum distance
```
+----------------------------------------------------------------------+------------------------------+--------------+
|Source Airport                                                        |Destination Airport           |Total Distance|
+----------------------------------------------------------------------+------------------------------+--------------+
|John F. Kennedy International Airport (New York International Airport)|Honolulu International Airport|4983          |
+----------------------------------------------------------------------+------------------------------+--------------+

```
