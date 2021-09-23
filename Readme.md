## Airline Flights Data Analysis: Find all the information about cancelled and delayed. 

### Here is the link to the original data from Kaggle: [Data Source](https://www.kaggle.com/usdot/flight-delays)

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
|United Air Lines Inc.       |647702424     |
|Virgin America              |86275456      |
|US Airways Inc.             |178393656     |
|American Eagle Airlines Inc.|118297439     |
|Southwest Airlines Co.      |924523336     |
|Delta Air Lines Inc.        |744626128     |
|Skywest Airlines Inc.       |288044533     |
|Spirit Air Lines            |113749628     |
|Alaska Airlines Inc.        |206001530     |
|American Airlines Inc.      |745699296     |
|Hawaiian Airlines Inc.      |48160938      |
|Atlantic Southeast Airlines |257284318     |
|JetBlue Airways             |279708698     |
|Frontier Airlines Inc.      |87302103      |
+----------------------------+--------------+
```
