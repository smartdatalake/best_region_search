The BRS service is available via a REST API receiving input parameters in JSON format and returning the locations of detected regions in GeoJSON format. By default, the BRS service listens on port 4646, and it is configurable at installation phase. The input data must exist in RAW as a materialized view, and it must be accessible via the user's Proteus JDBC connector. The input parameters and API functions are as follows:

/alive: it checks the status of the BRS service. It has no input parameter.

/changeAlgo: this request selects the BRS algorithm among single round, multi round, hybrid, and uniform grid. The default is hybrid.

algo: 0 for multi-round, 1 for single-round, 2 for hybrid, and 9 for uniform grid. 

/changeMemorySize: it sets the size of RAM assigned for submitting the BRS over the Spark cluster. Default is 10 GB.

memorySize: it indicates the new memory size in GB.

/BRS: this method returns the top k regions that maximize a monotonic scoring function. It is possible for the user to change the source code and separately define the function. By default, it is maximizing the summation of an attribute of items inside regions or frequency of items. The input parameters and their values are sent in JSON format, and the output is the center of top regions formatted as GeoJSON. As the BRS receives a query, it contacts the Proteus server to fetch required columns from the target table, so the table must be accessible from the Proteus instance. The names and definitions of input parameters are:

table: It is the name of a materialized view that contains columns named lat and lon indicating a geographical location on map.

topk: It indicates the number of top regions that the user is looking for.

eps: It is the length of the target region side in radian, where 1 radian is around 100km.

f: it is a column name used for scoring function; “null” uses item frequency as scoring function. 

dist: if true, it detects non-overlapping regions, otherwise it returns overlapping ones.

keywordsColumn: it is a column name for filtering the input records. If “null”, no filtering is applied.

keywords: it is composed of values which are separated with semicolons and is used for filtering an input record.

keywordsColumn2: it is the second column name for additional filtering of the input records.

keywords2: it is values for the second filtering column

/removeTables: BRS stores the input table to avoid re-accessing the original data from Proteus. Calling this request removes buffered tables. No input parameter is required.

/flushBuffer: BRS buffers previous results to avoid repeating the same query. This request flushes the buffered results. No input parameter is required.

/changeProteus: To execute a query, BRS requires a running Proteus server to fetch the proper data. Input parameters are:

ProteusURL: it is an address to a running Proteus server

ProteusUsername: it is a username to the Proteus server

ProteusPassword: it is a password for the Proteus username

#### Installation and usage
The BRS component is implemented in Scala, and it runs on top of Apache Spark. The final version is available as a Docker image hosting Spark instance and as a stand-alone jar file. The BRS service consists of two components: (a) SDL.main, which is a REST API manager, (b) SDL.Run, which is the BRS application and is submittable to a Spark cluster. Next, we first describe how to fetch the source code, install required dependencies, prepare the configuration files, and submit the stand-alone jar file to a Spark cluster for execution. Then, we present the instructions for running the BRS service inside a Docker image.

###### Stand-alone installation
The source code is available on the GitHub repository of SmartDataLake. To make a stand-alone jar file, the following steps are required:

1) Install Git, Java, Scala, and SBT.
2) Clone or download the BRS project from the source code.
3) Package and assembly the source project with SBT.
4) Go to project_directory/target/scala-2.11, and get the executable jar file named BRS_REST_API.jar
5) Run a Spark cluster and Proteus server.
6) Create an empty buffer.tmp in the same folder of BRS_REST_API.jar. This file stores the results of previously submitted queries.
7) Create a JSON file named conf.txt in the same folder of BRS_REST_API.jar which contains configurations as shown below:

{

“sparkSubmit” : path to spark-submit.sh of the Spark cluster,

“SparkMaster” : IP of the Spark cluster,

“partitionCNT” : number of partitions to distribute BRS space among workers, varying from 1 to 100000.

“algo” : set default BRS algorithm,

“dataPath” : path to store input tables,

“delimiter” : let it be ',',

“ProteusJDBC_URL”: set default address to the Proteus server,

“ProteusUsername” : set Proteus default username,

“ProteusPassword”: set password for the username,

“port” : port for listening to REST APIs. It cannot be changed after the installation.

}

8) In command line do:

java -cp BRS_REST_API.jar SDL.main.main

9) The BRS REST API is listening on the port configured in conf.txt. Check the service by sending a GET request at localhost:port/alive
10) Send your BRS query as a GET request to localhost:port/BRS.

###### Docker image installation
The BRS service requires a running Spark cluster to execute the stand-alone jar file. To make it easier for the user, we provide a Docker image hosting a Spark cluster and inject the BRS service inside this image. After executing and deploying the image, the BRS service exposed over a desired port, will be ready as a REST API. To prepare the Docker image, the following steps should be performed:
1) Generate the stand-alone jar file named BRS_REST_API.jar
2) In the same folder, put the jar file avatica-1.13.0.jar which is a library for Proteus JDBC connector, empty buffer.tmp, and conf.txt 
3) Set conf.txt as below:

{

“sparkSubmit” : “spark-submit”,

“SparkMaster” : “localhost”,

“partitionCNT” : 36,

“algo” : 2,

“dataPath” : ”/opt/spark-2.4.4-bin-hadoop2.7/local/BRS_DATA/”,

“jarPath” : “/opt/spark-2.4.4-bin-hadoop2.7/local/BRS_REST_API.jar”,

“delimiter” : ',' ,

“ProteusJDBC_URL”: set default address to the Proteus server,

“ProteusUsername” : set Proteus default username,

“ProteusPassword”: set password for the username,

"memorySize": 10,

“port” : 4646

}

4) Make a plain file named Dockerfile containing the code below:

FROM gradiant/spark:latest

RUN mkdir /opt/spark-2.4.4-bin-hadoop2.7/local/BRS_DATA

COPY BRS_REST_API.jar /opt/spark-2.4.4-bin-hadoop2.7/local/

COPY conf.txt /opt/spark-2.4.4-bin-hadoop2.7/local/

COPY buffer.tmp /opt/spark-2.4.4-bin-hadoop2.7/local/

COPY avatica-1.13.0.jar /opt/spark-2.4.4-bin-hadoop2.7/local/

EXPOSE 4646

CMD ["java", "-cp", "/opt/spark-2.4.4-bin-hadoop2.7/local/BRS_REST_API.jar", "SDL.main.main2" , ">>" , "/opt/spark-2.4.4-bin-hadoop2.7/local/out"]

5) Open a command line in the same directory and run:

docker build  -t brs:1.0

6) Deploy the image with:

docker run -d -p 127.0.0.1:4646:4646/tcp brs:1.0

7) Start a Proteus instance and add its credentials via localhost:4646/changeProteus
8) The BRS REST API is listening on port 4646. Check the service by sending a GET request to localhost:4646/alive

###### Create executable JAR file:

1) Install Git, java, scala, and SBT.
2) Clone or fork the project.
3) The executable jar file and configuration files are in the folder /executable; you can skip steps 4 and 5.
4) To make executable jar file, package and assembly the source project with SBT.
5) Go to project_directory/target/scala-2.11, and get executable BRS_REST_API.jar
6) The jar file comprises two separate main functions: main (REST API manager) and Run (Spark-executable BRS
   application).

conf.txt must contain the json below:

{

"sparkSubmit" : "path_to_spark-submit.sh_in_spark_directory",

"SparkMaster" : "IP_of_spark_cluster",

"partitionCNT" : numberOfPartitions_varying_from_1_to_100000,

"algo" : indicating_the_algorithm_varying_0 (for multiRound)_1 (for singleRound)_2 (for hybrid)_9 (for uniformGriding),

"dataPath" : "path_to_store_tables",

"delimiter" : ',',

"ProteusJDBC_URL":"jdbc:avatica:remote:url=http://diascld32.iccluster.epfl.ch:18007;serialization=PROTOBUF",

"ProteusUsername" : "user",

"ProteusPassword":"pass",

"port" : portForRestAPI

}

