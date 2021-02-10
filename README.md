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

"ProteusUsername" : "sdlhshah",

"ProteusPassword":"Shah13563556",

"port" : portForRestAPI

}

###### Setup BRS REST API:

1) The system requires a running Spark cluster and Proteus JDBC connection.
2) Create empty buffer.tmp in the same folder of BRS_REST_API.jar. This file stores the final results of previously
   submitted queries as json.
3) Create conf.txt in the same folder of BRS_REST_API.jar which contains configurations. Please, check
   /executable/conf.txt as an example.
4) In command line do: java -cp BRS_REST_API.jar SDL.main.main
5) Topk BRS REST API is listening on the port configured in conf.txt
6) Check the REST API by sending GET request IP:port/alive
7) Get request IP:port/flushBuffer removes cached results.
8) To change Proteus server address and credential, send GET request IP:port/changeProteus with the query params below:

{

'url' : "X", // new address.

'username' : "Y", // new username.

'pass' : "Z"    // new password.

}

9) Send your BRS query as a GET request to IP:port/BRS with the query params below:

{

'topk' : topk, // number of top regions

'eps' : eps, // length of target region in a radius.

'f' : f, // name of column used as maximizing score. Let it null to use frequency as scoring function.

'input' : input, // name of a table existing in the Proteus account, that contains columns named lat and lon.

"keywordsColumn" : keywordsColumn, // let it "null" if you do not have a filter. One column is allowed.

"keywords" : keywords, // let it "null" if you do not have a filter. Separate keywords with ;

"keywordsColumn2" : "", // let it "" if you do not have second filter. One column is allows.

"keywords2" : "", // let it "" if you do not have second filter. Separate keywords with ;

"dist" : True/False // if True, regions are non-overlapping.

}


