# Code Sample

This coding sample uses Apache Spark to convert a csv of transactions into a linked-list based on user-id and timestamp


input:
```
<event id>,<timestamp>,<session-id>,<page url>
```

output:
```
<event id>,<timestamp>,<session-id>,<page url>,<next-event-id>
```



## Build Versions

For this coding test I used Spark 1.6.0 with Scala 2.11.5.

## Building/Loading to Docker


This app was built using assembly, so to compile/package simply run
```
sbt clean assembly
```

This will create a CodingExample jar at `./target/scala_2.10/CodingExample-assembly-1.0.jar`

Once you have the jar, simply build the image with the following command:
```
docker build -t danielimberman/dsc-spark .
```

I personally chose to not use the sbt docker plugin, as I have had problems with integrating it with spark in the past.


## Running the Test

To run the jar locally, simply run the provided docker-compose file:

```bash
docker-compose up -d
```

This will run a local instance that already comes with the project jar at `/app/CodingExample.jar` and the provided dataset at `/app/data.csv`.



Once the instance is running, use the following command to log in to the instance's bash shell:
```bash
docker exec -it codingexample_master_1 /bin/bash
```

Finally, run the spark-submit command. I have provided a pre-baked command that will read from `/app/data.csv` and will write to `/app/output.csv`

```bash
 spark-submit --master local\
 	--num-executors 1\
 	--driver-memory 2g\
    --executor-memory 1g\
    --executor-cores 1\
    --class com.code_example.codetest.TransactionLinker\
    /app/CodingExample.jar\
    /app/input.csv\
    /app/output.csv
```

Since this example is only for a coding challenge, I've set the program to simply collect the data and output to a single file.

## Retrieving Data
To Retrieve your data, perform a docker cp on the master-node
```
docker cp codingexample_master_1:/app/output.csv .
```

## Running Custom Datasets
To run a custom dataset, simply copy your data into the docker image using docker cp

```
docker cp <local file> dcodingexample_master_1:</path/to/data/> .
```

Once the data is in the image, just follow the `Running The Test` instructions, replacing  the input and output paths.

```bash
 spark-submit --master local\
 	--num-executors 1\
 	--driver-memory 2g\
    --executor-memory 1g\
    --executor-cores 1\
    --class com.DollarShave.codetest.TransactionLinker\
    /app/DollarShaveTest.jar\
    <custom input file>\
    <custom output file>
```
