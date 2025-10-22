# htx\_technical

Submission for HTX technical



Step 1: Setting up the environment



Run setup.sh to set up Scala.



If you are running this on Windows, you will need to set the following in your environmental variables, if not already done:

HADOOP\_HOME="hadoop 3.3.6"

PATH=%PATH%;%HADOOP\_HOME%\\bin



hadoop 3.3.6 and its bin file are provided in the repository.



Ensure that Java 17 and a compatible version of Spark are set in the PATH.



Subsequently, compile the project by running "sbt clean compile" in the command line from the project directory.



Step 2 (Optional): Manually generate randomised Datasets A and B to serve as inputs with the command: sbt "runMain GenerateParquet"



Step 3: Run "sbt clean assembly" in the command line



Step 4: Run the following in the command line:

spark-submit \\

  --master local\[\*] \\

  --driver-memory 4g \\

  --executor-memory 8g \\

  --executor-cores 4 \\

  --conf spark.default.parallelism=1000 \\

  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \\

  --conf spark.kryoserializer.buffer.max=512m \\

  --class com.htx.ItemRanking \\

  target/scala-2.13/htx\_technical-assembly-0.1.0-SNAPSHOT.jar \\

  data/input/dataset\_A.parquet \\

  data/input/dataset\_B.parquet \\

  data/output/output.parquet \\

  10



The paths for the inputs, output, as well as the number of top items (10 in the above) can be changed.



Single line: spark-submit --master local\[\*] --driver-memory 4g --executor-memory 8g --executor-cores 4 --conf spark.default.parallelism=1000 --conf spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.kryoserializer.buffer.max=512m --class com.htx.ItemRanking target/scala-2.13/htx\_technical-assembly-0.1.0-SNAPSHOT.jar data/input/dataset\_A.parquet data/input/dataset\_B.parquet data/output/output.parquet 10



KNOWN ISSUES: The integration test fails when run with the unit tests. It passes when run on its own.

