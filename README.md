# htx\_technical

# HTX Technical Submission

This document outlines the steps required to set up the environment, compile the project, and execute the main application.

---

## 1. Setting Up the Environment

The following steps prepare your system with the necessary dependencies:

1.  **Run `setup.sh`**: Execute the provided `setup.sh` script to set up **Scala**.

2.  **Windows Environment Setup (If Applicable)**: If you're running on **Windows**, ensure the following environment variables are set. The `hadoop 3.3.6` folder and its `bin` file are provided in the repository.
    * Set **`HADOOP_HOME`** to `"hadoop 3.3.6"`.
    * Append the Hadoop binary directory to your **`PATH`**:
        ```bash
        PATH=%PATH%;%HADOOP_HOME%\bin
        ```

3.  **Java and Spark Check**: Verify that **Java 17** and a compatible version of **Spark** are correctly set in your system's **`PATH`**.

4.  **Compile the Project**: Navigate to the project directory in your command line and run the following command to compile the project:
    ```bash
    sbt clean compile
    ```

---

## 2. Generating Datasets (Optional)

You can manually generate randomized input datasets (**Datasets A and B**) to serve as inputs.

* Run the following command from the project directory:
    ```bash
    sbt "runMain GenerateParquet"
    ```

---

## 3. Creating the Executable Assembly

1.  Create a self-contained JAR file (assembly) for deployment by running the following command:
    ```bash
    sbt clean assembly
    ```

---

## 4. Running the Spark Job

Use the **`spark-submit`** command to execute the main application, `com.htx.ItemRanking`.

### Configuration Details

| Parameter                             | Value                                                         |
|:--------------------------------------|:--------------------------------------------------------------|
| **`--master`**                        | `local[*]`                                                    |
| **`--driver-memory`**                 | `4g`                                                          |
| **`--executor-memory`**               | `8g`                                                          |
| **`--executor-cores`**                | `4`                                                           |
| **`spark.default.parallelism`**       | `1000`                                                        |
| **`spark.serializer`**                | `org.apache.spark.serializer.KryoSerializer`                  |
| **`spark.kryoserializer.buffer.max`** | `512m`                                                        |
| **`--class`**                         | `com.htx.ItemRanking`                                         |
| **JAR Path**                          | `target/scala-2.13/htx_technical-assembly-0.1.0-SNAPSHOT.jar` |
| **Input A Path**                      | `data/input/dataset_a.parquet`                                |
| **Input B Path**                      | `data/input/dataset_b.parquet`                                |
| **Output Path**                       | `data/output/output.parquet`                                  |
| **Top N Items**                       | `10`                                                          |

The paths for the inputs, output, as well as the number of top items (10 in the above) can be changed.

### Execution Command

Run the application with the following command:

```bash
spark-submit \
    --master local[*] \
    --driver-memory 4g \
    --executor-memory 8g \
    --executor-cores 4 \
    --conf spark.default.parallelism=1000 \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf spark.kryoserializer.buffer.max=512m \
    --class com.htx.ItemRanking \
    target/scala-2.13/htx_technical-assembly-0.1.0-SNAPSHOT.jar \
    data/input/dataset_A.parquet \
    data/input/dataset_B.parquet \
    data/output/output.parquet \
    10
```

Single line:
```
 spark-submit --master local[*] --driver-memory 4g --executor-memory 8g --executor-cores 4 --conf spark.default.parallelism=1000 --conf spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.kryoserializer.buffer.max=512m --class com.htx.ItemRanking target/scala-2.12/htx_technical-assembly-0.1.0-SNAPSHOT.jar data/input/dataset_a.parquet data/input/dataset_b.parquet data/output/output.parquet 10
```


### KNOWN ISSUES

The integration test fails when run with the unit tests. It passes when run on its own.

