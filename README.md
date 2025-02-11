
# WordCount-Using-MapReduce-Hadoop

This repository is designed to test MapReduce jobs using a simple word count dataset.

## Objectives

By completing this activity, students will:

1. **Understand Hadoop's Architecture:** Learn how Hadoop's distributed file system (HDFS) and MapReduce framework work together to process large datasets.
2. **Build and Deploy a MapReduce Job:** Gain experience in compiling a Java MapReduce program, deploying it to a Hadoop cluster, and running it using Docker.
3. **Interact with Hadoop Ecosystem:** Practice using Hadoop commands to manage HDFS and execute MapReduce jobs.
4. **Work with Docker Containers:** Understand how to use Docker to run and manage Hadoop components and transfer files between the host and container environments.
5. **Analyze MapReduce Job Outputs:** Learn how to retrieve and interpret the results of a MapReduce job.

## Setup and Execution

### 1. **Start the Hadoop Cluster**

Run the following command to start the Hadoop cluster:

```bash
docker compose up -d
```

### 2. **Build the Code**

Build the code using Maven:

```bash
mvn install
```

### 3. **Move JAR File to Shared Folder**

Move the generated JAR file to a shared folder for easy access:

```bash
mv target/*.jar shared-folder/input/code/
```

### 4. **Copy JAR to Docker Container**

Copy the JAR file to the Hadoop ResourceManager container:

```bash
docker cp shared-folder/input/code/<your-jar-file>.jar resourcemanager:/opt/hadoop-3.2.1/share/hadoop/mapreduce/
```

### 5. **Move Dataset to Docker Container**

Copy the dataset to the Hadoop ResourceManager container:

```bash
docker cp shared-folder/input/data/input.txt resourcemanager:/opt/hadoop-3.2.1/share/hadoop/mapreduce/
```

### 6. **Connect to Docker Container**

Access the Hadoop ResourceManager container:

```bash
docker exec -it resourcemanager /bin/bash
```

Navigate to the Hadoop directory:

```bash
cd /opt/hadoop-3.2.1/share/hadoop/mapreduce/
```

### 7. **Set Up HDFS**

Create a folder in HDFS for the input dataset:

```bash
hadoop fs -mkdir -p /input/dataset
```

Copy the input dataset to the HDFS folder:

```bash
hadoop fs -put ./input.txt /input/dataset
```

### 8. **Execute the MapReduce Job**

Run your MapReduce job using the following command:

```bash
hadoop jar /opt/hadoop-3.2.1/share/hadoop/mapreduce/<your-jar-file>.jar com.example.controller.Controller /input/dataset/input.txt /output
```

### 9. **View the Output**

To view the output of your MapReduce job, use:

```bash
hadoop fs -cat /output/*
```

### 10. **Copy Output from HDFS to Local OS**

To copy the output from HDFS to your local machine:

1. Use the following command to copy from HDFS:
    ```bash
    hdfs dfs -get /output /opt/hadoop-3.2.1/share/hadoop/mapreduce/
    ```

2. use Docker to copy from the container to your local machine:
   ```bash
   exit 
   ```
    ```bash
    docker cp resourcemanager:/opt/hadoop-3.2.1/share/hadoop/mapreduce/output/ shared-folder/output/
    ```
3. Commit and push to your repo so that we can able to see your output

# Report
## Overview:
This project uses Hadoop MapReduce to count the occurances of words in a document.
## Implimentation:
The input to the job is a text file full of words.
The first step in the process is splitting. The document is divided into a specified number of chunks, each to be processed on a node.
Mapping is performed, where each word in the chunk is split and mapped to the number 1.
The reducer is called, which takes each key-value pair, and performs summation over the keys. This provides us with a corresponding word-count pair.
## Execution:
How to build and run the project:
0. Build the Project: `mvn install`
0. Move the jar into the shared folder: `mv target/WordCountUsingHadoop-0.0.1-SHAPSHOT.jar shared-folder/input/code`
0. Copy the jar into the container: `docker cp shared-folder/input/code/WordCountUsingHadoop-0.0.1-SNAPSHOT.jar resourcemanager:/opt/hadoop-3.2.1/share/hadoop/mapreduce/
0. Copy the input files into the container.
0. Inside the container, create an input directory: `hadoop fs -mkdir -p /input/dataset`
0. Copy input data from container to hdfs: `hadoop fs -put ./input.txt /input/dataset/`
0. Run the Job: `hadoop har WordCountUsingHadoop-0.0.1-SNAPSHOT.jar com.example.controller.Controller /input/dataset/input.txt /output`
0. Output will be in the hdfs /output directory.
## Challenges:
The biggest difficulty in this assignment is understanding the flow of execution in mapreduce.
There isnt much good documentation on this process, and it is not easy to step through and debug.
Therefore, it will take a lot of getting used to before I understand how to come up more complex jobs.
## Sample:
For the input:
```
Hello, this is an example
this example is very helpful
```
Should produce ouput:
```
Hello, 1
this 2
is 2
an 1
example 2
very 1
helpful 1
```