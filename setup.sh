#!/bin/bash
# Install Scala and Spark environment

# Exit if any command fails
set -e

# Install Scala (for Ubuntu/Debian)
sudo apt-get update
sudo apt-get install -y openjdk-11-jdk scala

# Download and set up Spark 4.0.1
wget https://downloads.apache.org/spark/spark-4.0.1/spark-4.0.1-bin-hadoop3.tgz
tar xvf spark-4.0.1-bin-hadoop3.tgz
sudo mv spark-4.0.1-bin-hadoop3 /opt/spark

# Set environment variables
echo "export SPARK_HOME=/opt/spark" >> ~/.bashrc
echo "export PATH=\$PATH:\$SPARK_HOME/bin" >> ~/.bashrc
source ~/.bashrc

echo "Setup complete. Run with: sbt runMain ItemRanking <args>"
