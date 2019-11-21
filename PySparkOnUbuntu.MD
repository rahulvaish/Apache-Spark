### Installation of PySpark on Ubuntu.
<hr>

##### STEP#0: Prerequisites:

###### [1] Get Java8:

<pre>
[1] <b>Download:</b> jdk-8u221-linux-x64.tar.gz from Oracle's Official Website.
[2] <b>Untar:</b> tar -xzvf jdk-8u221-linux-x64.tar.gz
[3] <b>Execute:</b> mv jdk1.8.0_221 /usr/local/java
[4] <b>Edit ~/.bashrc file with below Environental Variables:</b>
    - export JAVA_HOME=/usr/local/java/jdk1.8.0_221
    - export PATH=$PATH:/usr/local/java/jdk1.8.0_221/bin
[7] <b>Execute:</b> source ~/.bashrc
[8] <b>Execute:</b> java -version
</pre>

###### [2] Get PIP:
```
sudo apt install python3-pip
```
###### [3] Get Py4J:
```
sudo pip3 install py4j
```
###### [4] Get Jupyter Notebook:
```
sudo apt install jupyter-notebook
```

##### STEP#1: Download Apache Spark.
```
sudo wget http://mirrors.estointernet.in/apache/spark/spark-2.3.4/spark-2.3.4-bin-hadoop2.7.tgz 
```
##### STEP#2: Untar Apache Spark.
```
tar -zxvf spark-2.3.4-bin-hadoop2.7.tgz 
```
##### STEP#3: Move the unzipped file in the desired location. In my case I created /usr/local/spark [with permissions]:
```
mv spark-2.3.4-bin-hadoop2.7 /usr/local/spark
```
##### STEP#4: Modify the .bashrc file.
```
sudo gedit ~/.bashrc 
```
#####  In the .bashrc file, mention the below lines [in the end] and save it:  </br>
```
export JAVA_HOME=/usr/local/java/jdk1.8.0_221
export PATH=$PATH:/usr/local/java/jdk1.8.0_221/bin

export SPARK_HOME=/usr/local/spark/spark-2.3.4-bin-hadoop2.7
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
export SPARK_CONF_DIR=$SPARK_HOME/conf/
export PYSPARK_PYTHON=python3

# export PYTHONPATH=$SPARK_HOME/python/:$PYTHONPATH
# export PYTHONPATH=$SPARK_HOME/python/lib/py4j-0.10.7-src.zip:$PYTHONPATH

# ------------------------------------------------------------------------------------------------------
# If you have to launch Pyspark as Notebook, set the below property:
# [1] export PYSPARK_DRIVER_PYTHON=jupyter-notebook
# If you want to launch Pyspark on shell:
# [1] Comment the below property:
#     #export PYSPARK_DRIVER_PYTHON=jupyter-notebook
# [2] Execute the below command on terminal:
#     unset PYSPARK_DRIVER_PYTHON
#-------------------------------------------------------------------------------------------------------
 
```
##### STEP#5: Apply the changes.
```
source ~/.bashrc 
```

##### STEP#6: Advanced Cofiguration Changes:
<pre>
[1] <b>Navigate:</b> /usr/local/spark/spark-2.3.4-bin-hadoop2.7/conf/
[2] <b>Execute:</b> cp spark-defaults.conf.template spark-defaults.conf.sh
[3] <b>Edit spark-defaults.conf.sh with below property:</b>
    - spark.master                     spark://localhost:7077

-----------------------------------------------------------------------

[1] <b>Navigate:</b> /usr/local/spark/spark-2.3.4-bin-hadoop2.7/conf/
[2] <b>Execute:</b> cp spark-env.sh.template spark-env.sh
[3] <b>Edit spark-env.sh with below properties:</b>
    - JAVA_HOME=/usr/local/java/jdk1.8.0_211
    - SPARK_MASTER_HOST=localhost
    - SPARK_MASTER_PORT=7077
    - YARN_CONF_DIR=/usr/local/hadoop/hadoop-3.2.0/etc/hadoop
    - HADOOP_CONF_DIR=/usr/local/hadoop/hadoop-3.2.0/etc/hadoop
</pre>

##### STEP#7: Launch Spark Shell:
```
spark-shell  
```
##### STEP#8: Launch PySpark:
```
pyspark 
```
