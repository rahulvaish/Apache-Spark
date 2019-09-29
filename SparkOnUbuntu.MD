### Installation of Spark on Ubuntu.
<hr>

##### STEP#0: Get Java8:

```
[1] Download jdk-8u221-linux-x64.tar.gz from the Official Website.
[2] Extract jdk-8u221-linux-x64.tar.gz:
    tar -xzvf jdk-8u221-linux-x64.tar.gz
[3] Move Extracted folder (jdk1.8.0_221) to a desired location. In my case I created /usr/local/java [with permissions]:
    mv jdk1.8.0_221 /usr/local/java
[4] Open ~/.bashrc file to set the Environmental Varibles:
    sudo gedit ~/.bashrc	
[5] In the file, mention the below two lines (in the end):
    export JAVA_HOME=/usr/local/java/jdk1.8.0_221
    export PATH=$PATH:/usr/local/java/jdk1.8.0_221/bin
[6] Save the file.
[7] Publish the changes:
    source ~/.bashrc
[8] Check the Java Version: 
    java -version
```

##### STEP#1: Download Apache Spark.
```
sudo wget http://mirrors.estointernet.in/apache/spark/spark-2.3.4/spark-2.3.4-bin-hadoop2.7.tgz 
```
##### STEP#2: Unzip Apache Spark.
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
export SPARK_HOME=/usr/local/spark/spark-2.3.4-bin-hadoop2.7 
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin  
export SPARK_CONF_DIR=$SPARK_HOME/conf/  
```
##### STEP#5: Apply the changes.
```
source ~/.bashrc 
```
##### STEP#6: Launch Spark Shell.
```
spark-shell  
```
