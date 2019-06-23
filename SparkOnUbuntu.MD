### Installation of Spark on Ubuntu.
<hr>

###### STEP#1: Download Apache Spark.
```
sudo wget http://mirrors.estointernet.in/apache/spark/spark-2.4.3/spark-2.4.3-bin-hadoop2.7.tgz 
```
###### STEP#2: Unzip Apache Spark.
```
tar xvf spark-2.4.3-bin-hadoop2.7.tgz 
```
###### STEP#3: Move the unzipped file in the desired location.
```
mv spark-2.4.3-bin-hadoop2.7.tgz /usr/local/spark
```
###### STEP#4: Modify bashrc file.
```
sudo gedit ~/.bashrc 
```
######  In the file, mention the below lines [in the end] and save it:  </br>
         export SPARK_HOME=/usr/local/spark/spark  </br>
         export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin  </br>
         export SPARK_CONF_DIR=$SPARK_HOME/conf/  </br>
###### STEP#5: Apply the changes.
```
source ~/.bashrc 
```
###### STEP#6: Launch Spark Shell.
```
spark-shell  
```
