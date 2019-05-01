#### Get winutils.exe from here : https://github.com/steveloughran/winutils/blob/master/hadoop-2.7.1/bin/winutils.exe
![image](https://user-images.githubusercontent.com/45539698/56249220-93265780-60c8-11e9-89a5-b276f5620ebf.png)
![image](https://user-images.githubusercontent.com/45539698/56249226-9cafbf80-60c8-11e9-9aa4-af43b647969b.png)
![image](https://user-images.githubusercontent.com/45539698/56249246-adf8cc00-60c8-11e9-92d2-1ed8945d9b5d.png)
![image](https://user-images.githubusercontent.com/45539698/56249257-b8b36100-60c8-11e9-9fe8-374c482d0f0f.png)
![image](https://user-images.githubusercontent.com/45539698/56249265-c23cc900-60c8-11e9-97e2-2c4dddadb269.png)
![image](https://user-images.githubusercontent.com/45539698/56249274-ca950400-60c8-11e9-85ea-3d991b25eb01.png)
![image](https://user-images.githubusercontent.com/45539698/56857412-bd123080-698a-11e9-82af-a574f911fbcb.png)

## Additional Activity- Setting Up Scala

#### Download and Install scala and sbt:
#### [1] SCALA : The Environmental Variables sets up automatically !
![image](https://user-images.githubusercontent.com/45539698/56859653-88fb3780-69ab-11e9-8616-f9656c9d589f.png)
#### [2] SBT : The Environmental Variables sets up automatically !
![image](https://user-images.githubusercontent.com/45539698/56859910-ac73b180-69ae-11e9-9a22-c3d64d1cd620.png)


## Starting Spark in Stand-Alone Mode:

![image](https://user-images.githubusercontent.com/689226/57030702-3ea3e000-6c63-11e9-941c-762444bf9588.png)
![image](https://user-images.githubusercontent.com/689226/57031068-3c8e5100-6c64-11e9-829d-69ae46494989.png)
![image](https://user-images.githubusercontent.com/689226/57031145-7fe8bf80-6c64-11e9-948e-89f97d7087ea.png)
In my scenerio, it failed! Further, checked the logs, and then executed the below command on the terminsl:
java -cp "E:\SetUps\spark-2.4.1-bin-hadoop2.7/conf\;E:\SetUps\spark-2.4.1-bin-hadoop2.7\jars\*" -Xmx1g org.apache.spark.deploy.master.Master --host localhost --port 7077 --webui-port 8080 
![image](https://user-images.githubusercontent.com/689226/57031335-1fa64d80-6c65-11e9-933c-e0b790f4e3c6.png)
![image](https://user-images.githubusercontent.com/689226/57031549-9cd1c280-6c65-11e9-8a9c-4d3e76eed75e.png)
