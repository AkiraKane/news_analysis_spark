Instructions
============

1. Launch cloudformation.
2. SSH into the instance using the ssh imformation provided in EMR console.
3. Launch the zepplin browser from the console.
4. In zeppelin, under Interpreter > Spark set the following config options (see [this stackoverflow question](http://stackoverflow.com/questions/37871194/how-to-tune-spark-executor-number-cores-and-executor-memory) and [this cloudera post](http://blog.cloudera.com/blog/2015/03/how-to-tune-your-apache-spark-jobs-part-2/) to see how to best customize these values.)
    * spark.executor.instances 18
    * spark.executor.cores 5
    * spark.executor.memory 16g
5. When tuning these values, note that yarn will allocate something like 80% of the system's ram to itself, so this is the total ram you want to target. Furthermore, the value of spark.executor.memory will actually 10-20\% higher than specified, but the amount of memory actually available in each container can be up to 40% lower. This may have something to do with how I am loading the jars here, I'm not sure. Anyways, you should assume some significant memory overhead when picking these values. 
5. Inside the instance you have ssh into, find the file capacity-scheduler.xml. It should be located in /etc/hadoop/conf.empty.
6. In capacity-scheduler.xml there is an xml property that looks like this:

```
  <property>
    <name>yarn.scheduler.capacity.resource-calculator</name>
    <value>org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator</value>
    <description>
      The ResourceCalculator implementation to be used to compare
      Resources in the scheduler.
      The default i.e. DefaultResourceCalculator only uses Memory while
      DominantResourceCalculator uses dominant-resource to compare
      multi-dimensional resources such as Memory, CPU etc.
    </description>
  </property>
```
This should be edited so that instead of "DefaultResourceCalculator" it says "DominantResourceCalculator"
7. In yarn-site.xml change 

```
  <property>
    <name>yarn.nodemanager.resource.memory-mb</name>
    <value>12288</value>
  </property>
```
So that it reflects the amount of memory your want to allocate to your executors in this case (64512).

9. in yarn-site.xml change

```
  <property>
    <name>yarn.nodemanager.resource.cpu-vcores</name>
    <value>8</value>
  </property>
```
to equal the number of cores on your machine - 1. 

9. Verify that the memory usage listed in the hadoop gui is near maximum and that the total number of containers should equal the number you specified + 1. If you don't have enough containers, you probably set the mem/container too high. However, ignore the fact that the VCore usage is too low - the hadoop gui doesn't know the real number and just subs in 1/container as a default.
10. Verify in the spark gui that things are operating as planned - you can access it by clicking on the links in the hadoop gui for the job that you are running. Note that the links in the gui are broken - they links relative to the master node and not the internet. You will need to substitute in the ec2 url for the internal url. 