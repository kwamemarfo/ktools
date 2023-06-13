#!/usr/bin/env python
# coding: utf-8

class Auth:
    
    def __init__(self, pwd):
        self.password = pwd
        self.username = get_ipython().getoutput('cat ~/.k5login')
        
    def check_pass(self):
        self.password_check = get_ipython().getoutput('echo $self.password | kinit `cat ~/.k5login`')
        if "kinit: Password incorrect" in self.password_check[-1]:
            print(self.password_check)
            self.password_check = None
        return self.password_check
        
    def check_spark(self):
        if "spark" in dir():
            return spark
        else:
            return
        
    
    def authenticate(self):
        if self.password_check is None:
            return
        import os
        import sys
        os.environ["SPARK_HOME"]='/opt/cloudera/parcels/SPARK3-3.1.1.3.1.7290.3-87-1.p0.16457961/lib/spark3'
        os.environ["HADOOP_CONF_DIR"]='/etc/hadoop/conf/'
        os.environ["PYLIB"]=os.environ["SPARK_HOME"]+"/python/lib"
        os.environ["PYSPARK_PYTHON"]=sys.executable
        os.environ["PYSPARK_DRIVER"]=sys.executable
        os.environ["PYSPARK_SUBMIT_ARGS"]='--deploy-mode client pyspark-shell'
        os.environ["SPARK_MAJOR_VERSION"]="3"
        os.environ["JAVA_HOME"]="/usr/java/jdk1.8.0_191/jre"
        os.environ['KRB5CCNAME']= '/tmp/krb5cc_'+ str(os.getuid())
        sys.path.insert(0,os.environ["PYLIB"]+"/py4j-0.10.9-src.zip")
        sys.path.insert(0,os.environ["SPARK_HOME"]+"/python/lib/pyspark.zip")
        
        import pyspark
        from pyspark.sql import SparkSession
        app_name = self.username[0].split("@")[0]
        pyspark.__version__
        spark = SparkSession.builder.master("yarn").appName(f"{app_name}")                            .config("spark.port.maxRetries", 100)                            .config("spark.ui.port", (4040 + int(self.username[0][4:8])))                            .config("spark.cores.max", "4")                            .config("spark.yarn.queue", "da")                            .enableHiveSupport().getOrCreate()

        return spark
        


