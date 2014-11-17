package com.vnspot.spark.logprocessor

import org.apache.hadoop.mapred.JobConf
import com.maxmind.geoip.LookupService
import spark.SparkContext
import spark.SparkContext._
import com.vnspot.spark.logprocessor.impl.RawEventProcessor
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat


object Driver {

  def main(args: Array[String]) {

    val SPARK_HOME = "/root/spark"
    val PROJECT_JAR: Seq[String] = Seq("target/scala-2.9.2/log-processor_2.9.2-1.0.jar", "lib/maxmindgeoip.jar")  

    if (args.length == 0) {
      System.err.println("Usage: RawEventProcessor -m <master> -rpath resourcePath  -lpath s3://datalogs -d yyyy-mm-dd -o hh")
      System.exit(1)
    }
    
    
    val arglist = args.toList
    type OptionMap = Map[Symbol, Any]

    def nextOption(map : OptionMap, list: List[String]) : OptionMap = {
      def isSwitch(s : String) = (s(0) == '-')
      list match {
        case Nil => map
        case "-m" :: value :: tail =>
                               nextOption(map ++ Map('master -> value), tail)
        case "-d" :: value :: tail =>
                               nextOption(map ++ Map('dt -> value), tail)
        case "-o" :: value :: tail =>
                               nextOption(map ++ Map('hr -> value), tail)
        case "-rpath" :: value :: tail =>
                               nextOption(map ++ Map('resourcePath -> value), tail)                       
        
        case "-lpath" :: value :: tail =>
                               nextOption(map ++ Map('logPath -> value), tail) 
                               
        /*                       
        case string :: opt2 :: tail if isSwitch(opt2) => 
                               nextOption(map ++ Map('infile -> string), list.tail)
        case string :: Nil =>  nextOption(map ++ Map('infile -> string), list.tail)
        */
        case option :: tail => println("Unknown option " + option) 
                                exit(1)
      }
    }
    
    System.setProperty("spark.default.parallelism", "30")
    System.setProperty("spark.akka.threads", "10") 
    
    val options = nextOption(Map(),arglist)
    var master : String = if (options.contains('master)) options('master).toString else null
    var dt : String =  if (options.contains('dt))  options('dt).toString else null
    var hr : String =  if (options.contains('hr)) options('hr).toString else null
    var resourcePath : String = if (options.contains('resourcePath)) options('resourcePath).toString else null
    var logPath : String = if (options.contains('logPath))  options('logPath).toString else null
    val params = new Params(master, dt, hr, resourcePath, logPath)
    val sc = new SparkContext(master, "RawEventProcessor", SPARK_HOME, PROJECT_JAR)
    
    
    val jobConf = new JobConf()
    jobConf.setJobName("SparkLogProcessor")
    jobConf.set("mapred.output.compress", "true")
    jobConf.set("mapred.output.compression.type", "BLOCK")
    jobConf.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec")

    jobConf.set("mapred.compress.map.output", "true")
    jobConf.set("mapred.map.output.compression.type", "BLOCK")
    jobConf.set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec")
    
    
    val logData = sc.sequenceFile[Text, Text](params.logFiles) 
    //val logData = sc.hadoopRDD(jobConf, SequenceFileInputFormat[Text.class, Text.class], keyClass, valueClass, minSplits)
    sc.addFile(resourcePath + "/GeoIPCity.dat")
    
    var rawData = logData.map( e => e._2.toString() )

    rawData = new RawEventProcessor(sc, params)
                   .process(rawData)
    
    rawData.saveAsTextFile("/tmp/outfile1")
    //rawData.saveAsSequenceFile("/tmp/sequencefile2")
    //rawData.saveAsTextFile("hdfs://ip:9000/mnt/mylogdata")
    rawData.map( line => (new Text(), new Text(line)))
    
    println("Done!")
    System.exit(0)
  }


}




