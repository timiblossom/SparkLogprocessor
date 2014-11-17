package com.vnspot.spark.logprocessor

class Params(host : String, date : String, hour : String, resourcePath : String, logPath : String) extends Serializable {
    
    var logFiles = ""
      
    if (logPath == "" || logPath == null) {
      logFiles = "resources/seqfile" //local file
    } else {
      logFiles = logPath + "/" + date + "/" + hour + "/*"
    }
  
    println("master = " + host)
    println("dt = " + date)
    println("hr = " + hour)
    println("resourcePath = " + resourcePath)
    println("targetPath = " + logPath)
    
	def getHost() = host
    def getDate() = date
    def getHour() = hour
    def getResourcePath() = resourcePath
    def getTargetPath() = logPath 
    def getLogFiles() = logFiles
    
}