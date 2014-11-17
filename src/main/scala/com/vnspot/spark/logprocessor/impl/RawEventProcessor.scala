package com.vnspot.spark.logprocessor.impl

import java.io.File

import com.maxmind.geoip.Location
import com.maxmind.geoip.LookupService
import com.vnspot.spark.logprocessor.Config
import com.vnspot.spark.logprocessor.Params

import com.vnspot.spark.logprocessor.Config
import com.vnspot.spark.logprocessor.Params
import spark.SparkContext
import spark.SparkContext._


class RawEventProcessor(sc: SparkContext, params : Params) extends BaseEventProcessor {

  val TS            = "timestamp"
  val SENDER_ID     = "sender_id"
  val IP            = "ip"
  val EMAIL         = "email"
  val TEST_EMAIL    = "test@aabbcc.com"
  val HOUR          = "hr"
  val BLANK         = ""
  val UNKNOWN       = "unknown"
  val CITY          = "city"
  val COUNTRY_CODE  = "country"
  val LAT           = "lat"
  val LONG          = "long"
  val REGION        = "region"
  val ZIP           = "zip"
  var lookupService : LookupService = null 
  val CC_MAP     = Config.CC_MAP


  def process(data : spark.RDD[String]) : spark.RDD[String] = {
    
    val rawData = data.filter( lineFilter ).map( mymap ).cache()
    println("RawEventProcessor Done!")
    rawData
  }

  
  def lineFilter(line : String) : Boolean = {
      val ss = line.split("\u0001")

      if (ss == null || ss.length != 3) {
          false
      } else {
          var uid = ss(0)
          var ts = ss(1)
        
          ss(2).split("\u0002")
               .forall(x => {                 
                                val entry = x.split("\u0003", 2)
                                if (entry(0).equals(EMAIL) && entry(1).startsWith(TEST_EMAIL))
                                     false
                                else if (TS.equals(entry(0)) && (entry(1) == null || entry(1).equals(BLANK) || entry(1).equals(UNKNOWN))) 
                                     false
                                else                            
                                     true
                            })
      }
    
  }
  
  
  
  def mymap(line : String) : String = {
     val ss = line.split("\u0001")

     if (ss == null || ss.length != 3)
        return ""

     if (lookupService == null) {
           try {
               lookupService = new LookupService( new File("GeoIPCity.dat"), LookupService.GEOIP_MEMORY_CACHE )
           } catch {
               case e : java.io.FileNotFoundException =>
                     println("Error : could not open file")
                     println("       -> " + e)
                     exit(1)
           }
     }

     var uid = ss(0)
     var ts = ss(1)
     val data = ss(2).split("\u0002")
     var map : Map[String, String] = Map()

     for(pair <- data)  {
         val entry = pair.split("\u0003", 2)

         if (SENDER_ID.equals(entry(0))) {
               uid = entry(1)
               map += (SENDER_ID -> entry(1))
         } else if (TS.equals(entry(0))) {
               val ymdh = entry(1).split("-")
               map += (HOUR -> ymdh(3))
         } else if (IP.equals(entry(0))) {
               val loc : Location = lookupService.getLocation(entry(1))

               if (loc != null)  {
                   if (loc.countryCode != null) {
                      if (CC_MAP.contains(loc.countryCode))
                        map += (COUNTRY_CODE -> CC_MAP(loc.countryCode))
                      else
                        map += (COUNTRY_CODE ->  loc.countryCode)
                   }

                   if (loc.city != null) {
                        map += (CITY -> loc.city)
                   }

                   map += (LAT -> String.valueOf(loc.latitude))
                   map += (LONG -> String.valueOf(loc.longitude))

                   if (loc.region != null) 
                       map += (REGION -> loc.region)
                   

                   if (loc.postalCode != null) 
                       map += (ZIP -> loc.postalCode)
                   
               }
         } else 
               map += (entry(0) -> entry(1))
         
     }

     var sb = new StringBuilder(uid + "\u0001"  + ts + "\u0001")

     map.foreach( x => {sb.append(x._1 + "\u0003" + x._2 + "\u0002") })

     //(new Text(), new Text(sb.toString())
     sb.toString
  }
    

}




