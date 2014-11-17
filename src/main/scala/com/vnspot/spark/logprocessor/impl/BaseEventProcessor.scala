package com.vnspot.spark.logprocessor.impl

import spark.SparkContext
import SparkContext._


trait BaseEventProcessor  extends Serializable {

	def process(data : spark.RDD[String]) : spark.RDD[String]
	
}