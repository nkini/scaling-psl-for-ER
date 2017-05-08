/** File provided by V2 Maestros for its students for education purposes only
 * Copyright @2016 All rights reserved.
 */
package com.v2maestros.spark.bda.common;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession ;

import org.apache.spark.SparkConf;

public class SparkConnection {

	//A name for the spark instance. Can be any string
	private static String appName = "PSL-Spark-ADMM";
	//Pointer / URL to the Spark instance - embedded
	private static String sparkMaster = "local[2]";
	
	private static JavaSparkContext spContext = null;
	private static SparkSession sparkSession = null;
	private static String tempDir = "/tmp";
	
	private static void getConnection() {
		
		if ( spContext == null) {	
			//Setup Spark configuration
			SparkConf conf = new SparkConf()
					.setAppName(appName)
					.setMaster(sparkMaster);
			
			//Create Spark Context from configuration
			spContext = new JavaSparkContext(conf);
			
			 sparkSession = SparkSession
					  .builder()
					  .appName(appName)
					  .master(sparkMaster)
					  .config("spark.sql.warehouse.dir", tempDir)
					  .getOrCreate();
			 
		}
		
	}
	
	public static JavaSparkContext getContext() {
		
		if ( spContext == null ) {
			getConnection();
		}
		return spContext;
	}
	
	public static SparkSession getSession() {
		if ( sparkSession == null) {
			getConnection();
		}
		return sparkSession;
	}

}
