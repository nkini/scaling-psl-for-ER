
/****************************************************************************

                   Spark with Java

             Copyright : V2 Maestros @2016
                    
Code Samples : Spark Operations
*****************************************************************************/

package org.linqs.psladmmspark;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import com.esotericsoftware.minlog.Log;
import com.v2maestros.spark.bda.common.SparkConnection;
import org.apache.log4j.Logger;

import org.apache.log4j.Level;

import org.linqs.psladmmspark.PSLWorker;

public class PSLMaster {

	public static void main(String[] args) throws InstantiationException, IllegalAccessException {

		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);
		SparkSession spSession = SparkConnection.getSession();

		/* Here's the plan:
		 * 	Spark reads files
			Computes blocks
			Distributes data to nodes in the cluster using partitionBy('block')
			Nodes store the data as text files again
			Then each executor is just the regular MPEInference call that we all know
		 */
		
		//Spark reads files
		Dataset<Row> df = loadData(spSession);
		
		//compute blocks
		Dataset<Row> blockedData = computeBlocks(df);
		
		//Distributes data to nodes in the cluster using partitionBy('block')
		//partition by blocks
		int numBlocks = (int) blockedData.select("block").distinct().count();
		Dataset<Row> partitionedData = blockedData.repartition(numBlocks, blockedData.col("block"));
		
		//Log.info("Columns: "+);
		blockedData.printSchema();
		Log.info("Number of partitions: "+ partitionedData.rdd().getNumPartitions());
		
		//Nodes store the data as text files again
		saveAsCSV(partitionedData);

        //PSLWorker.main(args);
		//PSLWorker.run(partitionedData);
		
		Dataset<Row> crossProduct = PSLWorker.run(partitionedData);
		
		long numRecords = crossProduct.count();
		
		// Java vs Groovy: Aim to call the Groovy class from Java
		// 				   Compile to bytecode, call main
		//					
		//				   Redesign tip: Level of abstraction - MPEInference
		//	Can you say this in spark:
		//		A particular worker calls a particular function
		//		
		System.out.println("Num records"+numRecords);
	}

	private static void saveAsCSV(Dataset<Row> partitionedData) {
		partitionedData.write().option("header","false").csv("DBLP+ACM.csv");
	}

	private static Dataset<Row> computeBlocks(Dataset<Row> df) {
		//TODO: (nikhil) This function needs to accept
		//      a function as a parameter, which when 
		//      applied to df will return a column that
		//		corresponding to the block IDS.
		Dataset<Row> dfWithBlocks = df.withColumn("block", df.col("year"));
		return dfWithBlocks;
	}

	private static Dataset<Row> loadData(SparkSession spSession) {
		Dataset<Row> dfDBLP = spSession.read().option("header", "true").csv("data/DBLP-ACM/DBLP2.csv");
		Dataset<Row> dfACM = spSession.read().option("header", "true").csv("data/DBLP-ACM/ACM.csv");
		
		dfDBLP = dfDBLP.withColumn("DB", functions.lit("DBLP"));
		dfACM = dfACM.withColumn("DB", functions.lit("ACM"));
		Dataset<Row> df = dfDBLP.union(dfACM);
		
		return df;
	}

}
