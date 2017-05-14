package org.linqs.psladmmspark;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import scala.Tuple2;
import scala.Tuple3;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import com.esotericsoftware.minlog.Log;
import org.linqs.psladmmspark.SparkConnection;
import org.apache.log4j.Logger;

import java.util.Iterator;

import org.apache.log4j.Level;

import com.wcohen.ss.BasicStringWrapper;
import com.wcohen.ss.Level2JaroWinkler;

import org.linqs.psladmmspark.PSLWorker;

public class PSLMaster {

	public static void main(String[] args) throws InstantiationException, IllegalAccessException {

		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);
		SparkSession spSession = SparkConnection.getSession();
		JavaSparkContext spContext = SparkConnection.getContext();

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
		
		//convert to RDD
		JavaRDD<Row> data = blockedData.javaRDD();
		
		//Distributes data to nodes in the cluster using partitionBy('block')
		//partition by blocks
		int numBlocks = (int) blockedData.select("block").distinct().count();
		JavaPairRDD<Integer, Row> kvDataBlockAsKey = data.mapToPair(new getKV());
		JavaPairRDD<Integer, Row> partitionedData = kvDataBlockAsKey.repartition(numBlocks);
		
		//join the rows within a block
		JavaPairRDD<Integer, Tuple2<Row, Row>> joinedData = partitionedData.join(partitionedData);

		//Row r = joinedData.take(1).get(0)._2()._1();
		//System.out.println("\nschema: "+ r.schema().toString() + "\nDB index: " + r.fieldIndex("DB")  + "\nget(fieldIndex(DB)): "+r.get(r.fieldIndex("DB")));
		
		//filter the pairs that are double counted
		joinedData = joinedData.filter(new removeSelfAndDoubleCounts());
		
		//System.out.println("\nNum of joined data: " + joinedData.count());
		
		Log.info("\nNumber of partitions: "+ joinedData.getNumPartitions());
		
		//calculate the similarity metric
		JavaPairRDD<Integer, Tuple3<Row, Row, Double>> joinedDataWithSimilarity = joinedData.mapValues(new calculateSimilarity());
		joinedDataWithSimilarity = joinedDataWithSimilarity.cache();
		
		//get observations and targets
		JavaPairRDD<Integer, Tuple2<String, String>> targets = joinedDataWithSimilarity.mapValues(new getTargets());
		JavaPairRDD<Integer, Tuple3<String, String, Double>> observations = joinedDataWithSimilarity.mapValues(new getObservations());

		System.out.println("\nNum of observations: " + observations.count());
		System.out.println("\nNum of targets: " + targets.count());
		
		observations.mapValues(x -> String.join("\t", x._1(),x._2(),x._3().toString())).saveAsTextFile("data/similar_obs.txt");
		targets.mapValues(x -> String.join("\t", x._1(),x._2())).saveAsTextFile("data/same_targets.txt");

		JavaRDD<Tuple3<String, String, Double>> targets2 = joinedDataWithSimilarity.mapPartitionsWithIndex(new PSLWorker(),true);
		
		//targets.mapPartitions(observations);
		
		//Dataset<Row> results = PSLWorker.run2(spSession, partitionedData);
		
		//long resultsNum = results.count();
		
		//System.out.println("Num records"+resultsNum);
		
		//Redesign tip: Level of abstraction - MPEInference
		//	Can you say this in spark:
		//		A particular worker calls a particular function
	}

	private static void saveAsCSV(Dataset<Row> partitionedData) {
		partitionedData.write().option("header","true").csv("DBLP+ACM.csv");
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



class getKV implements PairFunction<Row, Integer, Row> {
	
    //private int counter = 0;
    //private double randid = Math.random();

	public Tuple2<Integer, Row> call(Row row) throws Exception{
        //counter++;
        //System.out.println("Random Id: "+randid+"\tCounter: "+counter);
		return new Tuple2<Integer, Row>(Integer.parseInt(row.getString(row.fieldIndex("year"))), row);
	}
}

class removeSelfAndDoubleCounts implements Function<Tuple2<Integer, Tuple2<Row, Row>>, Boolean> {
	
	public Boolean call(Tuple2<Integer, Tuple2<Row, Row>> x) {
		
		Row r1 = x._2()._1();
		Row r2 = x._2()._2();
		String db1 = (String) r1.get(r1.fieldIndex("DB"));
		String db2 = (String) r2.get(r2.fieldIndex("DB"));
		
		if (db1.equals("DBLP") || db1.equals(db2)) {
			return false;
		}
		
		return true;
	} 
}

class getObservations implements Function<Tuple3<Row, Row, Double>, Tuple3<String, String, Double>> {
	
	public Tuple3<String, String, Double> call(Tuple3<Row, Row, Double> arg0) {
		
		String row1ID = arg0._1().getString(arg0._1().fieldIndex("id"));
		String row2ID = arg0._1().getString(arg0._2().fieldIndex("id"));
		
		return new Tuple3<String, String, Double>(row1ID, row2ID, arg0._3());
	}
	
}

class getTargets implements Function<Tuple3<Row, Row, Double>, Tuple2<String, String>> {
	
	public Tuple2<String, String> call(Tuple3<Row, Row, Double> arg0) {
		
		String row1ID = arg0._1().getString(arg0._1().fieldIndex("id"));
		String row2ID = arg0._1().getString(arg0._2().fieldIndex("id"));
		
		return new Tuple2<String, String>(row1ID, row2ID);
	}
	
}

class calculateSimilarity implements Function<Tuple2<Row, Row>, Tuple3<Row, Row, Double>> {
	
	public Tuple3<Row, Row, Double> call(Tuple2<Row, Row> arg0) {
		
		Row dbRow1 = arg0._1();
		Row dbRow2 = arg0._2();
		
		String authordb1 = dbRow1.getString(dbRow1.fieldIndex("authors"));
		String authordb2 = dbRow2.getString(dbRow2.fieldIndex("authors"));
		//Calculate Similarity for authors
		double authSim = getL2JWSim(authordb1, authordb2);
		
		String titledb1 = dbRow1.getString(dbRow1.fieldIndex("title"));
		String titledb2 = dbRow2.getString(dbRow2.fieldIndex("title"));
		//Calculate Similarity for titles
		double titleSim = getL2JWSim(titledb1, titledb2);
		
		String venuedb1 = dbRow1.getString(dbRow1.fieldIndex("venue"));
		String venuedb2 = dbRow2.getString(dbRow2.fieldIndex("venue"));
		//Calculate Similarity for venues
		double venueSim = getL2JWSim(venuedb1, venuedb2);
		
		//Linearly combine the similarities
		double combinedSim  = (authSim + titleSim + venueSim) / 3;
		
		return new Tuple3<Row, Row, Double>(dbRow1, dbRow2, combinedSim);
	}
	
	public double getL2JWSim(String a, String b){

        if (a == null || b == null) 
            return 0.0;

		double simThresh = 0.5;
		
		BasicStringWrapper aWrapped = new BasicStringWrapper(a);
		BasicStringWrapper bWrapped = new BasicStringWrapper(b);
			
		Level2JaroWinkler l2jaroW = new Level2JaroWinkler();
		double sim = l2jaroW.score(aWrapped, bWrapped);
	
		if (sim < simThresh)
			return 0.0;
		else
			return sim;
	}
	
}
