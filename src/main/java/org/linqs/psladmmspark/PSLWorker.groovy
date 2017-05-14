package org.linqs.psladmmspark;

import org.linqs.psl.application.inference.MPEInference;
import org.linqs.psl.config.ConfigBundle;
import org.linqs.psl.config.ConfigManager;
import org.linqs.psl.database.Database;
import org.linqs.psl.database.DatabasePopulator;
import org.linqs.psl.database.DataStore;
import org.linqs.psl.database.Partition;
import org.linqs.psl.database.Queries;
import org.linqs.psl.database.ReadOnlyDatabase;
import org.linqs.psl.database.loading.Inserter;
import org.linqs.psl.database.rdbms.driver.H2DatabaseDriver;
import org.linqs.psl.database.rdbms.driver.H2DatabaseDriver.Type;
import org.linqs.psl.database.rdbms.RDBMSDataStore;
import org.linqs.psl.groovy.PSLModel;
import org.linqs.psl.model.atom.Atom;
import org.linqs.psl.model.predicate.StandardPredicate;
import org.linqs.psl.model.term.ConstantType;
import org.linqs.psl.utils.dataloading.InserterUtils;
import org.linqs.psl.utils.evaluation.printing.AtomPrintStream;
import org.linqs.psl.utils.evaluation.printing.DefaultAtomPrintStream;
import org.linqs.psl.utils.evaluation.statistics.ContinuousPredictionComparator;
import org.linqs.psl.utils.evaluation.statistics.DiscretePredictionComparator;
import org.linqs.psl.utils.evaluation.statistics.DiscretePredictionStatistics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import groovy.time.TimeCategory;
import scala.Tuple2;
import scala.Tuple3;

import java.nio.file.Paths;
import java.util.Iterator

import org.apache.spark.sql.functions;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.api.java.function.Function2;



/**
 * A simple transitivity example.
 * In this example, we infer sameness of references based on observed similarity, also applying the transitivity rule.
 *
 * @author Nikhil Kini <nkini@ucsc.edu>
 */
public class PSLWorker implements Function2<Integer, 
											Iterator<Tuple2<Integer, Tuple3<Row, Row, Double>>>, 
											Iterator<Tuple3<String, String, Double>>
											> {

	private static final String PARTITION_OBSERVATIONS = "observations";
	private static final String PARTITION_TARGETS = "targets";
	private static final String PARTITION_TRUTH = "truth";

	private Logger log;
	private DataStore ds;
	private PSLConfig config;
	private PSLModel model;

	/**
	 * Class for config variables
	 */
	private class PSLConfig {
		public ConfigBundle cb;

		public String experimentName;
		public String dbPath;
		public String dataPath;
		public String outputPath;

		public boolean sqPotentials = true;
		public Map weightMap = [
			"Similar":50,
			"Transitivity":50,
			"Symmetry":100,
			"Prior":1
		];
		public boolean useTransitivityRule = true;
		public boolean useSymmetryRule = false;

		public PSLConfig(ConfigBundle cb) {
			this.cb = cb;

			this.experimentName = cb.getString('experiment.name', 'default');
			this.dbPath = cb.getString('experiment.dbpath', '/tmp');
			this.dataPath = cb.getString('experiment.data.path', '../data');
			this.outputPath = cb.getString('experiment.output.outputdir', Paths.get('output', this.experimentName).toString());

			this.weightMap["Similar"] = cb.getInteger('model.weights.similar', weightMap["Similar"]);
			this.weightMap["Transitivity"] = cb.getInteger('model.weights.transitivity', weightMap["Transitivity"]);
			this.weightMap["Symmetry"] = cb.getInteger('model.weights.symmetry', weightMap["Symmetry"]);
			this.weightMap["Prior"] = cb.getInteger('model.weights.prior', weightMap["Prior"]);
			this.useTransitivityRule = cb.getBoolean('model.rule.transitivity', false);
			this.useSymmetryRule = cb.getBoolean('model.rule.symmetry', false);
		}
	}

	/*
	public PSLWorker(ConfigBundle cb) {
		log = LoggerFactory.getLogger(this.class);
		config = new PSLConfig(cb);
		ds = new RDBMSDataStore(new H2DatabaseDriver(Type.Disk, Paths.get(config.dbPath, 'transitivity').toString(), true), cb);
		model = new PSLModel(this, ds);
	}*/

	/**
	 * Defines the logical predicates used in this model
	 */
	private void definePredicates() {
		model.add predicate: "Similar", types: [ConstantType.UniqueID, ConstantType.UniqueID];
		model.add predicate: "Same", types: [ConstantType.UniqueID, ConstantType.UniqueID];
	}

	/**
	 * Defines the rules for this model, optionally including transitivty and
	 * symmetry based on the PSLConfig options specified
	 */
	private void defineRules() {
		log.info("Defining model rules");
		model.add(
				rule: ( Similar(R1,R2) & (R1-R2) ) >> Same(R1,R2),
				squared: config.sqPotentials,
				weight : config.weightMap["Similar"]
				);

		if (config.useTransitivityRule) {
			model.add(
					rule: ( Same(R1,R2) & Same(R2,R3) & (R1-R3) ) >> Same(R1,R3),
					squared: config.sqPotentials,
					weight : config.weightMap["Transitivity"]
					);
		}

		if (config.useSymmetryRule) {
			model.add(
					rule:  Same(R1,R2) >> Same(R2,R1),
					squared: config.sqPotentials,
					weight : config.weightMap["Symmetry"]
					);
		}

		model.add(
				rule: ~Same(R1,R2),
				squared:config.sqPotentials,
				weight: config.weightMap["Prior"]
				);

		log.debug("model: {}", model);
	}

	/**
	 * Load data from text files into the DataStore. Three partitions are defined
	 * and populated: observations, targets, and truth.
	 * Observations contains evidence that we treat as background knowledge and
	 * use to condition our inferences
	 * Targets contains the inference targets - the unknown variables we wish to infer
	 * Truth contains the true values of the inference variables and will be used
	 * to evaluate the model's performance
	 */
	private void loadData(Partition obsPartition, Partition targetsPartition, Partition truthPartition) {
		log.info("Loading data into database");

		Inserter inserter = ds.getInserter(Similar, obsPartition);
		InserterUtils.loadDelimitedDataTruth(inserter, Paths.get(config.dataPath, "similar_obs.txt").toString());

		//inserter = ds.getInserter(Same, obsPartition);
		//InserterUtils.loadDelimitedData(inserter, Paths.get(config.dataPath, "same_obs.txt").toString());

		inserter = ds.getInserter(Same, targetsPartition);
		InserterUtils.loadDelimitedData(inserter, Paths.get(config.dataPath, "same_targets.txt").toString());

		//inserter = ds.getInserter(Same, truthPartition);
		//InserterUtils.loadDelimitedDataTruth(inserter, Paths.get(config.dataPath, "same_truth.txt").toString());
	}

	/**
	 * Run inference to infer the unknown Knows relationships between people.
	 */
	private void runInference(Partition obsPartition, Partition targetsPartition) {
		log.info("Starting inference");

		Date infStart = new Date();
		HashSet closed = new HashSet<StandardPredicate>([Similar]);
		Database inferDB = ds.getDatabase(targetsPartition, closed, obsPartition);
		MPEInference mpe = new MPEInference(model, inferDB, config.cb);
		mpe.mpeInference();
		mpe.close();
		inferDB.close();

		log.info("Finished inference in {}", TimeCategory.minus(new Date(), infStart));
	}

	/**
	 * Writes the output of the model into a file
	 */
	private void writeOutput(Partition targetsPartition) {
		Database resultsDB = ds.getDatabase(targetsPartition);
		PrintStream ps = new PrintStream(new File(Paths.get(config.outputPath, "same_infer.txt").toString()));
		AtomPrintStream aps = new DefaultAtomPrintStream(ps);
		Set atomSet = Queries.getAllAtoms(resultsDB,Same);
		for (Atom a : atomSet) {
			aps.printAtom(a);
		}

		aps.close();
		ps.close();
		resultsDB.close();
	}

	/**
	 * Run statistical evaluation scripts to determine the quality of the inferences
	 * relative to the defined truth.
	 */
	private void evalResults(Partition targetsPartition, Partition truthPartition) {
		Database resultsDB = ds.getDatabase(targetsPartition, [Same] as Set);
		Database truthDB = ds.getDatabase(truthPartition, [Same] as Set);
		DiscretePredictionComparator dpc = new DiscretePredictionComparator(resultsDB);
		ContinuousPredictionComparator cpc = new ContinuousPredictionComparator(resultsDB);
		dpc.setBaseline(truthDB);
		//	 dpc.setThreshold(0.99);
		cpc.setBaseline(truthDB);
		DiscretePredictionStatistics stats = dpc.compare(Same);
		double mse = cpc.compare(Same);
		log.info("MSE: {}", mse);
		log.info("Accuracy {}, Error {}",stats.getAccuracy(), stats.getError());
		log.info(
				"Positive Class: precision {}, recall {}",
				stats.getPrecision(DiscretePredictionStatistics.BinaryClass.POSITIVE),
				stats.getRecall(DiscretePredictionStatistics.BinaryClass.POSITIVE));
		log.info("Negative Class Stats: precision {}, recall {}",
				stats.getPrecision(DiscretePredictionStatistics.BinaryClass.NEGATIVE),
				stats.getRecall(DiscretePredictionStatistics.BinaryClass.NEGATIVE));

		resultsDB.close();
		truthDB.close();
	}

	public String run() {
		log.info("Running experiment {}", config.experimentName);

		Partition obsPartition = ds.getPartition(PARTITION_OBSERVATIONS);
		Partition targetsPartition = ds.getPartition(PARTITION_TARGETS);
		Partition truthPartition = ds.getPartition(PARTITION_TRUTH);

		definePredicates();
		defineRules();
		loadData(obsPartition, targetsPartition, truthPartition);
		runInference(obsPartition, targetsPartition);
		writeOutput(targetsPartition);
		//evalResults(targetsPartition, truthPartition);

		ds.close();

		return Paths.get(config.outputPath, "same_infer.txt").toString();
	}

	/**
	 * Parse the command line options and populate them into a ConfigBundle
	 * Currently the only argument supported is the path to the data directory
	 * @param args - the command line arguments provided during the invocation
	 * @return - a ConfigBundle populated with options from the command line options
	 */
	public static ConfigBundle populateConfigBundle(String[] args) {
		ConfigBundle cb = ConfigManager.getManager().getBundle("transitivity");
		if (args.length > 0) {
			cb.setProperty('experiment.data.path', args[0]);
		}
		return cb;
	}

	public Iterator<Tuple3<String, String, Double>> call(Integer pnum, Iterator<Tuple2<Integer, Tuple3<Row, Row, Double>>> arg0)
			throws Exception {
		System.out.println("Inside call of partition "+pnum);
	}

}
