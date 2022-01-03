package correlation;

import java.io.StringReader;
import java.sql.Connection;
import java.util.Random;

import util.getDatabaseConnection;
import weka.classifiers.Evaluation;
import weka.classifiers.lazy.IBk;
import weka.classifiers.trees.RandomForest;
import weka.core.Instances;

public class Phase2Tester {

	public static void main(String[] args) throws Exception {
		IBk ibk = new IBk();
		RandomForest rf = new RandomForest();
//		KStar ks = new KStar();
		Connection conn = getDatabaseConnection.makeConnection();
//		DMIMakeARFFfromSQLPhase2 maker = new DMIMakeARFFfromSQLPhase2(false);
//		MAAveragesMakeARFFfromSQLPhase2 maker = new MAAveragesMakeARFFfromSQLPhase2(false);
//		MACDMakeARFFfromSQLPhase2 maker = new MACDMakeARFFfromSQLPhase2(false);
//		MALinesMakeARFFfromSQLPhase2 maker = new MALinesMakeARFFfromSQLPhase2(false);
		SMMakeARFFfromSQLPhase2 maker = new SMMakeARFFfromSQLPhase2(false, true);
//		TSFMakeARFFfromSQLPhase2 maker = new TSFMakeARFFfromSQLPhase2(false);

//		DMIMakeARFFfromSQL maker = new DMIMakeARFFfromSQL(false);
//		MAAveragesMakeARFFfromSQL maker = new MAAveragesMakeARFFfromSQL(false);
//		MACDMakeARFFfromSQL maker = new MACDMakeARFFfromSQL(false);
//		MALinesMakeARFFfromSQL maker = new MALinesMakeARFFfromSQL(false);
//		SMMakeARFFfromSQL maker = new SMMakeARFFfromSQL(false);
//		TSFMakeARFFfromSQL maker = new TSFMakeARFFfromSQL(false);
		for (int i = 1; i <= 30; i++) {
			System.out.print("i;" + i + ";");

			String arff = maker.makeARFFFromSQL("dia", i + "", conn, false);
//			FileReader fr = new FileReader(maker.makeARFFFromSQL("qqq", i + ""));
//			BufferedReader br = new BufferedReader(fr);
//			StringBuffer sb = new StringBuffer(1000);
//			String in = "";
//			while ((in = br.readLine()) != null) {
//				sb.append(in + "\n");
//			}
//			br.close();
//			String arff = sb.toString();

			Instances inst = new Instances(new StringReader(arff));
			inst.setClassIndex(inst.numAttributes() - 1);
			Evaluation eval = new Evaluation(inst);
			eval.crossValidateModel(ibk, inst, 10, new Random());
			System.out.print("ibk;" + eval.pctCorrect() + ";");
			eval.crossValidateModel(rf, inst, 10, new Random());
			System.out.print("rf;" + eval.pctCorrect() + ";");
//			eval.crossValidateModel(ks, inst, 10, new Random());
			System.out.println("ks;" + eval.pctCorrect());
		}
	}

}
