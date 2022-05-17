package correlation.MyBad;

import java.io.FileReader;

import correlation.ARFFMaker.DMIMakeARFFfromSQL;
import util.getDatabaseConnection;
import weka.classifiers.AbstractClassifier;
import weka.classifiers.Classifier;
import weka.core.Instance;
import weka.core.converters.ArffLoader.ArffReader;

public class DMI {

	public static void main(String[] args) throws Exception {
		String sym = "qqq";
		int dos = 1;
		Classifier classifier = AbstractClassifier.forName("weka.classifiers.lazy.LWL",
				new String[] { "-K", "120", "-A", "weka.core.neighboursearch.LinearNNSearch", "-W",
						"weka.classifiers.trees.RandomForest", "--", "-I", "160", "-K", "5", "-depth", "15" });

		DMIMakeARFFfromSQL dmi = new DMIMakeARFFfromSQL(false, false);
		dmi.makeARFFFromSQL(sym, dos, getDatabaseConnection.makeConnection());
		ArffReader ar = new ArffReader(new FileReader(DMIMakeARFFfromSQL.getFilename(sym, dos)));
		int classPos = ar.getData().numAttributes() - 1;
		ar.getData().setClassIndex(classPos);
		for (Instance inst : ar.getData()) {
			System.out.print(inst.classValue() + ";");
			double save = inst.classValue();
			inst.setClassMissing();
			classifier.buildClassifier(ar.getData());
			System.out.println(classifier.classifyInstance(inst));
			inst.setClassValue(save);
		}

	}

}
