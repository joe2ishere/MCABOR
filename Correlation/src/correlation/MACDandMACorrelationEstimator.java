package correlation;

import java.io.File;
import java.io.StringReader;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import weka.classifiers.Classifier;
import weka.classifiers.lazy.IBk;
import weka.core.Instances;

public class MACDandMACorrelationEstimator extends CorrelationEstimator {

	MACDMakeARFFfromSQL macd_makeSQL;
	MAMakeARFFfromSQL ma_makeSQL;

	public MACDandMACorrelationEstimator(Connection conn) throws SQLException {
		super(conn);
		function = "macd_ma";
		makeSQL = new MACDandMAMakeARFFfromSQL(false, true);
		macd_makeSQL = new MACDMakeARFFfromSQL(false, true);
		ma_makeSQL = new MAMakeARFFfromSQL(false, true);
	}

	public MACDandMACorrelationEstimator(Connection conn, boolean thirtyDayMode) throws SQLException {
		super(conn);
		function = "macd_ma";
	}

	@Override
	public double prun() throws Exception {
		AttributeParm macdParms = null;
		AttributeParm maParms = null;
		try {
			macdParms = macd_makeSQL.buildParameters(sym, daysOut, conn);
			maParms = ma_makeSQL.buildParameters(sym, daysOut, conn);
		} catch (Exception e1) {
			e1.printStackTrace();
			System.exit(0);
		}

		String $instances = null;
		try {
			File instanceFile = new File("c:/users/joe/correlationARFF/" + function + "/" + sym + "/D" + daysOut
					+ (whichHalf ? "f" : "s") + ".zrff");

			if (instanceFile.exists()) {
				synchronized (conn) {
					/*
					 * BufferedReader br = new BufferedReader(new FileReader(instanceFile));
					 * StringBuilder builder = new StringBuilder(250000); char[] buffer = new
					 * char[4096]; int numChars; while ((numChars = br.read(buffer)) > 0) {
					 * builder.append(buffer, 0, numChars); } byte[] output =
					 * builder.toString().getBytes(StandardCharsets.UTF_8);br.close(); builder = new
					 * StringBuilder(250000);
					 */
					byte[] output = Files.readAllBytes(instanceFile.toPath());
					Inflater inflater = new Inflater();
					inflater.setInput(output);
					StringBuilder builder = new StringBuilder(250000);
					while (inflater.finished() == false) {
						byte b100[] = new byte[100];
						int len2 = inflater.inflate(b100);
						builder.append(new String(b100, 0, len2, "Cp1252"));
					}
					inflater.end();
					$instances = builder.toString();
				}
			} else {
				$instances = makeSQL.makeARFFFromSQL(sym, daysOut, macdParms, maParms, whichHalf);
				byte[] bytes = $instances.getBytes("Cp1252");
				byte[] output = new byte[bytes.length / 2];
				Deflater deflater = new Deflater();
				deflater.setInput(bytes);
				deflater.finish();
				int len = deflater.deflate(output);
				File dirFile = new File("c:/users/joe/correlationARFF/" + function);
				if (dirFile.exists() == false) {
//			dirFile.createNewFile();
					dirFile.mkdir();
				}
				dirFile = new File("c:/users/joe/correlationARFF/" + function + "/" + sym);
				if (dirFile.exists() == false) {
//			dirFile.createNewFile();
					dirFile.mkdir();
				}
				instanceFile.createNewFile();
				Files.write(instanceFile.toPath(), output);

			}

		} catch (Exception e1) {
			e1.printStackTrace();
			System.exit(0);
		}

		if ($instances == null) {
			System.out.println("if ($instances == null)");
			System.exit(0);
		}

		String $lastLine = null;
		try {

			$lastLine = makeSQL.makeARFFFromSQLForQuestionMark(sym, daysOut, macdParms, maParms);

		} catch (

		Exception e1) {
			e1.printStackTrace();
			System.exit(0);
		}

		if ($lastLine == null) {
			System.out.println("if ($lastLine == null)");
			System.exit(0);
		}

		Instances trainInst = null;
		try {
			trainInst = new Instances(new StringReader($instances));
		} catch (Exception e) {
			e.printStackTrace();
		}

		trainInst.setClassIndex(trainInst.numAttributes() - 1);

		Classifier classifier = new IBk();

		classifier.buildClassifier(trainInst);

		Instances testInst;
		testInst = new Instances(new StringReader($lastLine));
		testInst.setClassIndex(testInst.numAttributes() - 1);
		return classifier.classifyInstance(testInst.get(testInst.size() - 1));
	}

	Classifier thisClassifier;

	public Classifier getClassifier() {
		return thisClassifier;
	}

}
