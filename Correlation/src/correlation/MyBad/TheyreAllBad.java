package correlation.MyBad;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import correlation.DMIMakeARFFfromSQL;
import correlation.MAAveragesMakeARFFfromSQL;
import correlation.MACDMakeARFFfromSQL;
import correlation.MALinesMakeARFFfromSQL;
import correlation.TSFMakeARFFfromSQL;
import util.getDatabaseConnection;
import weka.classifiers.Classifier;
import weka.classifiers.lazy.IBk;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.converters.ArffLoader.ArffReader;

public class TheyreAllBad implements Runnable {

	public String getFilename(String sym, String dos) {
		return "c:/users/joe/correlationARFF/bad/" + sym + "_" + dos + "_allbad_correlation.arff";
	}

	public static void main(String[] args) throws Exception {
		Connection conn = getDatabaseConnection.makeConnection();
		PreparedStatement ps = conn.prepareStatement("select distinct symbol" + " from dmi_correlation ");
		ResultSet rs = ps.executeQuery();

		int threadCount = 5;
		BlockingQueue<NameQueue> Que = new ArrayBlockingQueue<NameQueue>(threadCount);
		ArrayList<Thread> threadList = new ArrayList<>();
		TheyreAllBad tab = new TheyreAllBad(Que);
		for (int i = 0; i < threadCount; i++) {
			Thread thrswu = new Thread(tab);
			thrswu.start();
			threadList.add(thrswu);
		}

		while (rs.next()) {
			String sym = rs.getString(1);
			for (int i = 1; i < 31; i++) {
				NameQueue nq = new NameQueue();
				nq.sym = sym;
				nq.dos = i + "";
				Que.put(nq);
			}
		}

		for (int i = 0; i < threadCount; i++) {
			NameQueue nq = new NameQueue();
			nq.sym = "stop it you idiot";
			Que.put(nq);

		}

		checkThread: while (true) {
			for (Thread thread : threadList) {
				if (thread.isAlive()) {
					Thread.sleep(1000);
					continue checkThread;
				}
			}
			System.out.println("all done");
			return;
		}

	}

	static public class NameQueue {
		String sym;
		String dos;
	}

	BlockingQueue<NameQueue> Que;

	public TheyreAllBad(BlockingQueue<NameQueue> Que) {
		this.Que = Que;
	}

	@Override
	public void run() {
		try {
			while (true) {
				NameQueue q = Que.take();
				String sym = q.sym;
				if (sym.startsWith("stop it you idiot"))
					return;
				String dos = q.dos;
				makeARFFFromSQL(sym, dos);
			}
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}
	}

	public void makeARFFFromSQL(String sym, String dos) throws Exception {

		File f = new File(getFilename(sym, dos));
		if (f.exists())
			return;
//		PrintWriter pw = new PrintWriter(getFilename(sym, dos));
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		pw.println("% 1. Title: " + sym + "_" + dos + "_allbad_correlation");
		pw.println("@RELATION " + sym + "_" + dos);
		pw.println("@ATTRIBUTE dmi NUMERIC");
		pw.println("@ATTRIBUTE macd NUMERIC");
		pw.println("@ATTRIBUTE mavg NUMERIC");
		pw.println("@ATTRIBUTE mali NUMERIC");
		pw.println("@ATTRIBUTE tsf NUMERIC");
		pw.println("@ATTRIBUTE class NUMERIC");

		pw.println("@DATA");

		DMIMakeARFFfromSQL dmi = new DMIMakeARFFfromSQL(true);
		ArffReader dmiArff = new ArffReader(new FileReader(dmi.makeARFFFromSQL(sym, dos)));
		int dmiClassPos = dmiArff.getData().numAttributes() - 1;
		dmiArff.getData().setClassIndex(dmiClassPos);

		MACDMakeARFFfromSQL macd = new MACDMakeARFFfromSQL(true);
		ArffReader macdArff = new ArffReader(new FileReader(macd.makeARFFFromSQL(sym, dos)));
		int macdClassPos = macdArff.getData().numAttributes() - 1;
		macdArff.getData().setClassIndex(macdClassPos);

		MAAveragesMakeARFFfromSQL mavg = new MAAveragesMakeARFFfromSQL(true);
		ArffReader mavgArff = new ArffReader(new FileReader(mavg.makeARFFFromSQL(sym, dos)));
		int mavgClassPos = mavgArff.getData().numAttributes() - 1;
		mavgArff.getData().setClassIndex(mavgClassPos);

		MALinesMakeARFFfromSQL mali = new MALinesMakeARFFfromSQL(true);
		ArffReader maliArff = new ArffReader(new FileReader(mali.makeARFFFromSQL(sym, dos)));
		int maliClassPos = maliArff.getData().numAttributes() - 1;
		maliArff.getData().setClassIndex(maliClassPos);

		TSFMakeARFFfromSQL tsf = new TSFMakeARFFfromSQL(true);
		ArffReader tsfArff = new ArffReader(new FileReader(tsf.makeARFFFromSQL(sym, dos)));
		int tsfClassPos = tsfArff.getData().numAttributes() - 1;
		tsfArff.getData().setClassIndex(tsfClassPos);

		for (String dt : dmi.dateAttribute.keySet()) {

			Integer dmiPos = dmi.dateAttribute.get(dt);
			if (dmiPos == null)
				continue;

			Integer macdPos = macd.dateAttribute.get(dt);
			if (macdPos == null)
				continue;

			Integer mavgPos = mavg.dateAttribute.get(dt);
			if (mavgPos == null)
				continue;

			Integer maliPos = mali.dateAttribute.get(dt);
			if (maliPos == null)
				continue;

			Integer tsfPos = tsf.dateAttribute.get(dt);
			if (tsfPos == null)
				continue;

			Instance dmiInst = dmiArff.getData().get(dmiPos.intValue());
			double dmiClassValue = dmiInst.classValue();

			Instance macdInst = macdArff.getData().get(macdPos.intValue());
			double macdClassValue = macdInst.classValue();

			Instance mavgInst = mavgArff.getData().get(mavgPos.intValue());
			double mavgClassValue = mavgInst.classValue();

			Instance maliInst = maliArff.getData().get(maliPos.intValue());
			double maliClassValue = maliInst.classValue();

			Instance tsfInst = tsfArff.getData().get(tsfPos.intValue());
			double tsfClassValue = tsfInst.classValue();

			if (dmiClassValue != macdClassValue | dmiClassValue != mavgClassValue | dmiClassValue != maliClassValue
					| dmiClassValue != tsfClassValue)
				System.out.println("logic error");
			pw.print(doInstance(dmiArff.getData(), dmiInst) + ",");
			pw.print(doInstance(macdArff.getData(), macdInst) + ",");
			pw.print(doInstance(mavgArff.getData(), mavgInst) + ",");
			pw.print(doInstance(maliArff.getData(), maliInst) + ",");
			pw.print(doInstance(tsfArff.getData(), tsfInst) + ",");
			pw.println(dmiClassValue);
			pw.flush();
		}

		synchronized (Que) {
			FileWriter fw = new FileWriter(f);
			fw.write(sw.toString());
			fw.flush();
			fw.close();
			System.out.println("wrote to " + f.getName());

		}

	}

	double doInstance(Instances instances, Instance inst) throws Exception {

		Classifier classifier = new IBk();

		double save = inst.classValue();
		inst.setClassMissing();
		classifier.buildClassifier(instances);
		double got = classifier.classifyInstance(inst);
		inst.setClassValue(save);
		return got;
	}

}
