package correlation;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Random;
import java.util.Scanner;
import java.util.TreeMap;
import java.util.logging.LogManager;

import com.americancoders.dataGetAndSet.GetETFDataUsingSQL;
import com.americancoders.lineIntersect.Line;
import com.tictactec.ta.lib.MAType;

import bands.DeltaBands;
import movingAvgAndLines.MovingAvgAndLineIntercept;
import weka.classifiers.AbstractClassifier;
import weka.classifiers.Classifier;
import weka.classifiers.Evaluation;
import weka.classifiers.lazy.IBk;
import weka.core.Instances;

public class MAandLIMakeARFFfromCorrelationFile {

	public static void main(String[] args) throws Exception {

		LogManager.getLogManager().reset();

		Scanner sin = new Scanner(System.in);
		String sym;
		while (true) {
			System.out.print("\nSymbol: ");
			sym = sin.nextLine().trim();

			if (sym == null | sym.length() < 1) {
				sin.close();
				return;
			}
			break;
		}

		String dos;
		while (true) {
			System.out.print("\ndays out: ");
			dos = sin.nextLine().trim();

			if (dos == null | dos.length() < 1) {
				sin.close();
				return;
			}
			break;
		}
		sin.close();

		int daysOut = Integer.parseInt(dos);
		makeFile(sym, daysOut);
	}

	public static void makeFile(String sym, int daysOut) throws Exception {

		FileReader fr = new FileReader("SMATaB.txt");

		GetETFDataUsingSQL gsd = GetETFDataUsingSQL.getInstance(sym);

		DeltaBands db = new DeltaBands(gsd.inClose, daysOut);
		String startDate = gsd.inDate[50];

		TreeMap<String, Object> maAndLIs = new TreeMap<>();
		TreeMap<String, String[]> hmaDates = new TreeMap<>();
		BufferedReader br = new BufferedReader(fr);
		String in;
		File file = new File("c:/users/joe/correlationARFF/" + sym + "_" + daysOut + "_malis_correlation.arff");
		PrintWriter pw = new PrintWriter(file);
		pw.println("% 1. Title: " + sym + "_malis_correlation");
		pw.println("@RELATION " + sym + "_" + daysOut);
		boolean found = false;

		while ((in = br.readLine().trim()) != null) {
			String ins[] = in.split("_");

			if (sym.compareTo(ins[0]) == 0) {
				if (daysOut + "".compareTo(ins[1]) == 0) {
					found = true;
					break;
				}
			}
		}
		if (!found) {
			System.out.println(sym + " not found");
			return;
		}
		while ((in = br.readLine().trim()) != null) {
			if (in.length() < 1)
				break;
			if (in.contains("_"))
				break;
			String inColon[] = in.split(":");
			String ins[] = inColon[2].split(";");
			if (ins.length < 2)
				break;
			String symbol = ins[2];

			String symKey = symbol + "_" + inColon[0];

			GetETFDataUsingSQL pgsd = GetETFDataUsingSQL.getInstance(symbol);

			if (startDate.compareTo(pgsd.inDate[150]) < 0)
				startDate = pgsd.inDate[150];

			System.out.println("using " + symbol);
			int period = Integer.parseInt(ins[3]);
			String type = ins[4];
			MAType mat = ins[4].compareTo("Tema") == 0 ? MAType.Tema : MAType.Dema;
			int smooth = Integer.parseInt(ins[5]);

			MovingAvgAndLineIntercept mali = new MovingAvgAndLineIntercept(pgsd, period, mat, smooth, mat);

			maAndLIs.put(symKey, mali);
			pw.println("@ATTRIBUTE " + symKey + "currSlope NUMERIC");
			pw.println("@ATTRIBUTE " + symKey + "currYinterc NUMERIC");
			pw.println("@ATTRIBUTE " + symKey + "prevSlope NUMERIC");
			pw.println("@ATTRIBUTE " + symKey + "prevYinterc NUMERIC");
			pw.println("@ATTRIBUTE " + symKey + "prev2Slope NUMERIC");
			pw.println("@ATTRIBUTE " + symKey + "prev2Yinterc NUMERIC");
			pw.println("@ATTRIBUTE " + symKey + "prevCPSlope NUMERIC");
			pw.println("@ATTRIBUTE " + symKey + "prevCPYinterc NUMERIC");
			pw.println("@ATTRIBUTE " + symKey + "prev2CPSlope NUMERIC");
			pw.println("@ATTRIBUTE " + symKey + "prev2CPYinterc NUMERIC");
			pw.println("@ATTRIBUTE " + symKey + "prev2PrevSlope NUMERIC");
			pw.println("@ATTRIBUTE " + symKey + "prev2PrevYinterc NUMERIC");
			hmaDates.put(symKey, pgsd.inDate);
			if (startDate.compareTo(pgsd.inDate[mali.firstXOverStartsAt]) < 0)
				startDate = pgsd.inDate[mali.firstXOverStartsAt + 50];
		}
		br.close();

		pw.println(db.getAttributeDefinition());

		pw.println("@DATA");

		int arraypos[] = new int[maAndLIs.size()];
		int pos = 100;
		while (gsd.inDate[pos].compareTo(startDate) < 0)
			pos++;
		pos += 20;

		eemindexLoop: for (int iday = pos; iday < gsd.inDate.length - daysOut - 1;) {
			String posDate = gsd.inDate[iday];
			pos = 0;

			for (String key : maAndLIs.keySet()) {
				String sdate = hmaDates.get(key)[arraypos[pos]];
				int dcomp = posDate.compareTo(sdate);
				if (dcomp < 0) {
					iday++;
					continue eemindexLoop;
				}
				if (dcomp > 0) {
					arraypos[pos]++;
					continue eemindexLoop;
				}
				pos++;
			}

			Date now = sdf.parse(posDate);

			Calendar cdMove = Calendar.getInstance();
			cdMove.setTime(now);
			int idt = 1;
			int daysOutCalc = 0;
			while (true) {
				cdMove.add(Calendar.DAY_OF_MONTH, +1);
				if (cdMove.get(Calendar.DAY_OF_WEEK) == Calendar.SATURDAY)
					continue;
				if (cdMove.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY)
					continue;
				daysOutCalc++;
				idt++;
				if (idt > daysOut)
					break;
			}
			String endDate = gsd.inDate[iday + daysOut];
			if (daysOutCalc != daysOut)
				System.out.println(posDate + " here " + endDate + " by " + daysOutCalc + " not " + daysOut);

			printAttributeData(iday, daysOut, pw, maAndLIs, hmaDates, arraypos, gsd.inClose, db);
			iday++;
			for (pos = 0; pos < arraypos.length; pos++) {
				arraypos[pos]++;
			}

		}

		pw.close();

		Classifier classifier = AbstractClassifier.forName("weka.classifiers.lazy.IBk",
				new String[] { "-K", "48", "-X", });
		Instances inst = new Instances(new FileReader(file));
		inst.setClassIndex(inst.numAttributes() - 1);
		classifier.buildClassifier(inst);
		Evaluation eval = new Evaluation(inst);
		eval.crossValidateModel(classifier, inst, 10, new Random());
		double cm[][] = eval.confusionMatrix();
		for (double row[] : cm) {
			for (double col : row) {
				System.out.print(col + ";");
			}
			System.out.println();
		}
		System.out.println("\n\n\n");
		IBk ibk = new IBk();
		eval.crossValidateModel(ibk, inst, 10, new Random());

		for (double row[] : cm) {
			for (double col : row) {
				System.out.print(col + ";");
			}
			System.out.println();
		}

	}

	static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

	public static String getAttributeText(MovingAvgAndLineIntercept mali, int hmastart, int daysOut) throws Exception {

		String date = mali.getDate(hmastart);
		Line cl = mali.getCurrentLineIntercept(date, 1);
		Line pl = mali.getCurrentLineIntercept(date, 2);
		Line p2 = mali.getCurrentLineIntercept(date, 2);
		Line tl = mali.getInterceptBetweenPoints(date, 1, 2);
		Line t2 = mali.getInterceptBetweenPoints(date, 1, 3);
		Line t3 = mali.getInterceptBetweenPoints(date, 2, 3);
		return cl.slope + "," + cl.yintersect + "," + pl.slope + "," + pl.yintersect + "," + p2.slope + ","
				+ p2.yintersect + "," + tl.slope + "," + tl.yintersect + "," + t2.slope + "," + t2.yintersect + ","
				+ t3.slope + "," + t3.yintersect + ",";

	}

//	Classifier classifier = AbstractClassifier.forName("weka.classifiers.lazy.KStar", new String[]{"-B", "60", "-M", "n"});
//	classifier.buildClassifier(instances);
	public static void getAttributeData(int iday, int daysOut, PrintWriter pw, TreeMap<String, Object> hmas,
			int[] arraypos, double[] closes, DeltaBands priceBands) throws Exception {
		int pos = 0;
		for (String key : hmas.keySet()) {
			MovingAvgAndLineIntercept hma = (MovingAvgAndLineIntercept) hmas.get(key);

			int hmastart = arraypos[pos];

			pw.print(getAttributeText(hma, hmastart, daysOut));

			pos++;

		}

		pw.print(priceBands.getAttributeValue(iday, daysOut, closes));
		pw.println();
		pw.flush();

	}

	public static void printAttributeData(int iday, int daysOut, PrintWriter pw, TreeMap<String, Object> hmas,
			TreeMap<String, String[]> hmaDates, int[] arraypos, double[] closes, DeltaBands priceBands)
			throws Exception {

		int pos = 0;
		String retString = priceBands.getAttributeValue(iday, daysOut, closes);

		for (String key : hmas.keySet()) {
			MovingAvgAndLineIntercept mali = (MovingAvgAndLineIntercept) hmas.get(key);

			int hmastart = arraypos[pos];

			pw.print(getAttributeText(mali, hmastart, daysOut));

			pos++;

		}
		pw.println(retString);

	}

}
