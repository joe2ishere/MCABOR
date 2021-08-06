package correlation;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import com.americancoders.dataGetAndSet.GetETFDataUsingSQL;
import com.tictactec.ta.lib.Core;
import com.tictactec.ta.lib.MInteger;

import util.Realign;
import util.getDatabaseConnection;
import utils.TopAndBottomList;

public class NATRCorrelation implements Runnable {
	static TreeMap<String, TopAndBottomList> symbolsBest = new TreeMap<>();
	static DecimalFormat df = new DecimalFormat("0.0000");
	static TreeMap<String, Integer> symList = new TreeMap<>();
	static TreeSet<String> doneList = new TreeSet<>();
	static TreeMap<String, GetETFDataUsingSQL> gsds = new TreeMap<>();

	public static void main(String[] args) throws Exception {

		Connection conn = getDatabaseConnection.makeConnection();
		PreparedStatement psDate = conn
				.prepareStatement("select distinct txn_date  from etfprices order by txn_date desc limit 1");
		ResultSet rsDate = psDate.executeQuery();
		rsDate.next();
		String latestDate = rsDate.getString(1);

		PreparedStatement ps = conn.prepareStatement(
				"select symbol, AVG(volume) AS vavg, count(*) as txncnt  from etfprices  GROUP BY symbol ORDER BY symbol");
		ResultSet rs = ps.executeQuery();

		while (rs.next()) {

			String sym = rs.getString(1).toLowerCase();
			double vol = rs.getDouble(2);
			int cnt = rs.getInt(3);

			if (vol < 10000) {
				System.out.println("skipping " + sym + " with vol of " + vol);
				continue;
			}

			if (cnt < 2000) {
				System.out.println("skipping " + sym + " with cnt of " + cnt);
				continue;

			}

			GetETFDataUsingSQL pgsd = GetETFDataUsingSQL.getInstance(sym);
			if (pgsd.inDate[pgsd.inDate.length - 1].compareTo(latestDate) != 0) {
				System.out.println("skipping " + sym + " with end date of  " + pgsd.inDate[pgsd.inDate.length - 1]);
				continue;

			}

			symList.put(sym, cnt);

			gsds.put(sym, pgsd);

		}

		int threadCnt = 5;

		BlockingQueue<String> natrQue = new ArrayBlockingQueue<String>(threadCnt);
		NATRCorrelation corrs = new NATRCorrelation(natrQue);

		for (int i = 0; i < threadCnt; i++) {
			Thread thrswu = new Thread(corrs);
			thrswu.start();
		}

		for (String natrSym : symList.keySet()) {
			if (doneList.contains(natrSym))
				continue;
			if (symList.get(natrSym) < 2000)
				continue;
			natrQue.put(natrSym);

		}

		for (int i = 0; i < threadCnt; i++)
			natrQue.put("<<<STOP>>>");
		while (natrQue.isEmpty() == false)
			Thread.sleep(1000);

	}

	Core core = new Core();
	BlockingQueue<String> natrQue;

	static double bestCorrelation = -100;

	public NATRCorrelation(BlockingQueue<String> natrQue) {
		this.natrQue = natrQue;
		loadTables();

	}

	File doneFile = new File("natrDone.txt");
	File tabFile = new File("natrTaB.txt");

	void loadTables() {

		if (doneFile.exists() == false)
			return;
		try {
			System.out.println("start load");
			FileReader fr = new FileReader(doneFile);
			BufferedReader br = new BufferedReader(fr);
			String in = "";
			while ((in = br.readLine()) != null)
				doneList.add(in.trim());
			fr.close();
			fr = new FileReader(tabFile);
			br = new BufferedReader(fr);
			in = "";
			while ((in = br.readLine()) != null) {

				TopAndBottomList tab = symbolsBest.get(in);
				if (tab == null) {
					tab = new TopAndBottomList();
					symbolsBest.put(in, tab);
				}
				for (int i = 0; i < tab.size; i++) {
					in = br.readLine();
					// 0:0.16974685716495913:efu;aapl;6;22;6;1;16;0.1697
					String ins[] = in.split(":");
					tab.setTop(Double.parseDouble(ins[1]), ins[2]);
				}

			}
			fr.close();
			System.out.println("end of load tables");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	void dumpTables() {
		synchronized (symbolsBest) {

			try {
				System.out.println("start dump");
				PrintWriter pw = new PrintWriter(doneFile);
				for (String doneSym : doneList) {
					pw.println(doneSym);
				}
				pw.flush();
				pw.close();
				pw = new PrintWriter(tabFile);
				for (String bestSym : symbolsBest.keySet()) {
					pw.println(bestSym);
					TopAndBottomList tab = symbolsBest.get(bestSym);
					for (int i = 0; i < tab.size; i++) {
						pw.println(i + ":" + tab.getTopValue(i) + ":" + tab.getTopDescription()[i]);
					}
					pw.flush();
				}
				pw.close();
				System.out.println("end of dump tables");
			} catch (FileNotFoundException e) {

				e.printStackTrace();
			}
		}

	}

	@Override
	public void run() {

		try {
			while (true) {

				String functionSymbol = natrQue.take();

				if (functionSymbol.contains("<<<STOP>>>"))
					return;
				if (symList.get(functionSymbol) < 2500)
					continue;

				System.out.println("running " + functionSymbol);
				GetETFDataUsingSQL gsdDM = gsds.get(functionSymbol);

				for (int natrPeriod = 2; natrPeriod <= 20; natrPeriod += 2) {
					{

						double natr[] = new double[gsdDM.inClose.length];
						MInteger outBegInatr = new MInteger();
						MInteger outNBElement = new MInteger();
						core.natr(0, gsdDM.inClose.length - 1, gsdDM.inHigh, gsdDM.inLow, gsdDM.inClose, natrPeriod,
								outBegInatr, outNBElement, natr);

						Realign.realign(natr, outBegInatr.value);
						for (String closeSymbol : symList.keySet()) {

							GetETFDataUsingSQL gsdPrice = gsds.get(closeSymbol);

							for (int natrBack = 2; natrBack < 30; natrBack += 2) {
								for (int pricefunctionDaysDiff = 1; pricefunctionDaysDiff <= 30; pricefunctionDaysDiff += 1) {
									ArrayList<Double> ccArray1 = new ArrayList<>();
									ArrayList<Double> ccArray2 = new ArrayList<>();
									for (int priceIx = 50, natrIx = 50; priceIx < gsdPrice.inDate.length - pricefunctionDaysDiff
											& natrIx < gsdDM.inDate.length - pricefunctionDaysDiff;) {
										int dcomp = gsdPrice.inDate[priceIx].compareTo(gsdDM.inDate[natrIx]);
										if (dcomp < 0) {
											priceIx++;
											continue;
										}
										if (dcomp > 0) {
											natrIx++;
											continue;
										}

										ccArray1.add(
												gsdPrice.inClose[priceIx + pricefunctionDaysDiff] / gsdPrice.inClose[priceIx]);
										ccArray2.add(natr[natrIx] / natr[natrIx - natrBack]);

										if (gsdPrice.getInDate()[priceIx].compareTo(gsdDM.getInDate()[natrIx]) != 0) {
											System.out.println("here");
										}
										priceIx++;
										natrIx++;
									}

									double corr = Correlation.pearsonsCorrelation(ccArray1, ccArray2);
									if (Double.isInfinite(corr))
										continue;
									double absCorr = Math.abs(corr);
									synchronized (symbolsBest) {
										TopAndBottomList tab = symbolsBest.get(closeSymbol + "_" + pricefunctionDaysDiff);
										if (tab == null) {
											tab = new TopAndBottomList();
											symbolsBest.put(closeSymbol + "_" + pricefunctionDaysDiff, tab);
										}
										if (absCorr < tab.getTopValue(0)
												& tab.getTopDescription()[0].contains(functionSymbol))
											continue;
										tab.setTop(absCorr, makeKey(closeSymbol, pricefunctionDaysDiff, functionSymbol, natrPeriod,
												natrBack, corr));

										if (absCorr > bestCorrelation) {
											System.out.println("best so far " + absCorr);
											bestCorrelation = absCorr;
										}
									}

								}
							}
						}
						// if (bestCorrelation > .5)

					}
				}

				doneList.add(functionSymbol);
				dumpTables();
				System.out.println("done with " + functionSymbol);
			}
		} catch (

		InterruptedException e1) {

			e1.printStackTrace();
			return;
		} catch (NumberFormatException e1) {

			e1.printStackTrace();
		} catch (Exception e1) {

			e1.printStackTrace();
		}

	}

	public static String getCloseSym(String keyIn) {
		String ins[] = keyIn.split(";");
		return ins[0];
	}

	public static int getPricefunctionDaysDiff(String keyIn) {
		String ins[] = keyIn.split(";");
		return Integer.parseInt(ins[1]);
	}

	public static String getfunctionSymbol(String keyIn) {
		String ins[] = keyIn.split(";");
		return ins[2];
	}

	public static int getNATRPeriod(String keyIn) {
		String ins[] = keyIn.split(";");
		return Integer.parseInt(ins[3]);
	}

	public static int getNATRDaysBack(String keyIn) {
		String ins[] = keyIn.split(";");
		return Integer.parseInt(ins[4]);
	}

	public static int getDoubleBack(String keyIn) {
		String ins[] = keyIn.split(";");
		return Integer.parseInt(ins[5]);
	}

	public static double getCorr(String keyIn) {
		String ins[] = keyIn.split(";");
		return Double.parseDouble(ins[6]);
	}

	private String makeKey(String closeSymbol, int pricefunctionDaysDiff, String functionSymbol, int natrPeriod, int natrBack,
			double corr) {

		return closeSymbol + ";" + pricefunctionDaysDiff + ";" + functionSymbol + ";" + natrPeriod + ";" + natrBack + ";" + corr;
	}

}
