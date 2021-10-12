package correlation.MutualFund;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import com.americancoders.dataGetAndSet.GetETFDataUsingSQL;
import com.americancoders.lineIntersect.Line;
import com.tictactec.ta.lib.MAType;

import correlation.Correlation;
import movingAvgAndLines.MovingAvgAndLineIntercept;
import util.getDatabaseConnection;
import utils.TopAndBottomList;

public class MALinesCorrelationMutualFunds implements Runnable {

	static TreeMap<String, GetETFDataUsingSQL> gsds = new TreeMap<>();
	static TreeMap<String, TopAndBottomList> symbolsBest = new TreeMap<>();
	static TreeMap<String, ArrayList<Double>> mfcloses = new TreeMap<>();
	static TreeMap<String, ArrayList<String>> mfdates = new TreeMap<>();

	public static void main(String[] args) throws Exception {
		Connection conn = getDatabaseConnection.makeConnection();
		PreparedStatement ps = conn.prepareStatement(
				"select symbol, AVG(volume) AS vavg, count(*) as txncnt  from etfprices  GROUP BY symbol ORDER BY symbol");

		ResultSet rs = ps.executeQuery();

		while (rs.next()) {

			String sym = rs.getString(1).toLowerCase();
			double vol = rs.getDouble(2);
			int cnt = rs.getInt(3);

			if (vol < 25000) {
				System.out.println("skipping " + sym + " with vol of " + vol);
				continue;
			}

			if (cnt < 2000) {
				System.out.println("skipping " + sym + " with cnt of " + cnt);
				continue;

			}

			GetETFDataUsingSQL pgsd = GetETFDataUsingSQL.getInstance(sym);

			gsds.put(sym, pgsd);

		}

		PreparedStatement pmaLinesF = conn.prepareStatement("select distinct name from fidelitymutualfunds");
		PreparedStatement pmaLinesFGet = conn
				.prepareStatement("select dt, close from fidelitymutualfunds where name = ? order by dt");
		ResultSet rmaLinesF = pmaLinesF.executeQuery();

		while (rmaLinesF.next()) {
			String mfName = rmaLinesF.getString(1);
			pmaLinesFGet.setString(1, mfName);
			ArrayList<Double> closes = new ArrayList<>();
			ArrayList<String> dates = new ArrayList<>();
			mfcloses.put(mfName, closes);
			mfdates.put(mfName, dates);
			ResultSet rmaLinesFGet = pmaLinesFGet.executeQuery();
			while (rmaLinesFGet.next()) {
				closes.add((double) rmaLinesFGet.getFloat(2));
				dates.add(rmaLinesFGet.getString(1));
			}

		}

		BlockingQueue<String> maLinesiQue = new ArrayBlockingQueue<String>(4);
		MALinesCorrelationMutualFunds corrs = new MALinesCorrelationMutualFunds(maLinesiQue);

		for (int i = 0; i < 4; i++) {
			Thread thrswu = new Thread(corrs);
			thrswu.start();
		}

		for (String maLinesiSym : gsds.keySet()) {
//			if (maLinesiSym.compareTo("c") > 0)
//				break;

			maLinesiQue.put(maLinesiSym);

		}

		for (int i = 0; i < 4; i++) {
			maLinesiQue.put("<<<STOP>>>");
		}
		while (maLinesiQue.isEmpty() == false)
			Thread.sleep(1000);
	}

	public static void writeFile() throws FileNotFoundException {

		PrintWriter pw = new PrintWriter("maLinesCorrelationOverSignal_30days_MutualFunds.csv");
		pw.println("pos;etf;mf;closeSym;Period;DaysDiff;matype;corr");

		for (String sym : symbolsBest.keySet()) {
			pw.println(sym);
			TopAndBottomList tab = symbolsBest.get(sym);
			pw.println(tab.getTop());
		}
		pw.flush();
		pw.close();

	}

	BlockingQueue<String> q;
	double bestCorr = 0;

	public MALinesCorrelationMutualFunds(BlockingQueue<String> maLinesiQue) {
		this.q = maLinesiQue;

	}

	@Override
	public void run() {

		try {
			while (true) {
				String maLinesiSym = q.take();
				if (maLinesiSym.contains("<<<STOP>>>"))
					return;

				GetETFDataUsingSQL pgsd = gsds.get(maLinesiSym);

				System.out.println("running with " + pgsd.getSymbol());

				MAType[] types = { MAType.Sma, MAType.Ema, MAType.Dema, MAType.Tema };
				for (int maLinesPeriod = 2; maLinesPeriod <= 31; maLinesPeriod += 2) {

					Line ln, ln2, ln3, ln4;

					for (int priceDaysDiff = 30; priceDaysDiff <= 45; priceDaysDiff += 1) {
						{
							for (String closeSym : mfcloses.keySet()) {

								ArrayList<Double> closes = mfcloses.get(closeSym);
								ArrayList<String> dates = mfdates.get(closeSym);

								newType: for (MAType type : types) {

									int maDayIndex = 100, closingDayIndex = 100;
									while (dates.get(closingDayIndex).compareTo(pgsd.inDate[maDayIndex]) > 0)
										maDayIndex++;
									while (dates.get(closingDayIndex).compareTo(pgsd.inDate[maDayIndex]) < 0)
										closingDayIndex++;

									MovingAvgAndLineIntercept mali = new MovingAvgAndLineIntercept(pgsd, maLinesPeriod,
											type, maLinesPeriod, type);
									int startMADay = maDayIndex;
									int startClosingDay = closingDayIndex;
									{
										ArrayList<Double> ccArray1 = new ArrayList<Double>(),
												ccArray2 = new ArrayList<Double>();

										for (maDayIndex = startMADay, closingDayIndex = startClosingDay; maDayIndex < pgsd.inDate.length
												- priceDaysDiff
												& closingDayIndex < pgsd.inDate.length - priceDaysDiff;) {
											String MADate = pgsd.inDate[maDayIndex];
											String closingDate = pgsd.inDate[closingDayIndex];
											int dcomp = MADate.compareTo(closingDate);
											if (dcomp < 0) {
												maDayIndex++;
												continue;
											}
											if (dcomp > 0) {
												closingDayIndex++;
												continue;
											}

											if (pgsd.inDate[maDayIndex].compareTo(pgsd.inDate[closingDayIndex]) != 0) {
												System.out.println("logic error comparing dates");
												System.exit(-2);
											}

											try {
												ln = mali.getCurrentLineIntercept(pgsd.inDate[closingDayIndex], 1);
												ln2 = mali.getCurrentLineIntercept(pgsd.inDate[closingDayIndex], 2);
												ln3 = mali.getCurrentLineIntercept(pgsd.inDate[closingDayIndex], 3);
												ln4 = mali.getCurrentLineIntercept(pgsd.inDate[closingDayIndex], 4);
												if (ln == null | ln2 == null | ln3 == null) {
													maDayIndex++;
													closingDayIndex++;
													continue;
												}
											} catch (Exception e) {
												maDayIndex++;
												closingDayIndex++;
												continue;
											}
											try {
												double yln = pgsd.inClose[maDayIndex + priceDaysDiff] * ln.slope
														+ ln.yintersect;
												double yln2 = pgsd.inClose[maDayIndex + priceDaysDiff] * ln2.slope
														+ ln2.yintersect;
												double yln3 = pgsd.inClose[maDayIndex + priceDaysDiff] * ln3.slope
														+ ln3.yintersect;
												double yln4 = pgsd.inClose[maDayIndex + priceDaysDiff] * ln4.slope
														+ ln4.yintersect;
												ccArray1.add(closes.get(closingDayIndex + priceDaysDiff)
														/ closes.get(closingDayIndex));
												ccArray2.add(
														((yln + yln2 + yln3 + yln4) / 4) / pgsd.inClose[maDayIndex]);
												maDayIndex++;
												closingDayIndex++;
											} catch (Exception e) {

												continue newType;
											}

										}
										double corr = Correlation.pearsonsCorrelation(ccArray1, ccArray2);
										if (Double.isInfinite(corr))
											continue;
										if (Double.isNaN(corr))
											continue;

										synchronized (symbolsBest) {
											TopAndBottomList tab = symbolsBest.get(closeSym + "_" + priceDaysDiff);
											if (tab == null)
												tab = new TopAndBottomList();
											corr = Math.abs(corr);
											int setAt = tab.setTop(corr,
													pgsd.getSymbol() + ";" + closeSym + ";" + maLinesPeriod + ";"
															+ priceDaysDiff + ";" + type.name() + ";"
															+ df.format(corr));

											if (setAt != -1) {
												if (setAt > 0) {
													for (int i = 0; i < setAt; i++) {
														if (tab.getTopDescription()[i].contains(pgsd.getSymbol() + ";")
																& tab.getTopDescription()[i].contains(type.name()))
															tab.removeFromTop(setAt);

													}
												}
												for (int i = setAt + 1; i < tab.size; i++) {
													if (tab.getTopDescription()[i].contains(pgsd.getSymbol() + ";")
															& tab.getTopDescription()[i].contains(type.name()))
														tab.removeFromTop(i);
												}
											}

											symbolsBest.put(closeSym + "_" + priceDaysDiff, tab);
											if (corr > bestCorr) {
												bestCorr = corr;
												System.out.println("best so far is " + corr);

											}

										}
									}

								}
							}
						}
					}
				}
				System.out.println("done with " + pgsd.getSymbol());
				synchronized (symbolsBest) {
					writeFile();
				}
			}
		} catch (

		InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	static DecimalFormat df = new DecimalFormat("0.0000");
}