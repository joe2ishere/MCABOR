package correlation;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.TreeMap;

import com.americancoders.dataGetAndSet.GetETFDataUsingSQL;
import com.tictactec.ta.lib.Core;
import com.tictactec.ta.lib.MAType;

import bands.DeltaBands;
import movingAvgAndLines.MovingAvgAndLineIntercept;
import weka.classifiers.Classifier;
import weka.classifiers.lazy.KStar;
import weka.core.Instances;

public class MAAveragerCorrelationEstimator extends CorrelationEstimator {

	Core core = new Core();
	MAAveragesMakeARFFfromSQL makeSQL = new MAAveragesMakeARFFfromSQL(false);

	public MAAveragerCorrelationEstimator(Connection conn) throws SQLException {
		super(conn);
		psSelect = conn.prepareStatement(
				"select " + "symbol,   toCloseDays, significantPlace, functionSymbol, period, matype, correlation"
						+ "  from  maline_correlation"
						+ " where symbol = ? and toCloseDays = ?  order by symbol, toCloseDays, significantplace");
		function = "maAvg";
	}

	@Override
	public void buildHeaders(PrintWriter pw) throws Exception {

		ResultSet rsSelect = psSelect.executeQuery();

		myParms = new TreeMap<String, Object>();

		nextFunctionSymbol: while (rsSelect.next()) {

			String functionSymbol = rsSelect.getString("functionSymbol");

			GetETFDataUsingSQL pgsd;
			synchronized (gsds) {
				pgsd = gsds.get(functionSymbol);
				if (pgsd == null) {
					// pgsd = GetETFDataUsingSQL.getInstance(functionSymbol);
					if (pgsd == null) {
						System.err.println(
								" *** " + this.function + " is missing data from function symbol =>" + functionSymbol);
						continue nextFunctionSymbol;
					}
					gsds.put(functionSymbol, pgsd);
				}

			}

			if (pgsd.inDate[pgsd.inDate.length - 1].compareTo(currentMktDate) != 0) {
				System.out.println("missing data for " + functionSymbol);
				continue;
			}
			if (startDate.compareTo(pgsd.inDate[50]) < 0)
				startDate = pgsd.inDate[50];

			int period = rsSelect.getInt("period");
			String matype = rsSelect.getString("maType");
			MAType maType = null;
			for (MAType types : MAType.values()) {
				if (types.name().compareTo(matype) == 0) {
					maType = types;
					break;
				}
			}

			MovingAvgAndLineIntercept mal = new MovingAvgAndLineIntercept(pgsd, period, maType, period, maType);
			String symKey = function + "_" + functionSymbol + "_" + rsSelect.getInt("significantPlace");
			MaLineParmToPass ptp = new MaLineParmToPass(mal, pgsd.inClose, processDate);
			myParms.put(symKey, ptp);
			functionDaysDiffMap.put(symKey, 0);
			pw.println("@ATTRIBUTE " + symKey + "maAVG NUMERIC");

			dates.put(symKey, pgsd.inDate);

		}

	}

	@Override
	public void printAttributeData(int iday, int daysOutCalc, PrintWriter pw,
			TreeMap<String, Integer> functionDaysDiffMap, TreeMap<String, Integer> doubleBacks, int[] arraypos,
			double[] inClose, DeltaBands priceBands) {
		try {
			makeSQL.printAttributeData(iday, daysOutCalc, pw, myParms, arraypos, inClose, processDate, priceBands,
					true);
		} catch (Exception e) {

			e.printStackTrace();
			System.exit(0);
		}

	}

	@Override
	protected void getAttributeText(PrintWriter pw, Object myParm, int functionDaysDiff, int start, int doubleBack) {

		makeSQL.getAttributeText(pw, (MaLineParmToPass) myParm, functionDaysDiff);

	}

	Classifier thisClassifier;

	public Classifier getClassifier() {
		return thisClassifier;
	}

	@Override
	public double drun(Instances instances) throws Exception {
		Classifier classifier = new KStar();
		classifier.buildClassifier(instances);
		return classifier.classifyInstance(instances.get(instances.size() - 1));

	}

}
