package correlation.ARFFMaker.Parms;

import movingAvgAndLines.MovingAvgAndLineIntercept;

public class MaLineParmToPass {
	public MovingAvgAndLineIntercept mali;
	public double[] closes;
	public String processDate;

	public MaLineParmToPass(MovingAvgAndLineIntercept mali, double[] closes, String processDate) {
		this.mali = mali;
		this.closes = closes;
		this.processDate = processDate;
	}

}
