package correlation;

import movingAvgAndLines.MovingAvgAndLineIntercept;

public class MaLineParmToPass {
	MovingAvgAndLineIntercept mali;
	double[] closes;
	public String processDate;

	public MaLineParmToPass(MovingAvgAndLineIntercept mali, double[] closes, String processDate) {
		this.mali = mali;
		this.closes = closes;
		this.processDate = processDate;
	}

}
