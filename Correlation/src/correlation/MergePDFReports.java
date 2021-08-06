package correlation;

import java.io.File;

import org.apache.pdfbox.io.MemoryUsageSetting;
import org.apache.pdfbox.multipdf.PDFMergerUtility;

public class MergePDFReports {

	public static void main(String[] args) throws Exception {
		PDFMergerUtility merger = new PDFMergerUtility();
		merger.setDestinationFileName("BothReports.pdf");
		merger.addSource(new File("c:/users/joe/correlationOutput/report_2020-05-22.pdf"));
		merger.addSource(new File("c:/users/joe/correlationOutput/reportPerformance_2020-05-27.pdf"));
		merger.mergeDocuments(MemoryUsageSetting.setupMainMemoryOnly());

	}

}
