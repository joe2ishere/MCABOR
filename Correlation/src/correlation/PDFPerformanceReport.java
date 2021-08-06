package correlation;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.logging.LogManager;

import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.sax.SAXResult;
import javax.xml.transform.stream.StreamSource;

import org.apache.fop.apps.Fop;
import org.apache.fop.apps.FopFactory;
import org.apache.fop.apps.MimeConstants;

public class PDFPerformanceReport {

	public static void main(String args[]) throws Exception {
		LogManager.getLogManager().reset();
		FileInputStream fis = new FileInputStream("xmlFilesForPDFReports/Report-Performance-FO.xml");
		makeReport(fis, "test");

	}

	static String makeReport(InputStream is, String rptDate) throws Exception {

		FopFactory fopFactory = FopFactory.newInstance(new File("xmlFilesForPDFReports/fop.xconf"));

		// Step 2: Set up output stream.
		// Note: Using BufferedOutputStream for performance reasons (helpful with
		// FileOutputStreams).

		String outputFileName = "c:/users/joe/correlationOutput/reportPerformance_" + rptDate + ".pdf";
		OutputStream out = new BufferedOutputStream(new FileOutputStream(outputFileName));

		try {
			// Step 3: Construct fop with desired output format

			Fop fop = fopFactory.newFop(org.apache.xmlgraphics.util.MimeConstants.MIME_PDF, out);

			// Step 4: Setup JAXP using identity transformer
			TransformerFactory factory = TransformerFactory.newInstance();
			Transformer transformer = factory.newTransformer(); // identity transformer

			// Step 5: Setup input and output for XSLT transformation
			// Setup input stream
			Source src = new StreamSource(is);

			// Resulting SAX events (the generated FO) must be piped through to FOP
			Result res = new SAXResult(fop.getDefaultHandler());

			// Step 6: Start XSLT transformation and FOP processing
			transformer.transform(src, res);

		} catch (Exception e) {
			e.printStackTrace();
		} finally {

			// Clean-up
			out.close();
		}

		return outputFileName;

	}
}
