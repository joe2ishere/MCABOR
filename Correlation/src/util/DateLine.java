package util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;

public class DateLine {

	public static void main(String[] args) {

		try {
			System.out.println(dateLine("2022-01-13", 10));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static String dateLine(String start, int cnt) throws Exception {
		FileReader fr = new FileReader("holidays.txt");
		BufferedReader br = new BufferedReader(fr);
		String in = "";
		ArrayList<String> holidays = new ArrayList<String>();
		while ((in = br.readLine()) != null) {
			holidays.add(in);
		}
		br.close();
		StringBuffer ret = new StringBuffer();

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		SimpleDateFormat sdf2 = new SimpleDateFormat("MM-dd");
		Date dt = sdf.parse(start);
		Calendar cd = Calendar.getInstance();
		cd.setTime(dt);

		int i = 0;
		while (i < cnt) {
			cd.add(Calendar.DAY_OF_YEAR, 1);
			if (cd.get(Calendar.DAY_OF_WEEK) == Calendar.SATURDAY)
				continue;
			if (cd.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY)
				continue;

			String dt$ = sdf.format(cd.getTime());
			if (holidays.contains(dt$))
				continue;
			dt$ = sdf2.format(cd.getTime());
			ret.append(";");
			ret.append(dt$);
			i++;

		}

		return ret.toString();
	}

	public static String forecastDateLine(String start, int cnt) throws Exception {
		FileReader fr = new FileReader("holidays.txt");
		BufferedReader br = new BufferedReader(fr);
		String in = "";
		ArrayList<String> holidays = new ArrayList<String>();
		while ((in = br.readLine()) != null) {
			holidays.add(in);
		}
		br.close();
		StringBuffer ret = new StringBuffer();

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		SimpleDateFormat sdf2 = new SimpleDateFormat("MMM dd");
		Date dt = sdf.parse(start);
		Calendar cd = Calendar.getInstance();
		cd.setTime(dt);
		int i = 0;
		while (i < cnt) {
			cd.add(Calendar.DAY_OF_YEAR, 1);
			if (cd.get(Calendar.DAY_OF_WEEK) == Calendar.SATURDAY)
				continue;
			if (cd.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY)
				continue;

			String dt$ = sdf.format(cd.getTime());
			if (holidays.contains(dt$))
				continue;
			dt$ = sdf2.format(cd.getTime());
			ret.append("<fo:table-cell><fo:block>");
			ret.append(dt$);
			ret.append("</fo:block></fo:table-cell>");
			i++;

		}

		return ret.toString();
	}

	public static ArrayList<String> forecast5DaysLine(String start, int cnt) throws Exception {
		FileReader fr = new FileReader("holidays.txt");
		BufferedReader br = new BufferedReader(fr);
		String in = "";
		ArrayList<String> holidays = new ArrayList<String>();
		while ((in = br.readLine()) != null) {
			holidays.add(in);
		}
		br.close();
		StringBuffer ret = new StringBuffer();

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		SimpleDateFormat sdf2 = new SimpleDateFormat("MMM dd");
		Date dt = sdf.parse(start);
		Calendar cd = Calendar.getInstance();
		cd.setTime(dt);
		int i = 0;
		ArrayList<String> retArray = new ArrayList<>();
		while (i < cnt) {
			cd.add(Calendar.DAY_OF_YEAR, 1);
			if (cd.get(Calendar.DAY_OF_WEEK) == Calendar.SATURDAY)
				continue;
			if (cd.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY)
				continue;

			String dt$ = sdf.format(cd.getTime());
			if (holidays.contains(dt$))
				continue;

			switch (i) {
			case 0, 5, 10, 15, 20, 25:
				dt$ = sdf2.format(cd.getTime());
				ret.append(dt$ + " - ");
				break;
			case 4, 9, 14, 19, 24, 29:
				dt$ = sdf2.format(cd.getTime());
				ret.append(dt$);
				retArray.add(ret.toString());
				ret = new StringBuffer();
				break;
			default:
				break;
			}

			i++;

		}

		return retArray;
	}

}
