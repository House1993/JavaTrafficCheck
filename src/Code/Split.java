package Code;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class Split {

	private static final int THIRTY_MINUTES = 1800;

	private static final String rawFolder = "./Raw/Routes/";

	private static List<String> timeStrList = new ArrayList<String>();
	private static List<Long> timeList = new ArrayList<Long>();
	private static List<Double> longitudeList = new ArrayList<Double>();
	private static List<Double> latitudeList = new ArrayList<Double>();
	private static List<Double> mileageList = new ArrayList<Double>();
	private static List<Double> vList = new ArrayList<Double>();
	private static List<Double> instantaneousVList = new ArrayList<Double>();

	static Configuration cfg = HBaseConfiguration.create();

	public void Work() throws Exception {
		File folder = new File(rawFolder);
		File[] files = folder.listFiles();
		for (int k = 0; k < files.length; k++) {
			int length = 0;
			try {
				length = ExtractInfo(files[k].getPath());
			} catch (Exception e) {
				e.printStackTrace();
			}
			if (length < 2)
				continue; // 0 or 1 record
			String fileName = files[k].getName().split("\\.")[0];
			int rowWritten = -1;
			int lastdiff = 0;
			Long lastTime = Long.MAX_VALUE;
			Long timeI = timeList.get(0);
			double mileageI = mileageList.get(0);
			for (int i = 0, j = 1; j < length; i++, j++) {
				Long timeJ = timeList.get(j);
				double mileageJ = mileageList.get(j);
				if (mileageI == mileageJ) {
					if (rowWritten != -1 && timeJ - lastTime >= THIRTY_MINUTES) {
						if (rowWritten != lastdiff) {
							WriteHbase(rowWritten, lastdiff, fileName);
						}
						rowWritten = -1;
					}
				} else {
					lastdiff = i;
					lastTime = timeI;
					if (rowWritten == -1)
						rowWritten = i;
				}
				timeI = timeJ;
				mileageI = mileageJ;
			}
			if (rowWritten != -1 && rowWritten < length - 1) {
				WriteHbase(rowWritten, length - 1, fileName);
			}
		}
	}

	private int ExtractInfo(String path) {
		timeStrList.clear();
		timeList.clear();
		longitudeList.clear();
		latitudeList.clear();
		mileageList.clear();
		instantaneousVList.clear();
		vList.clear();
		List csv = new ArrayList();
		try {
			csv = myutil.ReadCSV(path);
		} catch (Exception e) {
			e.printStackTrace();
		}
		int len = csv.size();
		for (int i = 0; i < len; i++) {
			String[] csvRow = (String[]) csv.get(i);
			String timeStr = csvRow[1];
			Long time = myutil.DateStrToLong(timeStr);
			double lon = Double.parseDouble(csvRow[2]);
			double lat = Double.parseDouble(csvRow[3]);
			double mil = Double.parseDouble(csvRow[8]);
			double insv = Double.parseDouble(csvRow[4]) / 3.6;
			timeStrList.add(timeStr);
			timeList.add(time);
			longitudeList.add(lon);
			latitudeList.add(lat);
			mileageList.add(mil);
			instantaneousVList.add(insv);
		}
		for (int i = 0; i < len; i++) {
			double mil = 0;
			double tim = 0;
			if (i == 0) {
				mil = mileageList.get(1) - mileageList.get(0);
				tim = timeList.get(1) - timeList.get(0);
			} else if (i == len - 1) {
				mil = mileageList.get(i) - mileageList.get(i - 1);
				tim = timeList.get(i) - timeList.get(i - 1);
			} else {
				mil = mileageList.get(i + 1) - mileageList.get(i - 1);
				tim = timeList.get(i + 1) - timeList.get(i - 1);
			}
			vList.add(mil / tim * 1000);
		}
		return len;
	}

	private void WriteHbase(int rowS, int rowE, String partKey)
			throws Exception {
		HTable table = new HTable(cfg, Bytes.toBytes("traffic"));
		String key = partKey + "|" + timeStrList.get(rowS);
		Put put = new Put(Bytes.toBytes(key));
		List<String> resstrt = timeStrList.subList(rowS, rowE + 1);
		List<Long> restim = timeList.subList(rowS, rowE + 1);
		List<Double> reslat = latitudeList.subList(rowS, rowE + 1);
		List<Double> reslon = longitudeList.subList(rowS, rowE + 1);
		List<Double> resmil = mileageList.subList(rowS, rowE + 1);
		List<Double> resv = vList.subList(rowS, rowE + 1);
		List<Double> resinsv = instantaneousVList.subList(rowS, rowE + 1);
		put.add(Bytes.toBytes("time"), Bytes.toBytes("str"),
				Bytes.toBytes(resstrt.toString()));
		put.add(Bytes.toBytes("time"), Bytes.toBytes("int"),
				Bytes.toBytes(restim.toString()));
		put.add(Bytes.toBytes("coordinate"), Bytes.toBytes("lat"),
				Bytes.toBytes(reslat.toString()));
		put.add(Bytes.toBytes("coordinate"), Bytes.toBytes("lon"),
				Bytes.toBytes(reslon.toString()));
		put.add(Bytes.toBytes("mileage"), Bytes.toBytes("mileage"),
				Bytes.toBytes(resmil.toString()));
		put.add(Bytes.toBytes("v"), Bytes.toBytes("v"),
				Bytes.toBytes(resv.toString()));
		put.add(Bytes.toBytes("v"), Bytes.toBytes("insv"),
				Bytes.toBytes(resinsv.toString()));
		table.put(put);
		table.close();
	}
}
