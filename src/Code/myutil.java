package Code;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

public final class myutil {
	private static Gson gson = new Gson();
	private static SimpleDateFormat sdf = new SimpleDateFormat(
			"yyyy-MM-dd HH:mm:ss");

	public static ArrayList ReadJsonList(String path) throws Exception {
		ArrayList list = new ArrayList();
		String data = LoadFile(path).trim();
		list = gson.fromJson(data, new TypeToken<Object>() {
		}.getType());
		return list;
	}

	public static Map ReadJsonMap(String path) throws Exception {
		Map<String, Object> map = new HashMap();
		String data = LoadFile(path).trim();
		map = gson.fromJson(data, new TypeToken<Map<String, Object>>() {
		}.getType());
		return map;
	}

	public static void WriteJsonList(String path, List content)
			throws Exception {
		String data = gson.toJson(content, new TypeToken<Object>() {
		}.getType());
		WriteFile(path, data);
	}

	public static void WriteJsonMap(String path, Map content) throws Exception {
		String data = gson.toJson(content,
				new TypeToken<Map<String, Object>>() {
				}.getType());
		WriteFile(path, data);
	}

	public static List ReadCSV(String csvFilePath) throws Exception {
		CsvReader reader = new CsvReader(csvFilePath, ',',
				Charset.forName("utf-8"));
		List<String[]> list = new ArrayList<String[]>();
		while (reader.readRecord()) {
			list.add(reader.getValues());
		}
		return list;
	}

	public static void WriteCSV(String csvFilePath, List contents)
			throws Exception {
		CsvWriter wr = new CsvWriter(csvFilePath, ',', Charset.forName("utf-8"));
		int len = contents.size();
		for (int i = 0; i < len; i++) {
			String[] row = (String[]) contents.get(i);
			wr.writeRecord(row);
		}
		wr.close();
	}

	public static long DateStrToLong(String strDate) {
		long res = 0;
		try {
			Date date = sdf.parse(strDate);
			res = date.getTime();
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return res / 1000;
	}

	private static String LoadFile(String filePath) throws Exception {
		StringBuilder txtContent = new StringBuilder();
		byte[] b = new byte[8 * 1024];
		InputStream in = null;
		try {
			in = new FileInputStream(filePath);
			while (true) {
				int length = in.read(b);
				if (length == -1)
					break;
				txtContent.append(new String(b, 0, length));
			}
			in.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (in != null) {
				try {
					in.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return txtContent.toString();
	}

	private static void WriteFile(String filePath, String data)
			throws Exception {
		OutputStream out = null;
		try {
			out = new FileOutputStream(filePath);
			out.write(data.getBytes());
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (out != null) {
				try {
					out.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public static ArrayList ReadJsonList(Path path, FileSystem fs)
			throws IOException {
		ArrayList list = new ArrayList();
		String data = LoadFile(path, fs).trim();
		list = gson.fromJson(data, new TypeToken<Object>() {
		}.getType());
		return list;
	}

	public static Map ReadJsonMap(Path path, FileSystem fs) throws IOException {
		Map<String, Object> map = new HashMap();
		String data = LoadFile(path, fs).trim();
		map = gson.fromJson(data, new TypeToken<Map<String, Object>>() {
		}.getType());
		return map;
	}

	private static String LoadFile(Path filePath, FileSystem fs)
			throws IOException {
		StringBuilder txtContent = new StringBuilder();
		byte[] b = new byte[8 * 1024];
		FSDataInputStream in = fs.open(filePath);
		while (true) {
			int length = in.read(b);
			if (length == -1)
				break;
			txtContent.append(new String(b, 0, length));
		}
		in.close();
		return txtContent.toString();
	}
}
