package Code;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class SplitDriver {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			long f1 = System.currentTimeMillis();
			Split f = new Split();
			f.Work();
			long f2 = System.currentTimeMillis();
			System.out.println("split routes and put them into hbase cost "
					+ (f2 - f1));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// Configuration cfg = HBaseConfiguration.create();
		// try {
		// HTable table = new HTable(cfg, Bytes.toBytes("traffic"));
		// Scan scan = new Scan();
		// ResultScanner rs = table.getScanner(scan);
		// for (Result r : rs) {
		// System.out.println(Bytes.toString(r.getRow()));
		// for (KeyValue kv : r.list()) {
		// System.out.println(Bytes.toString(kv.getFamily()));
		// System.out.println(Bytes.toString(kv.getQualifier()));
		// System.out.println(Bytes.toString(kv.getValue()));
		// }
		// }
		// table.close();
		// } catch (IOException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }
	}
}
