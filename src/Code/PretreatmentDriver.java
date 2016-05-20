package Code;

import java.io.File;

public class PretreatmentDriver {
	private static final String intermediatePath = "./Intermediate/";

	public static void main(String[] args) {
		File file = new File(intermediatePath);
		if (!file.exists()) {
			long start = System.currentTimeMillis();
			file.mkdirs();
			Pretreatment p = new Pretreatment();
			p.SetMapInfo();
			p.SetGrids();
			long end = System.currentTimeMillis();
			System.out.println("Prepare cost " + (end - start) + "ms");
		}
	}
}
