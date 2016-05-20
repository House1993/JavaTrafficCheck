package Code;
import java.io.File;

public class Main {
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
		long start = System.currentTimeMillis();
		Solve f = new Solve();
		long mid = System.currentTimeMillis();
		f.WorkOn();
		long end = System.currentTimeMillis();
		System.out.println("Load files cost " + (mid - start) + "ms");
		System.out.println("Match 120MB cost " + (end - mid) + "ms");
	}
}
