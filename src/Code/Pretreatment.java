package Code;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class Pretreatment {
	private static final double STEP = 0.01;

	private static final String gridsPath = "./Intermediate/grids";
	private static final String wayNamePath = "./Intermediate/wayname";
	private static final String mapInfoPath = "./Intermediate/mapinfo";
	private static final String nodesPath = "./Raw/nodes";
	private static final String waysPath = "./Raw/ways";

	private static Map ways = new HashMap();
	private static double minLat = Double.MAX_VALUE, maxLat = Double.MIN_VALUE,
			minLon = Double.MAX_VALUE, maxLon = Double.MIN_VALUE;
	private static int numLat, numLon;

	public Pretreatment() {
		try {
			Map nodes = new HashMap();
			Map nodesData = (Map) myutil.ReadJsonMap(nodesPath);
			List nodesList = (List) nodesData.get("elements");
			int len = nodesList.size();
			for (int i = 0; i < len; i++) {
				Map nodeInfo = (Map) nodesList.get(i);
				if (((String) nodeInfo.get("type")).compareTo("node") == 0) {
					double tmpdouble = (Double) nodeInfo.get("id");
					long tmplong = (long) tmpdouble;
					String tmpId = String.valueOf(tmplong);
					double tmpLat = (Double) nodeInfo.get("lat");
					double tmpLon = (Double) nodeInfo.get("lon");
					ArrayList<Double> tmpList = new ArrayList<Double>();
					tmpList.add(tmpLat);
					tmpList.add(tmpLon);
					nodes.put(tmpId, tmpList);
				}
			}
			Map<String, String> wayName = new HashMap<String, String>();
			Map waysData = (Map) myutil.ReadJsonMap(waysPath);
			List waysList = (List) waysData.get("elements");
			len = waysList.size();
			for (int i = 0; i < len; i++) {
				Map wayInfo = (Map) waysList.get(i);
				if (((String) wayInfo.get("type")).compareTo("way") == 0
						&& wayInfo.containsKey("tags")
						&& ((Map) wayInfo.get("tags")).containsKey("highway")) {
					double tmpdouble = (Double) wayInfo.get("id");
					long tmplong = (long) tmpdouble;
					String tmpId = String.valueOf(tmplong);
					Map res = new HashMap();
					res.put("highway",
							((Map) wayInfo.get("tags")).get("highway"));
					List resList = new ArrayList();
					List nodesOnWay = (List) wayInfo.get("nodes");
					int length = nodesOnWay.size();
					for (int j = 0; j < length; j++) {
						tmpdouble = (Double) nodesOnWay.get(j);
						tmplong = (long) tmpdouble;
						String tmpNodeId = String.valueOf(tmplong);
						if (nodes.containsKey(tmpNodeId)) {
							List node = (List) nodes.get(tmpNodeId);
							resList.add(node);
						}
					}
					if (resList.size() > 1) {
						res.put("nodes", resList);
						ways.put(tmpId, res);
						String tmpName = "";
						if (((Map) wayInfo.get("tags")).containsKey("name"))
							tmpName = (String) ((Map) wayInfo.get("tags"))
									.get("name");
						wayName.put(tmpId, tmpName);
					}
				}
			}
			myutil.WriteJsonMap(wayNamePath, wayName);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void SetMapInfo() {
		Iterator i = ways.values().iterator();
		while (i.hasNext()) {
			Map tmp = (Map) i.next();
			List nodes = (List) tmp.get("nodes");
			int len = nodes.size();
			for (int j = 0; j < len; j++) {
				List node = (List) nodes.get(j);
				double lat = (Double) node.get(0);
				double lon = (Double) node.get(1);
				minLat = Math.min(minLat, lat);
				maxLat = Math.max(maxLat, lat);
				minLon = Math.min(minLon, lon);
				maxLon = Math.max(maxLon, lon);
			}
		}
		numLat = (int) Math.ceil((maxLat - minLat) / STEP);
		numLon = (int) Math.ceil((maxLon - minLon) / STEP);
		List res = new ArrayList();
		res.add(minLat);
		res.add(maxLat);
		res.add(minLon);
		res.add(maxLon);
		res.add(numLat);
		res.add(numLon);
		try {
			myutil.WriteJsonList(mapInfoPath, res);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void SetGrids() {
		Map grids = new HashMap();
		int numGrids = numLat * numLon;
		for (int i = 0; i < numGrids; i++)
			grids.put(String.valueOf(i), new ArrayList());
		Iterator i = ways.keySet().iterator();
		while (i.hasNext()) {
			String key = (String) i.next();
			Map value = (Map) ways.get(key);
			String type = (String) value.get("highway");
			List nodes = (List) value.get("nodes");
			int len = nodes.size();
			List node = (List) nodes.get(0);
			double lastLat = (Double) node.get(0);
			double lastLon = (Double) node.get(1);
			int lastLoc = FindGridId(lastLat, lastLon);
			for (int j = 1; j < len; j++) {
				node = (List) nodes.get(j);
				double lat = (Double) node.get(0);
				double lon = (Double) node.get(1);
				int loc = FindGridId(lat, lon);
				Map seg = GenerateSeg(lastLoc + "_" + key + "_" + (j - 1),
						lastLat, lastLon, lat, lon, type);
				((List) grids.get(String.valueOf(lastLoc))).add(seg);
				if (lastLoc != loc)
					((List) grids.get(String.valueOf(loc))).add(seg);
				lastLat = lat;
				lastLon = lon;
				lastLoc = loc;
			}
		}
		try {
			myutil.WriteJsonMap(gridsPath, grids);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private int FindGridId(double x, double y) {
		int locX = (int) ((x - minLat) / STEP);
		if (locX == numLat)
			locX--;
		int locY = (int) ((y - minLon) / STEP);
		if (locY == numLon)
			locY--;
		return locX * numLon + locY;
	}

	private Map GenerateSeg(String id, double slat, double slon, double elat,
			double elon, String type) {
		Map res = new HashMap();
		Map tmp = new HashMap();
		List<Double> s = new ArrayList<Double>();
		s.add(slat);
		s.add(slon);
		List<Double> e = new ArrayList<Double>();
		e.add(elat);
		e.add(elon);
		tmp.put("snode", s);
		tmp.put("enode", e);
		tmp.put("highway", type);
		res.put(id, tmp);
		return res;
	}
}
