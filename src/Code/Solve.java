package Code;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Solve {
	private static final int THIRTY_MINUTES = 1800;
	private static final double STEP = 0.01;
	private static final double RADIUS = 6371000;
	private static final double vconstant = 41.666667;

	private static final String gridsPath = "./Intermediate/grids";
	private static final String wayNamePath = "./Intermediate/wayname";
	private static final String mapInfoPath = "./Intermediate/mapinfo";
	private static final String speedLimitPath = "./Raw/speedlimit";
	private static final String resultFolder = "./Result/";
	private static final String rawFolder = "./Raw/Routes";

	private static Map gridsMap;
	private static Map wayName;
	private static double minLat, maxLat, minLon, maxLon;
	private static int numLat, numLon;
	private static Map speedLimit;

	private static List<String> timeStrList = new ArrayList<String>();
	private static List<Long> timeList = new ArrayList<Long>();
	private static List<Double> longitudeList = new ArrayList<Double>();
	private static List<Double> latitudeList = new ArrayList<Double>();
	private static List<Double> mileageList = new ArrayList<Double>();
	private static List<String> wayNameList = new ArrayList<String>();
	private static List<String> segmentList = new ArrayList<String>();
	private static List<String> typeList = new ArrayList<String>();
	private static List<Double> distanceList = new ArrayList<Double>();
	private static List<Double> vList = new ArrayList<Double>();
	private static List<String> overSpeedList = new ArrayList<String>();
	private static List<Double> instantaneousVList = new ArrayList<Double>();
	private static List<Boolean> instantaneousOverSpeedList = new ArrayList<Boolean>();

	private static List<String> wayIdList = new ArrayList<String>();

	private static double bestDistance;
	private static String bestSegment, bestType;

	public Solve() {
		try {
			gridsMap = myutil.ReadJsonMap(gridsPath);
			wayName = myutil.ReadJsonMap(wayNamePath);
			speedLimit = myutil.ReadJsonMap(speedLimitPath);
			ArrayList tmpMapInfo = myutil.ReadJsonList(mapInfoPath);
			minLat = (Double) tmpMapInfo.get(0);
			maxLat = (Double) tmpMapInfo.get(1);
			minLon = (Double) tmpMapInfo.get(2);
			maxLon = (Double) tmpMapInfo.get(3);
			double tmp;
			tmp = (Double) tmpMapInfo.get(4);
			numLat = (int) tmp;
			tmp = (Double) tmpMapInfo.get(5);
			numLon = (int) tmp;
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void WorkOn() {
		long readCost = 0;
		long writeCost = 0;
		File folder = new File(rawFolder);
		File[] files = folder.listFiles();
		for (int k = 0; k < files.length; k++) {
			int length = 0;
			try {
				long start = System.currentTimeMillis();
				length = ExtractInfo(files[k].getPath());
				long end = System.currentTimeMillis();
				readCost += end - start;
			} catch (Exception e) {
				e.printStackTrace();
			}
			if (length < 2)
				continue; // 0 or 1 record
			String fileName = files[k].getName().split("\\.")[0];
			String outputPath = resultFolder + fileName + "/";
			File outputFolder = new File(outputPath);
			outputFolder.mkdirs();
			int fileIdx = 0;
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
							Match(rowWritten, lastdiff);
							TestOverSpeed(rowWritten, lastdiff);
							long start = System.currentTimeMillis();
							WriteResule(rowWritten, lastdiff, outputPath, fileIdx++);
							long end = System.currentTimeMillis();
							writeCost += end - start;
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
				Match(rowWritten, length - 1);
				TestOverSpeed(rowWritten, length - 1);
				long start = System.currentTimeMillis();
				WriteResule(rowWritten, length - 1, outputPath, fileIdx);
				long end = System.currentTimeMillis();
				writeCost += end - start;
			}
		}
		System.out.println("Read cost " + readCost);
		System.out.println("Write cost " + writeCost);
	}

	private int ExtractInfo(String path) {
		timeStrList.clear();
		timeList.clear();
		longitudeList.clear();
		latitudeList.clear();
		mileageList.clear();
		instantaneousVList.clear();
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
		return len;
	}

	private void Match(int rowS, int rowE) {
		wayNameList.clear();
		segmentList.clear();
		typeList.clear();
		distanceList.clear();
		wayIdList.clear();
		for (int i = 0; i + rowS <= rowE; i++) {
			MatchPointNaive(latitudeList.get(rowS + i), longitudeList.get(rowS + i));
			String bestWay = "";
			String bestWayId = "";
			try {
				bestWayId = bestSegment.split("_")[1];
				bestWay = (String) wayName.get(bestWayId);
			} catch (Exception e) {
				bestWay = "";
				bestWayId = "";
			}
			wayNameList.add(bestWay);
			segmentList.add(bestSegment);
			typeList.add(bestType);
			distanceList.add(bestDistance);
			wayIdList.add(bestWayId);
		}
	}

	private void MatchPointNaive(double lat, double lon) {
		bestDistance = Double.MAX_VALUE;
		bestSegment = "";
		bestType = "unclassified";
		if (lat < minLat || lat > maxLat || lon < minLon || lon > maxLon)
			return;
		List<String> neighbor = FindNeighbor(lat, lon);
		int num = neighbor.size();
		for (int i = 0; i < num; i++) {
			List segsList = (List) gridsMap.get(neighbor.get(i));
			int length = segsList.size();
			for (int j = 0; j < length; j++) {
				Map seg = (Map) segsList.get(j);
				String key = (String) seg.keySet().iterator().next();
				Map data = (Map) seg.get(key);
				List sNode = (List) data.get("snode");
				double sx = (Double) sNode.get(0), sy = (Double) sNode.get(1);
				List eNode = (List) data.get("enode");
				double ex = (Double) eNode.get(0), ey = (Double) eNode.get(1);
				String wayType = (String) data.get("highway");
				double tmpDistance = GetDistance(lat, lon, sx, sy, ex, ey);
				if (tmpDistance < bestDistance) {
					bestDistance = tmpDistance;
					bestSegment = key;
					bestType = wayType;
				}
			}
		}
	}

	private List<String> FindNeighbor(double lat, double lon) {
		List<String> res = new ArrayList<String>();
		int gridId = FindGridId(lat, lon);
		int locx = gridId / numLon, locy = gridId % numLon;
		double connerX = minLat + STEP * locx, connerY = minLon + STEP * locy;
		double tmpx = lat - connerX, tmpy = connerY - lon;
		res.add(String.valueOf(gridId));
		int up = gridId - numLon;
		int down = gridId + numLon;
		int left = gridId - 1;
		int right = gridId + 1;
		int upleft = up - 1;
		int upright = up + 1;
		int downleft = down - 1;
		int downright = down + 1;
		if (tmpx < STEP / 2) {
			if (locx != 0)
				res.add(String.valueOf(up));
			if (tmpy < STEP / 2) {
				if (locy != 0) {
					res.add(String.valueOf(left));
					if (locx != 0)
						res.add(String.valueOf(upleft));
				}
			} else {
				if (locy != numLon - 1) {
					res.add(String.valueOf(right));
					if (locx != 0)
						res.add(String.valueOf(upright));
				}
			}
		} else {
			if (locx != numLat - 1)
				res.add(String.valueOf(down));
			if (tmpy < STEP / 2) {
				if (locy != 0) {
					res.add(String.valueOf(left));
					if (locx != numLat - 1)
						res.add(String.valueOf(downleft));
				}
			} else {
				if (locy != numLon - 1) {
					res.add(String.valueOf(right));
					if (locx != numLat - 1)
						res.add(String.valueOf(downright));
				}
			}
		}
		return res;
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

	private double GetDistance(double x0, double y0, double x1, double y1, double x2, double y2) {
		double molecule = (x1 - x0) * (x1 - x2) + (y1 - y0) * (y1 - y2);
		double denominator = (x1 - x2) * (x1 - x2) + (y1 - y2) * (y1 - y2);
		double X = x1, Y = y1;
		if (denominator > 1e-8) {
			double temp = molecule / denominator;
			X = x1 + temp * (x2 - x1);
			Y = y1 + temp * (y2 - y1);
		}
		if ((X - x1) * (X - x2) < 1e-8 && (Y - y1) * (Y - y2) < 1e-8)
			return CalDistance(x0, y0, X, Y);
		return Math.min(CalDistance(x0, y0, x1, y1), CalDistance(x0, y0, x2, y2));
	}

	private double CalDistance(double x1, double y1, double x2, double y2) {
		return CalTrueDistance(x1, y1, x2, y2);
		// return CalEulerDistance(x1, y1, x2, y2);
	}

	private double CalTrueDistance(double x1, double y1, double x2, double y2) {
		x1 = Math.toRadians(x1);
		y1 = Math.toRadians(y1);
		x2 = Math.toRadians(x2);
		y2 = Math.toRadians(y2);
		double tmpx = x1 - x2;
		double tmpy = y1 - y2;
		double first = Math.sin(tmpx / 2);
		first *= first;
		double second = Math.sin(tmpy / 2);
		second = second * second * Math.cos(x1) * Math.cos(x2);
		double angle = 2.0 * Math.asin(Math.sqrt(first + second));
		return RADIUS * angle;
	}

	private double CalEulerDistance(double x1, double y1, double x2, double y2) {
		double tmpx = x1 - x2;
		double tmpy = y1 - y2;
		return Math.sqrt(tmpx * tmpx + tmpy * tmpy);
	}

	private void TestOverSpeed(int rowS, int rowE) {
		int PPS = -1;
		int PPLen = 0;
		String PPWayid = "";
		int PS = -1;
		int PE = -1;
		int PLen = 0;
		String PWayid = "";
		boolean POverspeed = false;
		int NS = 0;
		int NE = 0;
		int NLen = 1;
		String NWayid = wayIdList.get(0);
		boolean NOverspeed = false;
		vList.clear();
		overSpeedList.clear();
		instantaneousOverSpeedList.clear();
		double unclassifiedSpeedLimit = (Double) speedLimit.get("unclassified");
		double dertDis = mileageList.get(rowS + 1) - mileageList.get(rowS);
		double dertTime = timeList.get(rowS + 1) - timeList.get(rowS);
		double v = 0;
		if (dertTime > 0)
			v = dertDis / dertTime * 1000;
		vList.add(v);
		try {
			NOverspeed = (v >= (Double) speedLimit.get(typeList.get(0)));
			if (NOverspeed) {
				if (v > vconstant)
					overSpeedList.add("Not decided");
				else
					overSpeedList.add("true");
			} else
				overSpeedList.add("false");
		} catch (Exception e) {
			NOverspeed = (v >= unclassifiedSpeedLimit);
			if (NOverspeed) {
				if (v > vconstant)
					overSpeedList.add("Not decided");
				else
					overSpeedList.add("true");
			} else
				overSpeedList.add("false");
		}
		try {
			instantaneousOverSpeedList.add(instantaneousVList.get(rowS) >= (Double) speedLimit.get(typeList.get(0)));
		} catch (Exception e) {
			instantaneousOverSpeedList.add(instantaneousVList.get(rowS) >= unclassifiedSpeedLimit);
		}
		for (int i = 1; i + rowS < rowE; i++) {
			if (wayIdList.get(i).compareTo(NWayid) == 0) {
				NE = i;
				NLen++;
			} else {
				if (PPS != -1) {
					if (PPWayid.compareTo(NWayid) == 0 && POverspeed && PLen <= 2 && PLen * 4 <= PPLen + NLen) {
						double tmp = unclassifiedSpeedLimit;
						try {
							tmp = (Double) speedLimit.get(typeList.get(NE));
						} catch (Exception e) {
							tmp = unclassifiedSpeedLimit;
						}
						for (int j = PS; j <= PE; j++) {
							double temp = vList.get(j);
							if (temp >= tmp) {
								if (temp > vconstant)
									overSpeedList.set(j, "Not decided");
								else
									overSpeedList.set(j, "true");
							} else
								overSpeedList.set(j, "false");
						}
					}
				}
				PPS = PS;
				PPLen = PLen;
				PPWayid = PWayid;
				PS = NS;
				PE = NE;
				PLen = NLen;
				PWayid = NWayid;
				POverspeed = NOverspeed;
				NS = i;
				NE = i;
				NLen = 1;
				NWayid = wayIdList.get(i);
			}
			dertDis = mileageList.get(rowS + i + 1) - mileageList.get(rowS + i - 1);
			dertTime = timeList.get(rowS + i + 1) - timeList.get(rowS + i - 1);
			v = 0;
			if (dertTime > 0)
				v = dertDis / dertTime * 1000;
			vList.add(v);
			try {
				if (v >= (Double) speedLimit.get(typeList.get(i))) {
					if (v > vconstant)
						overSpeedList.add("Not decided");
					else {
						overSpeedList.add("true");
						NOverspeed = true;
					}
				} else
					overSpeedList.add("false");
			} catch (Exception e) {
				if (v >= unclassifiedSpeedLimit) {
					if (v > vconstant)
						overSpeedList.add("Not decided");
					else {
						overSpeedList.add("true");
						NOverspeed = true;
					}
				} else
					overSpeedList.add("false");
			}
			try {
				instantaneousOverSpeedList
						.add(instantaneousVList.get(rowS + i) >= (Double) speedLimit.get(typeList.get(i)));
			} catch (Exception e) {
				instantaneousOverSpeedList.add(instantaneousVList.get(rowS + i) >= unclassifiedSpeedLimit);
			}
		}
		if (wayIdList.get(rowE - rowS).compareTo(NWayid) == 0) {
			NE = rowE;
			NLen++;
		} else {
			if (PPS != -1) {
				if (PPWayid.compareTo(NWayid) == 0 && POverspeed && PLen <= 2 && PLen * 4 <= PPLen + NLen) {
					double tmp = unclassifiedSpeedLimit;
					try {
						tmp = (Double) speedLimit.get(typeList.get(NE));
					} catch (Exception e) {
						tmp = unclassifiedSpeedLimit;
					}
					for (int j = PS; j <= PE; j++) {
						double temp = vList.get(j);
						if (temp >= tmp) {
							if (temp > vconstant)
								overSpeedList.set(j, "Not decided");
							else
								overSpeedList.set(j, "true");
						} else
							overSpeedList.set(j, "false");
					}
				}
			}
			PPS = PS;
			PPLen = PLen;
			PPWayid = PWayid;
			PS = NS;
			PE = NE;
			PLen = NLen;
			PWayid = NWayid;
			POverspeed = NOverspeed;
			NS = rowE;
			NE = rowE;
			NLen = 1;
			NWayid = wayIdList.get(rowE - rowS);
		}
		dertDis = mileageList.get(rowE) - mileageList.get(rowE - 1);
		dertTime = timeList.get(rowE) - timeList.get(rowE - 1);
		v = 0;
		if (dertTime > 0)
			v = dertDis / dertTime * 1000;
		vList.add(v);
		try {
			if (v >= (Double) speedLimit.get(typeList.get(rowE - rowS))) {
				if (v > vconstant)
					overSpeedList.add("Not decided");
				else {
					overSpeedList.add("true");
					NOverspeed = true;
				}
			} else
				overSpeedList.add("false");
		} catch (Exception e) {
			if (v >= unclassifiedSpeedLimit) {
				if (v > vconstant)
					overSpeedList.add("Not decided");
				else {
					overSpeedList.add("true");
					NOverspeed = true;
				}
			} else
				overSpeedList.add("false");
		}
		try {
			instantaneousOverSpeedList
					.add(instantaneousVList.get(rowE) >= (Double) speedLimit.get(typeList.get(rowE - rowS)));
		} catch (Exception e) {
			instantaneousOverSpeedList.add(instantaneousVList.get(rowE) >= unclassifiedSpeedLimit);
		}
		if (PPS != -1) {
			if (PPWayid.compareTo(NWayid) == 0 && POverspeed && PLen <= 2 && PLen * 4 <= PPLen + NLen) {
				double tmp = unclassifiedSpeedLimit;
				try {
					tmp = (Double) speedLimit.get(typeList.get(NE));
				} catch (Exception e) {
					tmp = unclassifiedSpeedLimit;
				}
				for (int j = PS; j <= PE; j++) {
					double temp = vList.get(j);
					if (temp >= tmp) {
						if (temp > vconstant)
							overSpeedList.set(j, "Not decided");
						else
							overSpeedList.set(j, "true");
					} else
						overSpeedList.set(j, "false");
				}
			}
		}
	}

	private void WriteResule(int rowS, int rowE, String outputPath, int fileName) {
		List<String[]> res = new ArrayList<String[]>();
		int num = rowE - rowS + 1;
		for (int i = 0; i < num; i++) {
			String timeStr = timeStrList.get(rowS + i);
			String time = timeList.get(rowS + i).toString();
			String latitude = latitudeList.get(rowS + i).toString();
			String longitude = longitudeList.get(rowS + i).toString();
			String mileage = mileageList.get(rowS + i).toString();
			String wayName = wayNameList.get(i);
			String segment = segmentList.get(i);
			String type = typeList.get(i);
			String distance = distanceList.get(i).toString();
			String v = vList.get(i).toString();
			String overSpeed = overSpeedList.get(i);
			String insv = instantaneousVList.get(rowS + i).toString();
			String insOverSpeed = instantaneousOverSpeedList.get(i).toString();
			String[] tmp = { timeStr, time, latitude, longitude, mileage, wayName, segment, type, distance, v,
					overSpeed, insv, insOverSpeed };
			res.add(tmp);
		}
		try {
			myutil.WriteCSV(outputPath + fileName + ".csv", res);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
