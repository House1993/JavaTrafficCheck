package Code;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

public class Check {

	public static class CheckMapper extends TableMapper<Text, Text> {

		private static final double STEP = 0.01;
		private static final double RADIUS = 6371000;
		private static final double vconstant = 41.666667;

		private static final String gridsPath = "hdfs://fang/Intermediate/grids";
		private static final String wayNamePath = "hdfs://fang/Intermediate/wayname";
		private static final String mapInfoPath = "hdfs://fang/Intermediate/mapinfo";
		private static final String speedLimitPath = "hdfs://fang/Raw/speedlimit";

		private static Map gridsMap;
		private static Map wayName;
		private static double minLat, maxLat, minLon, maxLon;
		private static int numLat, numLon;
		private static Map speedLimit;

		private static double bestDistance;
		private static String bestSegment, bestType;

		private static List<String> wayNameList = new ArrayList<String>();
		private static List<String> segmentList = new ArrayList<String>();
		private static List<String> typeList = new ArrayList<String>();
		private static List<Double> distanceList = new ArrayList<Double>();
		private static List<String> wayIdList = new ArrayList<String>();
		private static List<String> overSpeedList = new ArrayList<String>();
		private static List<String> instantaneousOverSpeedList = new ArrayList<String>();

		protected void setup(Context context) throws IOException {
			Configuration conf = context.getConfiguration();
			FileSystem fs = FileSystem.get(conf);
			Path path = new Path(gridsPath);
			if (!fs.exists(path)) {
				throw new IOException("grids file not exists!");
			}
			gridsMap = myutil.ReadJsonMap(path, fs);
			path = new Path(wayNamePath);
			if (!fs.exists(path)) {
				throw new IOException("wayname file not exists!");
			}
			wayName = myutil.ReadJsonMap(path, fs);
			path = new Path(speedLimitPath);
			if (!fs.exists(path)) {
				throw new IOException("speedlimit file not exists!");
			}
			speedLimit = myutil.ReadJsonMap(path, fs);
			path = new Path(mapInfoPath);
			if (!fs.exists(path)) {
				throw new IOException("mapinfo file not exists!");
			}
			ArrayList tmpMapInfo = myutil.ReadJsonList(path, fs);
			minLat = (Double) tmpMapInfo.get(0);
			maxLat = (Double) tmpMapInfo.get(1);
			minLon = (Double) tmpMapInfo.get(2);
			maxLon = (Double) tmpMapInfo.get(3);
			double tmp;
			tmp = (Double) tmpMapInfo.get(4);
			numLat = (int) tmp;
			tmp = (Double) tmpMapInfo.get(5);
			numLon = (int) tmp;
		}

		@Override
		protected void map(ImmutableBytesWritable key, Result value,
				Context context) throws IOException, InterruptedException {
			Text rowkey = new Text(Bytes.toString(value.getRow()));
			String latStr = Bytes.toString(value.getValue(
					Bytes.toBytes("coordinate"), Bytes.toBytes("lat")));
			String lonStr = Bytes.toString(value.getValue(
					Bytes.toBytes("coordinate"), Bytes.toBytes("lon")));
			String vStr = Bytes.toString(value.getValue(Bytes.toBytes("v"),
					Bytes.toBytes("v")));
			String insvStr = Bytes.toString(value.getValue(Bytes.toBytes("v"),
					Bytes.toBytes("insv")));
			Match(latStr.substring(1, latStr.length() - 1),
					lonStr.substring(1, lonStr.length() - 1));
			TestOverSpeed(vStr.substring(1, vStr.length() - 1),
					insvStr.substring(1, insvStr.length() - 1));
			// segmentList.add("fang");
			// typeList.add("zi");
			// wayIdList.add("cheng");
			// distanceList.add(521.0);
			// wayNameList.add("wang");
			// overSpeedList.add("lele");
			// instantaneousOverSpeedList.add("memememememda!");
			context.write(rowkey,
					new Text("match|segment|" + segmentList.toString()));
			context.write(rowkey,
					new Text("match|roadtype|" + typeList.toString()));
			context.write(rowkey,
					new Text("match|distance|" + distanceList.toString()));
			context.write(rowkey,
					new Text("match|wayid|" + wayIdList.toString()));
			context.write(rowkey,
					new Text("match|wayname|" + wayNameList.toString()));
			context.write(rowkey,
					new Text("overspeed|v|" + overSpeedList.toString()));
			context.write(rowkey, new Text("overspeed|insv|"
					+ instantaneousOverSpeedList.toString()));
		}

		private void Match(String latStr, String lonStr) {
			String[] latList = latStr.split(", ");
			String[] lonList = lonStr.split(", ");
			int len = latList.length;
			segmentList.clear();
			typeList.clear();
			distanceList.clear();
			wayIdList.clear();
			wayNameList.clear();
			for (int i = 0; i < len; i++) {
				double lat = Double.parseDouble(latList[i]);
				double lon = Double.parseDouble(lonList[i]);
				MatchPointNaive(lat, lon);
				String bestWay = "";
				String bestWayId = "";
				try {
					bestWayId = bestSegment.split("_")[1];
					bestWay = (String) wayName.get(bestWayId);
				} catch (Exception e) {
					bestWay = "";
					bestWayId = "";
				}
				segmentList.add(bestSegment);
				typeList.add(bestType);
				distanceList.add(bestDistance);
				wayIdList.add(bestWayId);
				wayNameList.add(bestWay);
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
					double sx = (Double) sNode.get(0), sy = (Double) sNode
							.get(1);
					List eNode = (List) data.get("enode");
					double ex = (Double) eNode.get(0), ey = (Double) eNode
							.get(1);
					String wayType = (String) data.get("highway");
					double tmpDistance = GetDistance(lat, lon, sx, sy, ex, ey);
					if (tmpDistance < 500 && tmpDistance < bestDistance) {
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
			double connerX = minLat + STEP * locx, connerY = minLon + STEP
					* locy;
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

		private double GetDistance(double x0, double y0, double x1, double y1,
				double x2, double y2) {
			double molecule = (x1 - x0) * (x1 - x2) + (y1 - y0) * (y1 - y2);
			double denominator = (x1 - x2) * (x1 - x2) + (y1 - y2) * (y1 - y2);
			double X = x1, Y = y1;
			if (denominator > 1e-8) {
				double temp = molecule / denominator;
				X = x1 + temp * (x2 - x1);
				Y = y1 + temp * (y2 - y1);
			}
			if ((X - x1) * (X - x2) < 1e-8 && (Y - y1) * (Y - y2) < 1e-8)
				return CalTrueDistance(x0, y0, X, Y);
			return Math.min(CalTrueDistance(x0, y0, x1, y1),
					CalTrueDistance(x0, y0, x2, y2));
		}

		private double CalTrueDistance(double x1, double y1, double x2,
				double y2) {
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

		private void TestOverSpeed(String vStr, String insvStr) {
			String[] vList = vStr.split(", ");
			String[] insvList = insvStr.split(", ");
			int len = vList.length;
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
			overSpeedList.clear();
			instantaneousOverSpeedList.clear();
			double unclassifiedSpeedLimit = (Double) speedLimit
					.get("unclassified");
			double v = Double.parseDouble(vList[0]);
			double insv = Double.parseDouble(insvList[0]);
			if (segmentList.get(0).compareTo("") == 0) {
				overSpeedList.add("Not decided");
				instantaneousOverSpeedList.add("Not decided");
			} else {
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
					instantaneousOverSpeedList
							.add(((Boolean) (insv >= (Double) speedLimit
									.get(typeList.get(0)))).toString());
				} catch (Exception e) {
					instantaneousOverSpeedList
							.add(((Boolean) (insv >= unclassifiedSpeedLimit))
									.toString());
				}
			}
			for (int i = 1; i < len - 1; i++) {
				v = Double.parseDouble(vList[i]);
				insv = Double.parseDouble(insvList[i]);
				if (wayIdList.get(i).compareTo(NWayid) == 0) {
					NE = i;
					NLen++;
				} else {
					if (PPS != -1) {
						if (PPWayid.compareTo(NWayid) == 0 && POverspeed
								&& PLen <= 2 && PLen * 4 <= PPLen + NLen) {
							double tmp = unclassifiedSpeedLimit;
							try {
								tmp = (Double) speedLimit.get(typeList.get(NE));
							} catch (Exception e) {
								tmp = unclassifiedSpeedLimit;
							}
							for (int j = PS; j <= PE; j++) {
								double temp = Double.parseDouble(vList[j]);
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
				if (segmentList.get(i).compareTo("") == 0) {
					overSpeedList.add("Not decided");
					instantaneousOverSpeedList.add("Not decided");
				} else {
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
								.add(((Boolean) (insv >= (Double) speedLimit
										.get(typeList.get(i)))).toString());
					} catch (Exception e) {
						instantaneousOverSpeedList
								.add(((Boolean) (insv >= unclassifiedSpeedLimit))
										.toString());
					}
				}
			}
			if (wayIdList.get(len - 1).compareTo(NWayid) == 0) {
				NE = len - 1;
				NLen++;
			} else {
				if (PPS != -1) {
					if (PPWayid.compareTo(NWayid) == 0 && POverspeed
							&& PLen <= 2 && PLen * 4 <= PPLen + NLen) {
						double tmp = unclassifiedSpeedLimit;
						try {
							tmp = (Double) speedLimit.get(typeList.get(NE));
						} catch (Exception e) {
							tmp = unclassifiedSpeedLimit;
						}
						for (int j = PS; j <= PE; j++) {
							double temp = Double.parseDouble(vList[j]);
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
				NS = len - 1;
				NE = len - 1;
				NLen = 1;
				NWayid = wayIdList.get(len - 1);
			}
			v = Double.parseDouble(vList[len - 1]);
			insv = Double.parseDouble(insvList[len - 1]);
			if (segmentList.get(len - 1).compareTo("") == 0) {
				overSpeedList.add("Not decided");
				instantaneousOverSpeedList.add("Not decided");
			} else {
				try {
					if (v >= (Double) speedLimit.get(typeList.get(len - 1))) {
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
							.add(((Boolean) (insv >= (Double) speedLimit
									.get(typeList.get(len - 1)))).toString());
				} catch (Exception e) {
					instantaneousOverSpeedList
							.add(((Boolean) (insv >= unclassifiedSpeedLimit))
									.toString());
				}
			}
			if (PPS != -1) {
				if (PPWayid.compareTo(NWayid) == 0 && POverspeed && PLen <= 2
						&& PLen * 4 <= PPLen + NLen) {
					double tmp = unclassifiedSpeedLimit;
					try {
						tmp = (Double) speedLimit.get(typeList.get(NE));
					} catch (Exception e) {
						tmp = unclassifiedSpeedLimit;
					}
					for (int j = PS; j <= PE; j++) {
						double temp = Double.parseDouble(vList[j]);
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

	}

	public static class CheckReducer extends TableReducer<Text, Text, Put> {

		protected void reduce(Text key, Iterable<Text> value, Context context)
				throws IOException, InterruptedException {
			Put put = new Put(Bytes.toBytes(key.toString()));
			for (Text it : value) {
				String[] vals = it.toString().split("\\|");
				String fml = vals[0];
				String qlf = vals[1];
				String ctt = vals[2];
				// System.out.println(it.toString());
				// System.out.println("family = " + fml);
				// System.out.println("qualifier = " + qlf);
				// System.out.println("value = " + ctt);
				put.add(Bytes.toBytes(fml), Bytes.toBytes(qlf),
						Bytes.toBytes(ctt));
			}
			context.write(null, put);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "Check ");
		job.setJarByClass(Check.class);
		Scan scan = new Scan();
		TableMapReduceUtil.initTableMapperJob("traffic", scan,
				CheckMapper.class, Text.class, Text.class, job);
		TableMapReduceUtil.initTableReducerJob("traffic", CheckReducer.class,
				job);
		long f1 = System.currentTimeMillis();
		int res = job.waitForCompletion(true) ? 0 : 1;
		long f2 = System.currentTimeMillis();
		System.out.println("status = " + res + " mapreduce time cost = "
				+ (f2 - f1));
	}
}