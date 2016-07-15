package com.jje.bigdata.travel.tourtag;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;

import com.jje.bigdata.travel.thesaurus.TourismHbaseOp;
import com.jje.bigdata.util.SceneConfig;
import com.jje.common.utils.StringUtils;

public class TagToTourismNEW {

	// HBase操作的对象

	private static byte[] colum_family = "info".getBytes();;
	private static byte[] name = "NAME".getBytes();
	private static byte[] begtim = "BEGIN_DATE".getBytes();
	private static byte[] endtim = "END_DATE".getBytes();

	/* 2.景点标签表 */
	// private static Map<String, String> scieneMap = new HashMap<String, String>() ;
	// private static Set<String> scieneSet = new HashSet<String>() ;
	private static Map<String, String> scieneMap = new HashMap<String, String>();

	public void getScinenMap(String scieneTable) {
		String rowkey = "";
		String[] scieneArr;
		String key = "";
		ResultScanner rs = TourismHbaseOp.getLineScan(scieneTable);
		for (Result result : rs) {
			rowkey = Bytes.toString(result.getRow());

			scieneArr = rowkey.split("_");
			for (int i = 0; i < scieneArr.length; i++) {
				key = scieneArr[i];
				if (StringUtils.isBlank(key)) {
					continue;
				}
				String sciene = "";
				for (int j = 0; j <= i; j++) {
					sciene += scieneArr[j] + "_";
				}
				if (sciene.length() > 0 && sciene.endsWith("_")) {
					sciene = sciene.substring(0, sciene.length() - 1);
				}
				if (scieneMap.containsKey(key)) {
					String temp = scieneMap.get(key);
					if (sciene.length() < temp.length()) {
						continue;
					}
				}

				scieneMap.put(key, sciene);
			}

		}

	}

	public static int getTourismTagNum(String str1, String str2) {
		int num = 0;
		while (str1.indexOf(str2) != -1) {
			num++;
			str1 = str1.replaceFirst(str2, "");
		}
		return num;
	}

	/* 遍历线路信息表,获取线路的基础信息 */
	public void putLabelToLine(String lineinfotable, String linetagtable, boolean increORfull) throws IOException {
		// 线路信息
		String line_id = "";
		String lineinfo = "";
		String linename = "";
		String begintime = "";
		String endtime = "";
		String tags = "";
		String scieneinfo = "";
		String sciencelist = "";
		String[] scenarr;
		byte[] midbyte;
		Result res = null;
		ResultScanner rs = TourismHbaseOp.getLineScan(lineinfotable);

		Map<TagLevel, Double> lineScenceMap = new HashMap<TagLevel, Double>();
		Map<String, Double> lineThemeMap = new HashMap<String, Double>();

		// 获取地点的权重
		SceneConfig scon = SceneConfig.create();
		Properties p = scon.getPara("TourismWeight");
		double con_weight = Double.parseDouble(p.getProperty("country_weight"));
		double pro_weight = Double.parseDouble(p.getProperty("province_weight"));
		double cit_weight = Double.parseDouble(p.getProperty("city_weight"));
		double tou_weight = Double.parseDouble(p.getProperty("tourism_weight"));

		List<Double> weightList = new ArrayList<Double>();
		weightList.add(con_weight);
		weightList.add(pro_weight);
		weightList.add(cit_weight);
		weightList.add(tou_weight);

		List<String> familyList = new ArrayList<String>();
		familyList.add("info3");
		familyList.add("info4");
		familyList.add("info5");
		familyList.add("info6");

		// 添加一个线路id的list
		List<String> lineidlist = new ArrayList<String>();
		for (Result result : rs) {
			line_id = new String(result.getRow());
			lineidlist.add(line_id);
		}

		// 对线路标签对应表进行遍历,把已经存在的线路从list中去除
		// if(increORfull){
		// rs = thOp.getLineScan(linetagtable) ;
		// for(Result result : rs){
		// line_id = new String(result.getRow()) ;
		// lineidlist.remove(line_id) ;
		// }
		// }
		System.out.println(lineidlist);
		for (String s : lineidlist) {
			 System.out.println("lineId:"+s);
			lineScenceMap.clear();
			lineThemeMap.clear();
			line_id = s;
			// System.out.println(line_id) ;
			lineinfo = TourismHbaseOp.getLineMapNew(line_id);
			// 根据line_id获取线路的其他信息
			res = TourismHbaseOp.getLineInfo(line_id);
			linename = "";
			midbyte = res.getValue(colum_family, name);
			if (!(midbyte == null)) {
				linename = new String(midbyte);
			}
			begintime = "";
			midbyte = res.getValue(colum_family, begtim);
			if (!(midbyte == null)) {
				begintime = new String(midbyte);
			}
			endtime = "";
			midbyte = res.getValue(colum_family, endtim);
			if (!(midbyte == null)) {
				endtime = new String(midbyte);
			}

			/*
			 * 对两个Map进行遍历,获取存在标签的信息 1.景点的Map
			 */
			for (Map.Entry<String, String> entry : scieneMap.entrySet()) {
				scieneinfo = entry.getKey();
				sciencelist = entry.getValue();
				int num = getSubStringCount(lineinfo, scieneinfo);
				if (num > 0) {
					scenarr = sciencelist.split("_");
					for (int i = 0; i < scenarr.length; i++) {
						String scen = scenarr[i];
						if (!StringUtils.isBlank(scen)) {
							String[] split = scen.split("/");
							for (String str : split) {
								String family = familyList.get(i);
								TagLevel tl = new TagLevel(family, str);
								Double val = weightList.get(i) * num;
								if (lineScenceMap.containsKey(tl)) {
									val += lineScenceMap.get(tl);
								}
								lineScenceMap.put(tl, val);
							}
						}
					}
				}
			}
			System.out.println("lineScenceMap:"+lineScenceMap);
			
			/* 对lineScenceMap进行遍历,插入到HBase表中 */
			for (Map.Entry<TagLevel, Double> entry : lineScenceMap.entrySet()) {
				if ("".equals(entry.getKey())) {
					continue;
				}
				TagLevel tl = entry.getKey();
				TourismHbaseOp.putScenicToTourismNew(line_id, tl.getFamily(), tl.getPlace(), entry.getValue() + "");
			}
		}
		 TourismHbaseOp.putAllTagsToTourNew();
	}

	public static void main(String[] args) throws Exception {
		TagToTourismNEW ttt = new TagToTourismNEW();

		ttt.getScinenMap("TB_REC_CH_ALL_TOURISM_TAG");

		// 设定变量是增量跑,还是全量跑

		TourismHbaseOp.truncateTable("TB_REC_CH_TOURISM_NEW_TAG", new String[] { "info", "info1", "info2", "info3", "info4", "info5", "info6" });
		ttt.putLabelToLine("TB_REC_LINE_MID_MAP_NEW", "TB_REC_CH_TOURISM_NEW_TAG", false);

		System.out.println("SUCCESS!");
	}

	private static int getSubStringCount(final String original, final String sub) {
		String str = original;
		int count = 0;

		String[] split = str.split(",");
		for (String s : split) {
			if (s.equals(sub)) {
				count++;
			}
		}

		return count;
	}
}
