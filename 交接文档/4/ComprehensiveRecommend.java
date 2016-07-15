/**
 * 酒店综合推荐
 */
package com.jje.bigdata.hotel.recommend;

import com.jje.bigdata.travel.bigdata.domain.Hotel;
import com.jje.bigdata.travel.bigdata.service.HotelService;
import com.jje.bigdata.util.SceneConfig;
import com.jje.bigdata.util.SparkUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class ComprehensiveRecommend {
	private final static double DEF_PI = 3.14159265359; // PI
	private final static double DEF_PI180 = 0.01745329252; // PI/180.0
	private final static double DEF_R = 6370693.5; // radius of earth
	
//	static String startDate; // 开始时间
//	static String endDate;// 结束时间
//	static int topLength; // 取热度排行的前几个
	
	static Properties properties = null;
	static JavaSparkContext sc = null;
	
	static Configuration conf = null;
	static Configuration conf1 = null;
	static HTable tb_time_series_freq_info = null;
	static HTable tb_user_time_geographic_info = null;
	static HTable tb_user_time_series_prefer_info = null;
	static HTable tb_user_geographic_info = null;
	static HTable tb_user_preference_info = null;
	
	static HTable tb_holiday_map = null;
	
	static HTable tb_comprehensive_rec_data = null;
	
	static {
		try {
			conf = HBaseConfiguration.create();
			conf1 = HBaseConfiguration.create();
			tb_time_series_freq_info = new HTable(conf, "TB_REC_CH_USER_TIME_SERIES_FREQ_INFO");
			tb_user_time_geographic_info = new HTable(conf, "TB_REC_CH_USER_TIME_GEOGRAPHIC");
			tb_user_time_series_prefer_info = new HTable(conf, "TB_REC_CH_USER_TIME_SERIES_PREFER_INFO");
			tb_user_geographic_info = new HTable(conf, "TB_REC_CH_USER_GEOGRAPHIC");
			tb_user_preference_info = new HTable(conf, "TB_REC_CH_USER_PREFERENCE");
			
			tb_holiday_map = new HTable(conf, "TB_CLENDAR_HOLIDAY_MAP");
			
			tb_comprehensive_rec_data = new HTable(conf, "TB_REC_CH_COMPREHENSIVE_REC_DATA");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	/**
	 * 将scan编码，该方法copy自 org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil
	 * 
	 * @param scan input Scan
	 * @return return base64 scan string
	 * @throws java.io.IOException
	 */
	static String convertScanToString(Scan scan) throws IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(out);
		scan.write(dos);
		return Base64.encodeBytes(out.toByteArray());
	}

	static void run(String[] timeSeries, double radius, int topLength, int days) throws Exception {
		final Broadcast<String[]> bc_timeSeries = sc.broadcast(timeSeries);
		final Broadcast<Double> bc_radius = sc.broadcast(radius);
		final Broadcast<Integer> bc_topLength = sc.broadcast(topLength);

		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		final Broadcast<String> bc_date = sc.broadcast(sdf.format(new Date()));

		Scan scan = new Scan();
		scan.setTimeRange(System.currentTimeMillis()-60*60*24*1000*days, System.currentTimeMillis());
		String tableName = "TB_REC_CH_USER_TIME_SERIES_FREQ_INFO";
        conf.set(TableInputFormat.INPUT_TABLE, tableName);
        conf.set(TableInputFormat.SCAN, convertScanToString(scan));
        JavaPairRDD<ImmutableBytesWritable,Result> userTimeSeriesHbaseRdd = sc.newAPIHadoopRDD(conf,TableInputFormat.class, ImmutableBytesWritable.class,Result.class);

        JavaPairRDD<String, HashMap<String, Double>> userTimeSeriesRdd = userTimeSeriesHbaseRdd.map(new PairFunction<Tuple2<ImmutableBytesWritable, Result>, String, HashMap<String, Double>>(){
			@Override
			public Tuple2<String, HashMap<String, Double>> call(Tuple2<ImmutableBytesWritable, Result> in) throws Exception {
				HashMap<String, Double> result = new HashMap<String, Double>();

				String member_id = Bytes.toString(in._1().get());
				for(String timeSeries: bc_timeSeries.value()) {
					String[] timeSeriesSplited = timeSeries.split("#");//
					NavigableMap<byte[], byte[]> map = in._2().getFamilyMap(timeSeriesSplited[1].getBytes());
					if(map != null && !map.isEmpty()) {
						byte[] timeWeightBytes = map.get(Bytes.toBytes(timeSeriesSplited[2]));
						if(timeWeightBytes != null) {
							double timeWeight = Double.parseDouble(Bytes.toString(timeWeightBytes));
							result.put(timeSeries, timeWeight);
						}
					}
				}
				return new Tuple2<String, HashMap<String, Double>>(member_id,result);
			}
		}).partitionBy(new HashPartitioner(300)).cache();

		SparkUtils.logRDDcount("userTimeSeriesHbaseRdd", userTimeSeriesHbaseRdd);

        JavaPairRDD<String, HashMap<String, Double>> userTimeSeriesFilteredRdd = userTimeSeriesRdd.filter(new Function<Tuple2<String,HashMap<String,Double>>, Boolean>() {
			@Override
			public Boolean call(Tuple2<String, HashMap<String, Double>> in)
					throws Exception {
				return !(in._2() == null || in._2().isEmpty());
			}
		});
		
		

        /*System.out.println("------------------------------------------: " + userTimeSeriesFilteredRdd.count());
        Tuple2<String, HashMap<String, Double>> t = userTimeSeriesFilteredRdd.first();
        for(String key:t._2.keySet()) {
        	System.out.println(t._1 + "---  " + key + ": " +  t._2.get(key));
        }*/

		Map<String, Hotel> hotelMap = HotelService.getHotelMap(sc, conf);

//		System.out.println("===========hotelMap:" + hotelMap);

		JavaRDD<com.jje.bigdata.travel.recommend.RecResult> user_recommendList_Rdd = userTimeSeriesFilteredRdd.map(new com.jje.bigdata.travel.recommend.MyFunction<Tuple2<String,HashMap<String, Double>>, com.jje.bigdata.travel.recommend.RecResult>(hotelMap) {
			@Override
			public com.jje.bigdata.travel.recommend.RecResult call(Tuple2<String, HashMap<String, Double>> in) throws Exception {

//				long starTime = System.currentTimeMillis();
				String member_id = in._1();
				Hashtable<String, com.jje.bigdata.travel.recommend.LineCount> hotel_count_1 = new Hashtable<String, com.jje.bigdata.travel.recommend.LineCount>(); //临时存储酒店及其权重
				Hashtable<String, com.jje.bigdata.travel.recommend.LineCount> hotel_count_2 = new Hashtable<String, com.jje.bigdata.travel.recommend.LineCount>(); //临时存储酒店及其权重

//				long timeSeriesTime = System.currentTimeMillis();
				for(String timeSeries: in._2().keySet()) {
					double timeWeight;
					if(in._2().get(timeSeries) != null) {
						timeWeight = in._2().get(timeSeries);
					}else {
						break;//如果用户时间序列频次信息表中对于该用户查不到此时间序列的信息则break
					}

					List<double[]> geographics = getGeographic(member_id, timeSeries, bc_radius.value());
					Map<String, Double> brand_value = getBrand_Value(member_id, timeSeries);
					Map<String, Double> star_value = getStar_Value(member_id, timeSeries);

					if(geographics == null || brand_value == null || star_value == null) {//地理坐标,品牌和星级这三者有任何一个查不到，则break
						break;
					}else {
						for(double[] geographic:geographics) {
							//利用这三个信息去酒店信息表(构建使用内存数据库)查询
							List<String> hotels = findHotels(geographic[0], geographic[1], geographic[2], geographic[3], brand_value.keySet(), star_value.keySet());
							for(String hotel:hotels) {//对于查到的酒店列表
								String[] hotelInfo = findHotelInfo(hotel);//获得该酒店的品牌与星级
								//计算该酒店的权重=时间维度的权重*地理维度的权重*品牌维度的权重*星级维度的权重
								double count = timeWeight * geographic[4] * brand_value.get(hotelInfo[0]) * star_value.get(hotelInfo[1]);
								if(!hotel_count_1.containsKey(hotel)) {
									hotel_count_1.put(hotel, new com.jje.bigdata.travel.recommend.LineCount(hotel, count));
								}else {
									hotel_count_1.get(hotel).count += count;
								}
							}
						}
					}
				}
//				long timeSeriesDuring = System.currentTimeMillis() - timeSeriesTime;
//				if(true){
//					throw new RuntimeException("==========================="+timeSeriesDuring);
//				}
				ArrayList<com.jje.bigdata.travel.recommend.LineCount> hotel_weight_1 = new ArrayList<com.jje.bigdata.travel.recommend.LineCount>(hotel_count_1.values());
				Collections.sort(hotel_weight_1);

				//如果在上述过程中得到TopN数量已经够了
				if(bc_topLength.value() <= hotel_weight_1.size()) {
					ArrayList<com.jje.bigdata.travel.recommend.LineCount> hotel_weight = new ArrayList<com.jje.bigdata.travel.recommend.LineCount>();
					for (int i = 0; i < bc_topLength.value(); i++) {
						hotel_weight.add(hotel_weight_1.get(i));
					}
					return new com.jje.bigdata.travel.recommend.RecResult(member_id, hotel_weight);////return
				}else {//如果在上述过程中得不到TopN或者TopN的数量不够

					List<double[]> geographics = getGeographic(member_id, "", bc_radius.value());
					Map<String, Double> brand_value = getBrand_Value(member_id, "");
					Map<String, Double> star_value = getStar_Value(member_id, "");

					if(geographics == null || brand_value == null || star_value == null) {//地理坐标,品牌和星级这三者有任何一个查不到
						return new com.jje.bigdata.travel.recommend.RecResult(member_id, hotel_weight_1);////return
					}else {
						for(double[] geographic:geographics) {
							//利用这三个信息去酒店信息表(构建使用内存数据库)查询
							List<String> hotels = findHotels(geographic[0], geographic[1], geographic[2], geographic[3], brand_value.keySet(), star_value.keySet());
							for(String hotel:hotels) {//对于查到的酒店列表
								String[] hotelInfo = findHotelInfo(hotel);//获得该酒店的品牌与星级
								//计算该酒店的权重=地理维度的权重*品牌维度的权重*星级维度的权重
								double count = geographic[4] * brand_value.get(hotelInfo[0]) * star_value.get(hotelInfo[1]);
								if(!hotel_count_2.containsKey(hotel)) {
									hotel_count_2.put(hotel, new com.jje.bigdata.travel.recommend.LineCount(hotel, count));
								}else {
									hotel_count_2.get(hotel).count += count;
								}
							}
						}
					}

					if(!hotel_count_2.isEmpty()) {
						ArrayList<com.jje.bigdata.travel.recommend.LineCount> hotel_weight_2 = new ArrayList<com.jje.bigdata.travel.recommend.LineCount>(hotel_count_2.values());
						Collections.sort(hotel_weight_2);

						int restLength = bc_topLength.value() - hotel_weight_1.size();
						int leng = restLength<hotel_weight_2.size()?restLength:hotel_weight_2.size();

						double minWeightOfhotel_weight_1;
						if(!hotel_weight_1.isEmpty()) {//hotel_weight_1中结果为空
							minWeightOfhotel_weight_1 = hotel_weight_1.get(hotel_weight_1.size()-1).count;//hotel_weight_1中的最小的权重
							for (int i = 0; i < leng; i++) {
//								hotel_weight_2.get(i).count = minWeightOfhotel_weight_1 * (1-0.01*(i+1));
								double newCount = minWeightOfhotel_weight_1 * (1-0.01*(i+1));
//								hotel_weight_1.add(hotel_weight_2.get(i));
								if(hotel_count_1.containsKey(hotel_weight_2.get(i).line_id)) {//如果hotel_weight_2和hotel_count_1有相同酒店，则去重再累加
									hotel_count_1.get(hotel_weight_2.get(i).line_id).count += newCount;
								} else { //否则直接放入hotel_count_1中去
									hotel_count_1.put(hotel_weight_2.get(i).line_id, new com.jje.bigdata.travel.recommend.LineCount(hotel_weight_2.get(i).line_id, newCount));
								}
								hotel_weight_1 = new ArrayList<com.jje.bigdata.travel.recommend.LineCount>(hotel_count_1.values());
							}
						}else if(hotel_weight_2.size() < bc_topLength.value()){
							hotel_weight_1 = hotel_weight_2;
						}else {
							for (int i = 0; i < leng; i++) {
								hotel_weight_1.add(hotel_weight_2.get(i));
							}
						}
					}
				}
//				throw new RuntimeException("=================" + (System.currentTimeMillis() - starTime));
				return new com.jje.bigdata.travel.recommend.RecResult(member_id, hotel_weight_1);
			}
		});

//		SparkUtils.logRDDcount("user_recommendList_Rdd", user_recommendList_Rdd);

//      System.out.println("====================================----------" + user_recommendList_Rdd.count());
//      System.in.read();
        JavaRDD<com.jje.bigdata.travel.recommend.RecResult> user_recommendList_filtered_Rdd = user_recommendList_Rdd.filter(new Function<com.jje.bigdata.travel.recommend.RecResult, Boolean>() {
			@Override
			public Boolean call(com.jje.bigdata.travel.recommend.RecResult recResult) throws Exception {
				return !(recResult.line_count == null || recResult.line_count.isEmpty());
			}
		});



//		SparkUtils.logRDDcount("user_recommendList_filtered_Rdd", user_recommendList_filtered_Rdd);
//        List<RecResult> l = user_recommendList_filtered_Rdd.collect();
//        System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++" + l.size());
//        System.in.read();
        /*for (int i = 0; i < 10; i++) {
			System.out.println("=============" + l.get(i).user_id + " \t" + l.get(i).getLine_count().get(0).line_id);
		}

        System.in.read();*/

//      HbaseHelper.createTable("TB_REC_CH_COMPREHENSIVE_REC_DATA", new String[]{"f"});
        conf1.set(TableOutputFormat.OUTPUT_TABLE, "TB_REC_CH_COMPREHENSIVE_REC_DATA");
        JobConf jobConf = new JobConf(conf1,ComprehensiveRecommend.class);
        jobConf.setOutputFormat(org.apache.hadoop.hbase.mapred.TableOutputFormat.class);
        Map<String, String> oldToNewHotelIdMap = HotelService.getOldToNewHotelIdMap(sc, conf);
        final Broadcast<Map<String, String>> oldToNewHotelIdMap_bd = sc.broadcast(oldToNewHotelIdMap);
        
        user_recommendList_filtered_Rdd.map(new PairFunction<com.jje.bigdata.travel.recommend.RecResult,ImmutableBytesWritable, Put>() {
			@Override
			public Tuple2<ImmutableBytesWritable, Put> call(com.jje.bigdata.travel.recommend.RecResult recResult)
					throws Exception {
				Map<String, String> oldToNewHotelIdMap = oldToNewHotelIdMap_bd.value();
				ImmutableBytesWritable row = new ImmutableBytesWritable(recResult.user_id.getBytes());
				Map<String,Double> hotelidCountMap = new HashMap<String,Double>();
				for(com.jje.bigdata.travel.recommend.LineCount lc:recResult.line_count) {
					String newHotelId = oldToNewHotelIdMap.get(lc.line_id);
					if(newHotelId != null){
						addHotelCount(hotelidCountMap, newHotelId, lc);
					}else{
						addHotelCount(hotelidCountMap, lc.line_id, lc);
					}
				}
				Put put = new Put((bc_date.value() + "_" + recResult.user_id).getBytes());//定义put
				for (Map.Entry<String, Double> entry : hotelidCountMap.entrySet()) {
					put.add("f".getBytes(),entry.getKey().getBytes(),String.valueOf(entry.getValue()).getBytes());
				}
				return new Tuple2<ImmutableBytesWritable, Put>(row,put);
			}

			private void addHotelCount(Map<String, Double> hotelidCountMap, String newHotelId, com.jje.bigdata.travel.recommend.LineCount lc) {
				Double newHotelCount = hotelidCountMap.get(newHotelId);
				if(newHotelCount == null){
					hotelidCountMap.put(newHotelId, lc.count);
				}else{
					hotelidCountMap.put(newHotelId, newHotelCount + lc.count);
				}
			}
		}).saveAsHadoopDataset(jobConf);
	}


	/**
	 * 根据时间序列获得用户的经纬度范围及其权重
	 * @param member_id input member id
	 * @param timeSeries input timeSeries
	 * @return 返回的每个double数组的5个元素分别表示最小经度，最大经度，最小纬度，最大纬度和权重
	 * @throws java.io.IOException
	 */
	static List<double[]> getGeographic(String member_id, String timeSeries, double radius) throws IOException {
		Get get = new Get(Bytes.toBytes(member_id+timeSeries));
		HTable tb;
		if(!"".equals(timeSeries)) {
			tb = tb_user_time_geographic_info;
		}else {
			tb = tb_user_geographic_info;
		}
		Result rs = tb.get(get);
		List<double[]> result = new ArrayList<double[]>();
//		result.add(new double[]{10.56236151638898, 202.01058646258504, 30.440873087522185, 56.80061903676694,0.3333333333333333});
//		return result;
		if(rs != null && !rs.isEmpty()) {
			 NavigableMap<byte[], byte[]> map = rs.getFamilyMap("info".getBytes());
			 for(byte[] key:map.keySet()) {
				 String[] lon_lat = Bytes.toString(key).split("_");
				 double[] range = getRange(Double.parseDouble(lon_lat[0]), Double.parseDouble(lon_lat[1]), radius);
				 result.add(new double[]{range[0], range[1], range[2], range[3], Double.parseDouble(Bytes.toString(map.get(key)))});
			 }
			 return result.isEmpty()?null:result;
		}
		else {
			return null;
		}
	}

	/**
	 * 根据时间序列获得用户的品牌及其权重
	 * @param member_id input member id
	 * @param timeSeries input timeSeries
	 * @return return value
	 * @throws java.io.IOException
	 */
	static Map<String, Double> getBrand_Value(String member_id, String timeSeries) throws IOException {
		Get get = new Get(Bytes.toBytes(member_id+timeSeries));
		HTable tb;
		if(!"".equals(timeSeries)) {
			tb = tb_user_time_series_prefer_info;
		}else {
			tb = tb_user_preference_info;
		}
		Result rs = tb.get(get);
		Map<String, Double> result = new HashMap<String, Double>();
//		result.put("JJINN", 0.009615384615384616);
//		return result;
		if(rs != null && !rs.isEmpty()) {
			 NavigableMap<byte[], byte[]> map = rs.getFamilyMap("brand".getBytes());
			 for(byte[] key:map.keySet()) {
				 result.put(Bytes.toString(key), Double.parseDouble(Bytes.toString(map.get(key))));
			 }
			 return result.isEmpty()?null:result;
		}
		else {
			return null;
		}
	}

	/**
	 * 根据时间序列获得用户的星级及其权重
	 * @param member_id input member id
	 * @param timeSeries input timeSeries
	 * @return return value
	 * @throws java.io.IOException
	 */
	static Map<String, Double> getStar_Value(String member_id, String timeSeries) throws IOException {
		Get get = new Get(Bytes.toBytes(member_id+timeSeries));
		HTable tb;
		if(!"".equals(timeSeries)) {
			tb = tb_user_time_series_prefer_info;
		}else {
			tb = tb_user_preference_info;
		}
		Result rs = tb.get(get);
		Map<String, Double> result = new HashMap<String, Double>();
//		result.put("INN", 0.009615384615384616);
//		return result;
		if(rs != null && !rs.isEmpty()) {
			NavigableMap<byte[], byte[]> map = rs.getFamilyMap("star".getBytes());
			for(byte[] key:map.keySet()) {
				result.put(Bytes.toString(key), Double.parseDouble(Bytes.toString(map.get(key))));
			}
			return result.isEmpty()?null:result;
		}
		else {
			return null;
		}
	}

	/**
	 * 根据给定开始时间和结束时间获得时间序列
	 * @param startStr starStr
	 * @param endStr endStr
	 * @return return value
	 * @throws java.io.IOException
	 * @throws java.text.ParseException
	 */
	static String[] getTimeSeriesByTimeSection(String startStr, String endStr) throws IOException, ParseException{
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		Date startTime = sdf.parse(startStr);
		Date endTime = sdf.parse(endStr);
		
		long day=(endTime.getTime()-startTime.getTime())/(24*60*60*1000);
		Calendar date = Calendar.getInstance();
		date.setTime(startTime);
		
		Set<String> monthSet = new HashSet<String>();
		Set<String> weekSet = new HashSet<String>();
		Set<String> holidaySet = new HashSet<String>();
		
		for (int i = 0; i <= day; i++) {
			int month = date.get(Calendar.MONTH) + 1;
			int week = date.get(Calendar.DAY_OF_WEEK)==1?7:date.get(Calendar.DAY_OF_WEEK)-1;
			String holiday = getHolidayByDate(sdf.format(date.getTime()));
			monthSet.add("#m#"+month);
			weekSet.add("#w#"+week);
			holidaySet.add("#h#"+holiday);
			
			date.add(Calendar.HOUR, 24);
		}
		String[] rs = new String[monthSet.size() + weekSet.size() + holidaySet.size()];
		int i = 0;
		for (String month:monthSet) {
			rs[i++] = month;
		}
		for (String week:weekSet) {
			rs[i++] = week;
		}
		for (String holiday:holidaySet) {
			rs[i++] = holiday;
		}
		return rs;
	}
	
	static String getHolidayByDate(String dateStr) throws IOException {
		Get get = new Get(dateStr.getBytes());
		Result rs = tb_holiday_map.get(get);
		if (rs != null && rs.size() > 0) {
			NavigableMap<byte[], byte[]> map = rs.getFamilyMap("f".getBytes());
			Set<byte[]> keys = map.keySet();
			String holiday = "";
			for (byte[] bs : keys) {
				holiday = holiday + "_" + Bytes.toString(bs);
			}
			return holiday.substring(1);
		}else {
			return "common";//平日
		}
	}
	
	/**
	 * 根据圆心、半径算出经纬度范围
	 * @param lon 圆心经度
	 * @param lat 圆心纬度
	 * @param r 半径（米）
	 * @return double[4] 最小经度，最大经度，最小纬度，最大纬度
	 */
	public static double[] getRange(double lon, double lat, double r) {
		double[] range = new double[4];
		// 角度转换为弧度
		double ns = lat * DEF_PI180;
		double sinNs = Math.sin(ns);
		double cosNs = Math.cos(ns);
		double cosTmp = Math.cos(r / DEF_R);
		// 经度的差值
		double lonDif = Math.acos((cosTmp - sinNs * sinNs) / (cosNs * cosNs)) / DEF_PI180;
		// 保存经度
		range[0] = lon - lonDif;
		range[1] = lon + lonDif;
		double m = 0 - 2 * cosTmp * sinNs;
		double n = cosTmp * cosTmp - cosNs * cosNs;
		double o1 = (0 - m - Math.sqrt(m * m - 4 * (n))) / 2;
		double o2 = (0 - m + Math.sqrt(m * m - 4 * (n))) / 2;
		// 纬度
		double lat1 = 180 / DEF_PI * Math.asin(o1);
		double lat2 = 180 / DEF_PI * Math.asin(o2);
		// 保存
		range[2] = lat1;
		range[3] = lat2;
		return range;
	}
	
	public static void main(String[] args) throws Exception {
		System.out.println("-------------------------startDate: " + new Date());
		if (args.length < 2) {
			System.out.println("/usr/lib/ngmr/run_app /czp/jje-bentley-1.0-SNAPSHOT.jar com.jje.bigdata.hotel.recommend.ComprehensiveRecommend ngmr-yarn-client /czp/jje-bentley-1.0-SNAPSHOT.jar");
			System.exit(1);
		}
		sc = new JavaSparkContext(args[0], "CollaborativeFilter",new SparkConf());
		sc.addJar(args[1]);
		sc.addJar("/usr/lib/spring/hsqldb.jar");
		
		properties = SceneConfig.create().getPara("hotelComprehensiveRecommend");
		String startDate = properties.getProperty("startDate");
		String endDate = properties.getProperty("endDate");
		int topLength = Integer.parseInt(properties.getProperty("topLength"));
		int days = Integer.parseInt(properties.getProperty("days"));
		double redius = Double.parseDouble(properties.getProperty("redius"));
		
		String[] timeSeries = getTimeSeriesByTimeSection(startDate, endDate);
		for (String timeSery : timeSeries) {
			System.out.println(timeSery);
		}
//		System.in.read();	
		
		SimpleDateFormat sdf1 = new SimpleDateFormat("yyyyMMdd");
		String dateStr = sdf1.format(new Date());
		Put put = new Put("what_day_is_it_today".getBytes());
		put.add("f".getBytes(), "date".getBytes(), dateStr.getBytes());
		
		tb_comprehensive_rec_data.put(put);//插入处理日期
		
		
		run(timeSeries, redius, topLength, days);
		System.out.println("-------------------------endDate: " + new Date());
		/*
		List<double[]> rs = getGeographic("1-PKT-4579","#h#common",20000);
		System.out.println("rs.size: " + rs.size());
		for (double[] r : rs) {
			System.out.println(r[0] + "\t" + r[1] + "\t" + r[2] + "\t" + r[3] + "\t-----" + r[4]);
		}*/
		/*
		Map<String, Double> rs = getBrand_Value("1-PKT-4579", "");
		for(String r:rs.keySet()) {
			System.out.println(r + ": " + rs.get(r));
		}
		Map<String, Double> rs = getStar_Value("1-PKT-4579", "#h#common");
		for(String r:rs.keySet()) {
			System.out.println(r + ": " + rs.get(r));
		}*/
	}
}
