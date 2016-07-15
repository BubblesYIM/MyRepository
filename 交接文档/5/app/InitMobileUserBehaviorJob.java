package com.jje.bigdata.userProfile.app;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import net.sf.json.JSONObject;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;

import com.google.common.base.Optional;
import com.jje.bigdata.census.ThreadSafeDateUtils;
import com.jje.bigdata.travel.bigdata.service.MobileService;
import com.jje.bigdata.travel.realRecommend.StreamingRealRecommendTravelLine;
import com.jje.bigdata.userProfile.app.domain.MobileSessionDto;
import com.jje.bigdata.userProfile.app.domain.MobileUserBehaviorDto;
import com.jje.bigdata.userProfile.app.utils.AddressUtils;
import com.jje.bigdata.util.SparkUtils;
import com.jje.bigdata.util.WritableTableOutputFormat;

public class InitMobileUserBehaviorJob implements Serializable {
	private static final long serialVersionUID = 1L;
	private static JavaSparkContext sc = null;
	private static Configuration inConf = null;
	private static Configuration outConf = null;
	
	private static boolean debugFlag = false;
	private static boolean testFlag = false;
	
	static {
		try {
			inConf = HBaseConfiguration.create();
			outConf = HBaseConfiguration.create();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public static void main(String[] args) throws IOException {
		System.out.println("============================InitMobileUserProfileJob Main开始" + new Date() + "=============================");
		if (args.length < 3) {
			System.out.println("/usr/lib/ngmr/run_app_min lexus-1.0-SNAPSHOT.jar com.jje.bigdata.userProfile.app.InitMobileUserBehaviorJob ngmr-yarn-client ./lexus-1.0-SNAPSHOT.jar true true");
			System.exit(1);
		}
		sc = new JavaSparkContext(args[0], "InitMobileUserProfileJob", new SparkConf().setAppName("InitMobileUserProfileJob"));

		sc.addJar(args[1]);

		System.out.println("#############args:" + Arrays.asList(args));

		if (args.length >= 3) {
			debugFlag = Boolean.parseBoolean(args[2]);
			System.out.println("###############debugFlag = " + debugFlag);
		}

		if (args.length >= 4) {
			testFlag = Boolean.parseBoolean(args[3]);
			System.out.println("###############testFlag = " + testFlag);
		}

		run();

		System.out.println("============================InitMobileUserProfileJob Main结束" + new Date() + "=============================");
	}
	
	private static void run() throws IOException {
		
		JavaPairRDD<String, MobileUserBehaviorDto> mobileViewRDD = MobileService.getMobileViewRDD(sc, inConf);
		if(debugFlag){
			SparkUtils.logRDDcount("mobileViewRDD", mobileViewRDD);
			mobileViewRDD.saveAsTextFile("/tmp/jarvis/mobileViewRDD");
		}
		
		JavaPairRDD<String, MobileUserBehaviorDto> mobileEventRDD = MobileService.getMobileEventRDD(sc, inConf);
		if(debugFlag){
			SparkUtils.logRDDcount("mobileEventRDD", mobileEventRDD);
			mobileEventRDD.saveAsTextFile("/tmp/jarvis/mobileEventRDD");
		}
		
		JavaPairRDD<String, MobileUserBehaviorDto> mobileBusinessRDD = MobileService.getMobileBusinessRDD(sc, inConf);
		if(debugFlag){
			SparkUtils.logRDDcount("mobileBusinessRDD", mobileBusinessRDD);
			mobileBusinessRDD.saveAsTextFile("/tmp/jarvis/mobileBusinessRDD");
		}
		
		JavaPairRDD<String, MobileUserBehaviorDto> userBehaviorRDD = mobileViewRDD.union(mobileEventRDD).union(mobileBusinessRDD);
//		JavaPairRDD<String, MobileUserBehaviorDto> userBehaviorRDD = mobileViewRDD.filter(new Function<Tuple2<String,MobileUserBehaviorDto>, Boolean>() {
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public Boolean call(Tuple2<String, MobileUserBehaviorDto> original) throws Exception {
//				String userID = original._2().getUserID();
//				return "4194190".equals(userID);
//			}
//		});
		
		SparkUtils.logRDDcount("userBehaviorRDD", userBehaviorRDD, debugFlag);
		
		JavaPairRDD<String, MobileSessionDto> sessionInfo = MobileService.getSessionInfo(sc, inConf);
		
		SparkUtils.logRDDcount("sessionInfo", sessionInfo, debugFlag);
				
		JavaPairRDD<String, Tuple2<MobileUserBehaviorDto, Optional<MobileSessionDto>>> filterLeftJoin = userBehaviorRDD.leftOuterJoin(sessionInfo).filter(new Function<Tuple2<String,Tuple2<MobileUserBehaviorDto,Optional<MobileSessionDto>>>, Boolean>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<String, Tuple2<MobileUserBehaviorDto, Optional<MobileSessionDto>>> sessionDto) throws Exception {
				MobileSessionDto session = sessionDto._2()._2().orNull();
				return session!=null;
			}
		});
		
		JavaPairRDD<MobileUserBehaviorDto, MobileSessionDto> behaviorAndSessionRDD = filterLeftJoin.map(new PairFunction<Tuple2<String, Tuple2<MobileUserBehaviorDto, Optional<MobileSessionDto>>>, MobileUserBehaviorDto, MobileSessionDto>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<MobileUserBehaviorDto, MobileSessionDto> call(Tuple2<String, Tuple2<MobileUserBehaviorDto, Optional<MobileSessionDto>>> original) throws Exception {
				MobileUserBehaviorDto behavior = original._2()._1();
				MobileSessionDto session = original._2()._2().get();
				String descript = behavior.getDescript();
				JSONObject json = JSONObject.fromObject(descript);
				if(json.has("lon") && json.has("lat") && json.has("hotel_lon") && json.has("hotel_lat")){
					double distance = AddressUtils.getDistance(json.getString("lat"), json.getString("lon"), json.getString("hotel_lat"), json.getString("hotel_lon"));
					json.put("distance", distance + "");
				}else if(json.has("hotel_lon") && json.has("hotel_lat")){
					String[] location = session.getLocation().split(",");
					String lat = location[0];
					String lon = location[1];
					try{
						double distance = AddressUtils.getDistance(lat, lon, json.getString("hotel_lat"), json.getString("hotel_lon"));
						json.put("distance", distance + "");
					}catch(Exception e){
						throw new RuntimeException( "session" + session.getRowkey() + "behavior" + behavior.getRowkey() + "," + behavior.getType() +"------lon&lat:" + session.getLocation() + "hotel_location:" + String.format("%s,%s",json.getString("hotel_lat"),json.getString("hotel_lon") ));
					}
				}
				behavior.setDescript(json.toString());
				return new Tuple2<MobileUserBehaviorDto, MobileSessionDto>(behavior, session);
			}
		});
		
		SparkUtils.logRDDcount("behaviorAndSessionRDD", behaviorAndSessionRDD, debugFlag);
		
		Map<String, String> advertUserID = behaviorAndSessionRDD.map(new PairFunction<Tuple2<MobileUserBehaviorDto, MobileSessionDto>, String, Tuple2<String, Date>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Tuple2<String, Date>> call(Tuple2<MobileUserBehaviorDto, MobileSessionDto> original) throws Exception {
				MobileUserBehaviorDto behavior = original._1();
				MobileSessionDto session = original._2();
				if(behaviorHasUserID(behavior)){
					return new Tuple2<String, Tuple2<String, Date>>(session.getAdvertID(), new Tuple2<String, Date>(behavior.getUserID(), behavior.getBeHaviorTime()));
				}else if(sessionHasUserID(session)){
					return new Tuple2<String, Tuple2<String, Date>>(session.getAdvertID(), new Tuple2<String, Date>(session.getUserID(), behavior.getBeHaviorTime()));
				}else{
					return null;
				}
			}
		}).filter(new Function<Tuple2<String,Tuple2<String, Date>>, Boolean>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<String, Tuple2<String, Date>> original) throws Exception {
				return original!=null;
			}
		}).reduceByKey(new Function2<Tuple2<String,Date>, Tuple2<String,Date>, Tuple2<String,Date>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Date> call(Tuple2<String, Date> arg0, Tuple2<String, Date> arg1) throws Exception {
				Date date1 = arg0._2();
				Date date2 = arg1._2();
				return date1.after(date2) ? arg0 : arg1;
			}
		}).map(new PairFunction<Tuple2<String, Tuple2<String, Date>>, String, String>() {
			private static final long serialVersionUID = -9172248589420085787L;

			@Override
			public Tuple2<String, String> call(Tuple2<String, Tuple2<String, Date>> original) throws Exception {
				return new Tuple2<String, String>(original._1(),original._2()._1());
			}
		}).collectAsMap();
		
		ConcurrentHashMap<String, String> conAdvertUserID = new ConcurrentHashMap<String, String>(advertUserID);
		final Broadcast<ConcurrentHashMap<String, String>> bAdvertUserID = sc.broadcast(conAdvertUserID);
		
		JavaPairRDD<MobileUserBehaviorDto, MobileSessionDto> finalBehaviorAndSessionRDD = behaviorAndSessionRDD.map(new PairFunction<Tuple2<MobileUserBehaviorDto, MobileSessionDto>, MobileUserBehaviorDto, MobileSessionDto>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<MobileUserBehaviorDto, MobileSessionDto> call(Tuple2<MobileUserBehaviorDto, MobileSessionDto> original) throws Exception {
				MobileUserBehaviorDto behavior = original._1();
				MobileSessionDto session = original._2();
				String realUserID = null;
				
				if(behaviorHasUserID(behavior)){
					realUserID = behavior.getUserID();
				}else if(sessionHasUserID(session)){
					realUserID = session.getUserID();
				}else{
					ConcurrentHashMap<String, String> advertUserID = bAdvertUserID.value();
					realUserID = advertUserID.get(session.getAdvertID());
				}
				
				behavior.setRealUserID(realUserID);
				
				return original;
			}
		});
		
		SparkUtils.logRDDcount("finalBehaviorAndSessionRDD", finalBehaviorAndSessionRDD, debugFlag);
		saveDataToHbase(finalBehaviorAndSessionRDD);
	}

	protected static boolean sessionHasUserID(MobileSessionDto session) {
		String userID = session.getUserID();
		return SparkUtils.isNotBlank(userID) && !"0".equals(userID);
	}

	private static Boolean behaviorHasUserID(MobileUserBehaviorDto behavior) {
		String userID = behavior.getUserID();
		return SparkUtils.isNotBlank(userID) && !"0".equals(userID);
	}

	private static void saveDataToHbase(JavaPairRDD<MobileUserBehaviorDto, MobileSessionDto> behaviorAndSessionRDD) {
		outConf.set(TableOutputFormat.OUTPUT_TABLE, "TB_MOBILE_BEHAVIOR_DATA");
		JobConf jobConf = new JobConf(outConf, StreamingRealRecommendTravelLine.class);
		jobConf.setOutputFormat(WritableTableOutputFormat.class);

		behaviorAndSessionRDD.map(new PairFunction<Tuple2<MobileUserBehaviorDto, MobileSessionDto>, ImmutableBytesWritable, Put>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<ImmutableBytesWritable, Put> call(Tuple2<MobileUserBehaviorDto, MobileSessionDto> input) throws Exception {
				try {
					Map<String, String> fieldMap = new HashMap<String, String>();
					addField(input._1(), MobileUserBehaviorDto.class, "B_", fieldMap);
					addField(input._2(), MobileSessionDto.class, "S_", fieldMap);

					String rowkey = input._1().getRowkey() + "#" + input._2().getRowkey();
					ImmutableBytesWritable row = new ImmutableBytesWritable(Bytes.toBytes(rowkey));
					if (fieldMap.isEmpty()) {
						return new Tuple2<ImmutableBytesWritable, Put>(row, null);
					}

					Put put = new Put(Bytes.toBytes(rowkey));

					for (Map.Entry<String, String> entry : fieldMap.entrySet()) {
						put.add(Bytes.toBytes("info"), Bytes.toBytes(entry.getKey()), Bytes.toBytes(entry.getValue()));
					}

					return new Tuple2<ImmutableBytesWritable, Put>(row, put);
				} catch (Exception e) {
		            StringWriter sw = new StringWriter();
		            PrintWriter pw = new PrintWriter(sw);
		            e.printStackTrace(pw);
					throw new RuntimeException(e.getMessage()+"::::::"+input.toString()+"\n"+pw, e);
				}
			}

			private void addField(Object obj, Class<?> clazz, String prefix, Map<String, String> fieldMap) throws IllegalArgumentException, IllegalAccessException {
				if (obj == null) {
					return;
				}
				Field[] fields = clazz.getDeclaredFields();
				
				for (Field field : fields) {
					field.setAccessible(true);
					String dataStr = null;
					String fieldName = field.getName();
					Object dataObj = field.get(obj);
					if( "serialVersionUID".equals(fieldName) || dataObj==null){
						continue;
					}
					if (dataObj instanceof Date) {
						Date date = (Date) dataObj;
						dataStr = ThreadSafeDateUtils.secFormat(date);
					}else{
						dataStr = dataObj.toString();
					}
					if (StringUtils.isNotBlank(dataStr)) {
						fieldMap.put(prefix + fieldName, dataStr.trim());
					}
				}
			}

		}).filter(new Function<Tuple2<ImmutableBytesWritable, Put>, Boolean>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<ImmutableBytesWritable, Put> arg0) throws Exception {
				return arg0._2() != null;
			}

		}).saveAsHadoopDataset(jobConf);
	}
}
