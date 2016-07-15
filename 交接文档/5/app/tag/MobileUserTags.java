package com.jje.bigdata.userProfile.app.tag;

import com.jje.bigdata.census.ThreadSafeDateUtils;
import com.jje.bigdata.travel.bigdata.service.MobileService;
import com.jje.bigdata.userProfile.app.domain.MobileSessionDto;
import com.jje.bigdata.userProfile.app.domain.MobileUserBehaviorDto;
import com.jje.bigdata.userProfile.app.domain.MobileUserBehaviorDto.BehaviorType;
import com.jje.bigdata.userProfile.app.domain.MobileUserProfileInfo;
import com.jje.bigdata.userProfile.travel.domain.NumberMap;
import com.jje.bigdata.userProfile.travel.tag.TravelUserConsumptionProfile;
import com.jje.bigdata.util.BigdataMemberInfoUtils;
import com.jje.bigdata.util.SparkUtils;
import com.jje.bigdata.util.WritableTableOutputFormat;
import net.sf.json.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import scala.collection.mutable.StringBuilder;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class MobileUserTags implements Serializable {
	private static final long serialVersionUID = 1L;
	private static JavaSparkContext sc;
	private static Configuration conf;
	private static HConnection conn;
	private static HTableInterface locationTable;

	private static final byte[] FAMILY = Bytes.toBytes("info");
	private static final byte[] SYSTEM_OS = Bytes.toBytes("SYSTEM_OS");
	private static final byte[] BRAND = Bytes.toBytes("BRAND");
	private static final byte[] ADVERT_ID = Bytes.toBytes("ADVERT_ID");
	private static final byte[] LAST_OPEN_TIME = Bytes.toBytes("LAST_OPEN_TIME");
	private static final byte[] LAST_OPEN_TIME_BETWEEN_DAYS = Bytes.toBytes("LAST_OPEN_TIME_BETWEEN_DAYS");
	private static final byte[] LBS_INFO = Bytes.toBytes("LBS_INFO");
	private static final byte[] QUERY_WORDS = Bytes.toBytes("QUERY_WORDS");
	private static final byte[] BOOKING_DISTANCE = Bytes.toBytes("BOOKING_DISTANCE");
	private static final byte[] PREFERENCE_WITH_VIEW_HOUR = Bytes.toBytes("PREFERENCE_WITH_VIEW_HOUR");
	private static final byte[] APP_VISIT_TIMES = Bytes.toBytes("APP_VISIT_TIMES");
	private static final byte[] HAS_BEEN_BOOKING = Bytes.toBytes("HAS_BEEN_BOOKING");
	private static final byte[] FOCUS_ACTIVITY = Bytes.toBytes("FOCUS_ACTIVITY");
	private static final byte[] IS_MAP_BOOKING = Bytes.toBytes("IS_MAP_BOOKING");
	private static final byte[] ZONE_SCREEN = Bytes.toBytes("ZONE_SCREEN");
	private static final byte[] BRAND_SCREEN = Bytes.toBytes("BRAND_SCREEN");
	private static final byte[] SORT_TYPE = Bytes.toBytes("SORT_TYPE");
	private static final byte[] PREFERENCE_CITY = Bytes.toBytes("PREFERENCE_CITY");
	private static boolean logSwitch;

	static {
		try {
			conf = HBaseConfiguration.create();
			conf.set(TableInputFormat.SCAN, SparkUtils.convertScanToString(new Scan()));
			conn = HConnectionManager.createConnection(conf);
			locationTable = conn.getTable("TB_REC_BIGDATA_LOCALTION_DATA");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		Date startDate = new Date();
		Date endDate;
		System.out.println(startDate);
		System.out.println(args.length);
		if (args.length < 3) {
			System.exit(-1);
		}
		System.out.println("/usr/lib/ngmr/run_app_min lexus-1.0-SNAPSHOT.jar com.jje.bigdata.userProfile.app.tag.MobileUserTags ngmr-yarn-client lexus-1.0-SNAPSHOT.jar true");
		try {
			SparkConf sparkConf = new SparkConf().setAppName("MobileUserTags");
			sc = new JavaSparkContext(args[0], "MobileUserTags", sparkConf);
			sc.addJar(args[1]);
			logSwitch = Boolean.parseBoolean(args[2]);
			run();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		} finally {
			endDate = new Date();
			try {
				conn.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			System.out.println(endDate);
			long durion = endDate.getTime() - startDate.getTime();
			System.out.println("耗时:" + durion + "毫秒");
		}
	}

	private static void run() throws IOException {
		Configuration outConfig = HBaseConfiguration.create();
		outConfig.set(TableOutputFormat.OUTPUT_TABLE, "TB_MOBILE_USER_PROFILE");
		JobConf outJobConfig = new JobConf(outConfig, TravelUserConsumptionProfile.class);
		outJobConfig.setOutputFormat(WritableTableOutputFormat.class);

		JavaPairRDD<String, MobileUserProfileInfo> mobileUserProfile = BigdataMemberInfoUtils.getBigdataMobileMemberInfo(sc, conf).filter(new Function<Tuple2<MobileUserBehaviorDto,MobileSessionDto>, Boolean>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<MobileUserBehaviorDto, MobileSessionDto> original) throws Exception {
				String realUserID = original._1().getRealUserID();
				return SparkUtils.isNotBlank(realUserID);
			}
		}).map(new PairFunction<Tuple2<MobileUserBehaviorDto, MobileSessionDto>, String, MobileUserProfileInfo>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, MobileUserProfileInfo> call(Tuple2<MobileUserBehaviorDto, MobileSessionDto> original) throws Exception {
				MobileUserBehaviorDto behavior = original._1();


				MobileSessionDto session = original._2();
				String realUserID = behavior.getRealUserID();
				MobileUserProfileInfo info = new MobileUserProfileInfo();
				info.setBehavior(behavior);
				info.setSession(session);
				String SystemVersion;
				String brand = behavior.getSystemType();
				if( "Apple".equals(brand) ){
					SystemVersion = "IOS " + session.getOsVersion();
				}else{
					SystemVersion = session.getOsVersion();
				}
				info.setBrand(new NumberMap(brand, 1));
				info.setSystemOS(new NumberMap(SystemVersion, 1));
				info.setAdvertID(new NumberMap(session.getAdvertID(), 1));
				Date startViewTime = session.getStartViewTime();
				info.setLastOpenTime(startViewTime);

				String hours = ThreadSafeDateUtils.getTimeField(startViewTime, Calendar.HOUR_OF_DAY) + "";
				info.setPreferenceWithViewHour(new NumberMap(hours, 1));

				info.setAppVisitTimes(new NumberMap(hours, 1));
				//LBS信息
				StringBuilder LBSinfo = new StringBuilder();
				String openAppTime = ThreadSafeDateUtils.minFormat(startViewTime);
				LBSinfo.append(openAppTime);
				String location = session.getLocation();

				LBSinfo.append("/" + location);
				Result result = locationTable.get(new Get(Bytes.toBytes(location)));
				if(result!=null && !result.isEmpty()){
					String province = SparkUtils.getValue(result, "info", "province");
					LBSinfo.append("/" + province);
					String city = SparkUtils.getValue(result, "info", "city");
					LBSinfo.append("/" + city);
				}
				info.setLBSInfo(new NumberMap(LBSinfo.toString(), 1));
				long endViewTime = session.getEndViewTime().getTime();
				String during = ThreadSafeDateUtils.formatTime(endViewTime - startViewTime.getTime());
				info.setAppVisitTimes(new NumberMap(openAppTime + "/" + during, 1));

				JSONObject json = JSONObject.fromObject(behavior.getDescript());

				if(json.has("hotelID") && json.has("distance") && json.has("hotel_lat") && json.has("hotel_lon")){
					String hotelID = json.getString("hotelID");
					String distance = json.getString("distance");
					String hotelLocation = json.getString("hotel_lat") + "," + json.getString("hotel_lon");
					if(json.has("lat")  && json.has("lon")){
						location = json.getString("lat") + "," + json.getString("lon");
					}
					info.setBookingDistance(new NumberMap(location + "/" + hotelLocation + "/" + hotelID + "/" + distance, 1));
				}
				String behaviorName = "";
				if(json.has("behaviorName")){
					behaviorName = json.getString("behaviorName");
				}

				BehaviorType type = behavior.getType();
				//搜索关键字

				info.setQueryWords(new NumberMap());
				info.setZoneScreen(new NumberMap());
				info.setBrandScreen(new NumberMap());
				info.setSortType(new NumberMap());
				info.setPreferenceCity(new NumberMap());

				if(json.has("words")){
					String monthFormat = ThreadSafeDateUtils.monthFormat(behavior.getBeHaviorTime());
					String queryWords = json.getString("words");
					info.setQueryWords(new NumberMap(monthFormat + "/" + queryWords, 1));
				}
				if(json.has("cityName")){
					Date beHaviorTime = behavior.getBeHaviorTime();
					String month = beHaviorTime!=null?ThreadSafeDateUtils.getTimeField(beHaviorTime, Calendar.MONTH) + 1 + "月":"unknow";
					String cityName = json.getString("cityName");
					info.setPreferenceCity(new NumberMap(month + "/" + cityName, 1));
				}

				if(BehaviorType.EVENT == type && "点击筛选区域".equals(behaviorName) && json.has("behaviorValue")){
					JSONObject behaviorValue = json.getJSONObject("behaviorValue");
					String zone = behaviorValue.getString("zone");
					info.setZoneScreen(new NumberMap(zone, 1));
				}else if(BehaviorType.EVENT == type && ("点击筛选品牌".equals(behaviorName) || "点击筛选品牌:".equals(behaviorName)) && json.has("behaviorValue")){
					JSONObject behaviorValue = json.getJSONObject("behaviorValue");
					String brandScreen = behaviorValue.getString("brand");
					info.setBrandScreen(new NumberMap(brandScreen, 1));
				}else if(BehaviorType.EVENT == type && "点击排序方式".equals(behaviorName) && json.has("behaviorValue")){
					JSONObject behaviorValue = json.getJSONObject("behaviorValue");
					String sortType = behaviorValue.getString("sortType");
					info.setSortType(new NumberMap(sortType, 1));
				}

				if("预录入酒店订单".equals(behaviorName)){
					info.setHasBeenBooking(true);
				}else if("营销中心列表页面".equals(behaviorName)){
					info.setHasFocusActivity(true);
				}
				return new Tuple2<String, MobileUserProfileInfo>(realUserID, info);
			}
		}).cache();

		saveMobileSystemTag(mobileUserProfile, outJobConfig);
		saveBrandTag(mobileUserProfile, outJobConfig);
		saveMobileAdvertTag(mobileUserProfile, outJobConfig);
		saveLastOpenTimeTag(mobileUserProfile, outJobConfig);
		saveLBSInfoTag(mobileUserProfile, outJobConfig);
		saveQueryWordsTag(mobileUserProfile, outJobConfig);
		saveBookingDistanceTag(mobileUserProfile, outJobConfig);
		saveAppVisitTimesTag(mobileUserProfile, outJobConfig);
		saveHasBeenBookingTag(mobileUserProfile, outJobConfig);
		saveUseMapBookingTag(mobileUserProfile, outJobConfig);
		saveZoneScreenTag(mobileUserProfile, outJobConfig);
		saveBrandScreenTag(mobileUserProfile, outJobConfig);
		saveSortTypeTag(mobileUserProfile, outJobConfig);
		savePreferenceCityTag(mobileUserProfile, outJobConfig);

		JavaPairRDD<String, MobileUserProfileInfo> distinctMobileUserProfile = mobileUserProfile.map(new PairFunction<Tuple2<String, MobileUserProfileInfo>, String, Tuple2<String, MobileUserProfileInfo>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Tuple2<String, MobileUserProfileInfo>> call(Tuple2<String, MobileUserProfileInfo> original) throws Exception {
				String sessionID = original._2().getSession().getSessionID();
				String realUserID = original._2().getBehavior().getRealUserID();
				return new Tuple2<String, Tuple2<String, MobileUserProfileInfo>>(realUserID + "_" + sessionID, original);
			}
		}).reduceByKey(new Function2<Tuple2<String,MobileUserProfileInfo>, Tuple2<String,MobileUserProfileInfo>, Tuple2<String,MobileUserProfileInfo>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, MobileUserProfileInfo> call(Tuple2<String, MobileUserProfileInfo> userProfileInfo1, Tuple2<String, MobileUserProfileInfo> userProfileInfo2) throws Exception {
				boolean hasFocusActivity = userProfileInfo1._2().isHasFocusActivity();
				return hasFocusActivity ? userProfileInfo1 : userProfileInfo2;
			}
		}).map(new PairFunction<Tuple2<String, Tuple2<String, MobileUserProfileInfo>>, String, MobileUserProfileInfo>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, MobileUserProfileInfo> call(Tuple2<String, Tuple2<String, MobileUserProfileInfo>> original) throws Exception {
				return original._2();
			}
		});
		SparkUtils.logRDDcount("distinctMobileUserProfile", distinctMobileUserProfile, logSwitch);

		savePreferenceWithViewHourTag(distinctMobileUserProfile, outJobConfig);
		saveFocusAcrtivityTag(distinctMobileUserProfile, outJobConfig);
	}

	private static void saveUseMapBookingTag(JavaPairRDD<String, MobileUserProfileInfo> mobileUserProfile, JobConf outJobConfig) throws IOException {
//		Map<String, List<MobileUserBehaviorDto>> views = MobileService.
//				getMobileViewRDD(sc, conf).groupByKey().collectAsMap();
//		ConcurrentHashMap<String, List<MobileUserBehaviorDto>> conViews = new ConcurrentHashMap<String, List<MobileUserBehaviorDto>>(views);
//		final Broadcast<ConcurrentHashMap<String, List<MobileUserBehaviorDto>>> bViews = sc.broadcast(conViews);


		JavaPairRDD<String, List<MobileUserBehaviorDto>> views = MobileService.getMobileViewRDD(sc, conf).groupByKey();

		JavaPairRDD<ImmutableBytesWritable, Put> result = mobileUserProfile.map(new PairFunction<Tuple2<String,MobileUserProfileInfo>, String, Tuple2<String, MobileUserProfileInfo>>() {
			@Override
			public Tuple2<String, Tuple2<String, MobileUserProfileInfo>> call(Tuple2<String, MobileUserProfileInfo> original) throws Exception {
				String sessionID = original._2().getSession().getSessionID();
				return new Tuple2<String, Tuple2<String, MobileUserProfileInfo>>(sessionID, original);
			}
		}).join(views).map(new PairFunction<Tuple2<String,Tuple2<Tuple2<String,MobileUserProfileInfo>,List<MobileUserBehaviorDto>>>, String, MobileUserProfileInfo>() {
			@Override
			public Tuple2<String, MobileUserProfileInfo> call(Tuple2<String, Tuple2<Tuple2<String, MobileUserProfileInfo>, List<MobileUserBehaviorDto>>> joins) throws Exception {
				Tuple2<String, MobileUserProfileInfo> original = joins._2()._1();
				List<MobileUserBehaviorDto> views = joins._2()._2();
				MobileUserProfileInfo info = original._2();
				MobileUserBehaviorDto behavior = info.getBehavior();
				JSONObject json = JSONObject.fromObject(behavior.getDescript());
				if(!json.has("behaviorName")){
					return original;
				}
				if("预录入酒店订单".equals(json.getString("behaviorName"))){
//					String sessionID = original._2().getSession().getSessionID();
					Date bookingTime = behavior.getBeHaviorTime();
					MobileUserBehaviorDto lastView = null;
					for(MobileUserBehaviorDto dto : views){
						JSONObject obj = JSONObject.fromObject(dto.getDescript());
						String behaviorName = null;
						if(obj.has("behaviorName")){
							behaviorName = obj.getString("behaviorName");
						}
						Date viewTime = dto.getBeHaviorTime();
						if( viewTime.before(bookingTime) && ("酒店列表页面".equals(behaviorName) || "酒店搜索页面".equals(behaviorName) || "酒店地图页面".equals(behaviorName)) ){
							if(lastView==null){
								lastView = dto;
							}else{
								Date lastViewTime = lastView.getBeHaviorTime();
								if( viewTime.after(lastViewTime) ){
									lastView = dto;
								}
							}
						}
					}
					if(lastView!=null){
						JSONObject obj = JSONObject.fromObject(lastView.getDescript());
						String behaviorName = obj.getString("behaviorName");
						if("酒店地图页面".equals(behaviorName)){
							info.setMapBooking(true);
						}
					}
				}
				return original;
			}
		}).reduceByKey(new Function2<MobileUserProfileInfo, MobileUserProfileInfo, MobileUserProfileInfo>() {
			private static final long serialVersionUID = 1L;

			@Override
			public MobileUserProfileInfo call(MobileUserProfileInfo userProfileInfo1, MobileUserProfileInfo userProfileInfo2) throws Exception {
				boolean mapBooking = userProfileInfo1.isMapBooking();
				return mapBooking ? userProfileInfo1 : userProfileInfo2;
			}
		}).map(new PairFunction<Tuple2<String, MobileUserProfileInfo>, ImmutableBytesWritable, Put>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<ImmutableBytesWritable, Put> call(Tuple2<String, MobileUserProfileInfo> original) throws Exception {
				boolean isMapBooking = original._2().isMapBooking();
				byte[] userID = Bytes.toBytes(original._1());
				ImmutableBytesWritable rowkey = new ImmutableBytesWritable(userID);

				Put put = new Put(userID);
				put.add(FAMILY, IS_MAP_BOOKING, Bytes.toBytes(isMapBooking + ""));
				return new Tuple2<ImmutableBytesWritable, Put>(rowkey, put);
			}
		});


//		JavaPairRDD<ImmutableBytesWritable, Put> result = mobileUserProfile.map(new PairFunction<Tuple2<String, MobileUserProfileInfo>, String, MobileUserProfileInfo>() {
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public Tuple2<String, MobileUserProfileInfo> call(Tuple2<String, MobileUserProfileInfo> original) throws Exception {
//				MobileUserProfileInfo info = original._2();
//				MobileUserBehaviorDto behavior = info.getBehavior();
//				JSONObject json = JSONObject.fromObject(behavior.getDescript());
//				if(!json.has("behaviorName")){
//					return original;
//				}
//				if("预录入酒店订单".equals(json.getString("behaviorName"))){
//					String sessionID = original._2().getSession().getSessionID();
//					ConcurrentHashMap<String, List<MobileUserBehaviorDto>> views = bViews.value();
//					List<MobileUserBehaviorDto> list = views.get(sessionID);
//					Date bookingTime = behavior.getBeHaviorTime();
//					MobileUserBehaviorDto lastView = null;
//					for(MobileUserBehaviorDto dto : list){
//						JSONObject obj = JSONObject.fromObject(dto.getDescript());
//						String behaviorName = obj.getString("behaviorName");
//						Date viewTime = dto.getBeHaviorTime();
//						if( viewTime.before(bookingTime) && ("酒店列表页面".equals(behaviorName) || "酒店搜索页面".equals(behaviorName) || "酒店地图页面".equals(behaviorName)) ){
//							if(lastView==null){
//								lastView = dto;
//							}else{
//								Date lastViewTime = lastView.getBeHaviorTime();
//								if( viewTime.after(lastViewTime) ){
//									lastView = dto;
//								}
//							}
//						}
//					}
//					if(lastView!=null){
//						JSONObject obj = JSONObject.fromObject(lastView.getDescript());
//						String behaviorName = obj.getString("behaviorName");
//						if("酒店地图页面".equals(behaviorName)){
//							info.setMapBooking(true);
//						}
//					}
//				}
//				return original;
//			}
//		}).reduceByKey(new Function2<MobileUserProfileInfo, MobileUserProfileInfo, MobileUserProfileInfo>() {
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public MobileUserProfileInfo call(MobileUserProfileInfo userProfileInfo1, MobileUserProfileInfo userProfileInfo2) throws Exception {
//				boolean mapBooking = userProfileInfo1.isMapBooking();
//				return mapBooking ? userProfileInfo1 : userProfileInfo2;
//			}
//		}).map(new PairFunction<Tuple2<String, MobileUserProfileInfo>, ImmutableBytesWritable, Put>() {
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public Tuple2<ImmutableBytesWritable, Put> call(Tuple2<String, MobileUserProfileInfo> original) throws Exception {
//				boolean isMapBooking = original._2().isMapBooking();
//				byte[] userID = Bytes.toBytes(original._1());
//				ImmutableBytesWritable rowkey = new ImmutableBytesWritable(userID);
//
//				Put put = new Put(userID);
//				put.add(FAMILY, IS_MAP_BOOKING, Bytes.toBytes(isMapBooking + ""));
//				return new Tuple2<ImmutableBytesWritable, Put>(rowkey, put);
//			}
//		});
		
		SparkUtils.logRDDcount("useMapBooking", result, logSwitch);
		result.saveAsHadoopDataset(outJobConfig);
	}

	private static void saveFocusAcrtivityTag(JavaPairRDD<String, MobileUserProfileInfo> distinctMobileUserProfile, JobConf outJobConfig) {
		JavaPairRDD<ImmutableBytesWritable, Put> result = distinctMobileUserProfile.map(new PairFunction<Tuple2<String, MobileUserProfileInfo>, String, MobileUserProfileInfo>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, MobileUserProfileInfo> call(Tuple2<String, MobileUserProfileInfo> original) throws Exception {
				MobileUserProfileInfo info = original._2();
				boolean hasFocusActivity = info.isHasFocusActivity();
				NumberMap activitySessionNumber = new NumberMap();
				if(hasFocusActivity){
					activitySessionNumber.put("focus", new BigDecimal(1));
				}
				activitySessionNumber.put("all", new BigDecimal(1));
				info.setActivitySessionNumber(activitySessionNumber);
				return original;
			}
		}).reduceByKey(new Function2<MobileUserProfileInfo, MobileUserProfileInfo, MobileUserProfileInfo>() {
			private static final long serialVersionUID = 1L;

			@Override
			public MobileUserProfileInfo call(MobileUserProfileInfo userProfileInfo1, MobileUserProfileInfo userProfileInfo2) throws Exception {
				NumberMap map = new NumberMap();
				map.putAll(userProfileInfo1.getActivitySessionNumber());
				map.putAll(userProfileInfo2.getActivitySessionNumber());
				userProfileInfo1.setActivitySessionNumber(map);
				return userProfileInfo1;
			}
		}).map(new PairFunction<Tuple2<String, MobileUserProfileInfo>, ImmutableBytesWritable, Put>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<ImmutableBytesWritable, Put> call(Tuple2<String, MobileUserProfileInfo> original) throws Exception {
				NumberMap activitySessionNumber = original._2().getActivitySessionNumber();
				BigDecimal allSessionNumber = activitySessionNumber.get("all");
				BigDecimal focusSessionNumber = activitySessionNumber.get("focus");
				byte[] userID = Bytes.toBytes(original._1());
				ImmutableBytesWritable rowkey = new ImmutableBytesWritable(userID);
				Put put = new Put(userID);
				if(focusSessionNumber == null){
					put.add(FAMILY, FOCUS_ACTIVITY, Bytes.toBytes(0 + "/" +0));
					return new Tuple2<ImmutableBytesWritable, Put>(rowkey, put);
				}else{
					BigDecimal percent = focusSessionNumber.divide(allSessionNumber, 2, RoundingMode.HALF_UP);
					put.add(FAMILY, FOCUS_ACTIVITY, Bytes.toBytes(focusSessionNumber + "/" +percent));
					return new Tuple2<ImmutableBytesWritable, Put>(rowkey, put);
				}
			}
		});

		SparkUtils.logRDDcount("focusActrivity", result, logSwitch);
		result.saveAsHadoopDataset(outJobConfig);
	}

	private static void saveHasBeenBookingTag(JavaPairRDD<String, MobileUserProfileInfo> mobileUserProfile, JobConf outJobConfig) {
		JavaPairRDD<ImmutableBytesWritable, Put> result = mobileUserProfile.reduceByKey(new Function2<MobileUserProfileInfo, MobileUserProfileInfo, MobileUserProfileInfo>() {
			private static final long serialVersionUID = 1L;

			@Override
			public MobileUserProfileInfo call(MobileUserProfileInfo userProfileInfo1, MobileUserProfileInfo userProfileInfo2) throws Exception {
				return userProfileInfo1.isHasBeenBooking()?userProfileInfo1:userProfileInfo2;
			}
		}).map(new PairFunction<Tuple2<String, MobileUserProfileInfo>, ImmutableBytesWritable, Put>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<ImmutableBytesWritable, Put> call(Tuple2<String, MobileUserProfileInfo> original) throws Exception {
				boolean hasBeenBooking = original._2().isHasBeenBooking();
				byte[] userID = Bytes.toBytes(original._1());
				ImmutableBytesWritable rowkey = new ImmutableBytesWritable(userID);
				
				Put put = new Put(userID);
				put.add(FAMILY, HAS_BEEN_BOOKING, Bytes.toBytes(hasBeenBooking + ""));
				return new Tuple2<ImmutableBytesWritable, Put>(rowkey, put);
			}
		});
		
		SparkUtils.logRDDcount("hasBeenBookingTag", result, logSwitch);
		result.saveAsHadoopDataset(outJobConfig);
	}

	private static void saveBrandTag(JavaPairRDD<String, MobileUserProfileInfo> mobileUserProfile, JobConf outJobConfig) {
		JavaPairRDD<ImmutableBytesWritable, Put> result = mobileUserProfile.reduceByKey(new Function2<MobileUserProfileInfo, MobileUserProfileInfo, MobileUserProfileInfo>() {
			private static final long serialVersionUID = 1L;

			@Override
			public MobileUserProfileInfo call(MobileUserProfileInfo userProfileInfo1, MobileUserProfileInfo userProfileInfo2) throws Exception {
				NumberMap result = new NumberMap();
				result.putAll(userProfileInfo1.getBrand());
				result.putAll(userProfileInfo2.getBrand());
				userProfileInfo1.setBrand(result);
				return userProfileInfo1;
			}
		}).map(new PairFunction<Tuple2<String, MobileUserProfileInfo>, ImmutableBytesWritable, Put>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<ImmutableBytesWritable, Put> call(Tuple2<String, MobileUserProfileInfo> original) throws Exception {
				NumberMap brand = original._2().getBrand();
				byte[] userID = Bytes.toBytes(original._1());
				ImmutableBytesWritable rowkey = new ImmutableBytesWritable(userID);
				
				Put put = new Put(userID);
				put.add(FAMILY, BRAND, Bytes.toBytes(brand.getMapKeyString()));
				return new Tuple2<ImmutableBytesWritable, Put>(rowkey, put);
			}
		});
		
		SparkUtils.logRDDcount("brandTag", result, logSwitch);
		result.saveAsHadoopDataset(outJobConfig);
	}

	private static void saveAppVisitTimesTag(JavaPairRDD<String, MobileUserProfileInfo> mobileUserProfile, JobConf outJobConfig) {
		JavaPairRDD<ImmutableBytesWritable, Put> result = mobileUserProfile.reduceByKey(new Function2<MobileUserProfileInfo, MobileUserProfileInfo, MobileUserProfileInfo>() {
			private static final long serialVersionUID = 1L;

			@Override
			public MobileUserProfileInfo call(MobileUserProfileInfo userProfileInfo1, MobileUserProfileInfo userProfileInfo2) throws Exception {
				NumberMap result = new NumberMap();
				result.putAll(userProfileInfo1.getAppVisitTimes());
				result.putAll(userProfileInfo2.getAppVisitTimes());
				userProfileInfo1.setAppVisitTimes(result);
				return userProfileInfo1;
			}
		}).map(new PairFunction<Tuple2<String, MobileUserProfileInfo>, ImmutableBytesWritable, Put>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<ImmutableBytesWritable, Put> call(Tuple2<String, MobileUserProfileInfo> original) throws Exception {
				NumberMap appVisitTimes = original._2().getAppVisitTimes();
				byte[] userID = Bytes.toBytes(original._1());
				ImmutableBytesWritable rowkey = new ImmutableBytesWritable(userID);
				
				Put put = new Put(userID);
				put.add(FAMILY, APP_VISIT_TIMES, Bytes.toBytes(appVisitTimes.getMapKeyString()));
				return new Tuple2<ImmutableBytesWritable, Put>(rowkey, put);
			}
		});
		
		SparkUtils.logRDDcount("appVisitTimes", result, logSwitch);
		result.saveAsHadoopDataset(outJobConfig);
	}

	private static void savePreferenceWithViewHourTag(JavaPairRDD<String, MobileUserProfileInfo> distinctMobileUserProfile, JobConf outJobConfig) {
		JavaPairRDD<ImmutableBytesWritable, Put> result = distinctMobileUserProfile.reduceByKey(new Function2<MobileUserProfileInfo, MobileUserProfileInfo, MobileUserProfileInfo>() {
			private static final long serialVersionUID = 1L;

			@Override
			public MobileUserProfileInfo call(MobileUserProfileInfo userProfileInfo1, MobileUserProfileInfo userProfileInfo2) throws Exception {
				NumberMap result = new NumberMap();
				result.putAll(userProfileInfo1.getPreferenceWithViewHour());
				result.putAll(userProfileInfo2.getPreferenceWithViewHour());
				userProfileInfo1.setPreferenceWithViewHour(result);
				return userProfileInfo1;
			}
		}).map(new PairFunction<Tuple2<String, MobileUserProfileInfo>, ImmutableBytesWritable, Put>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<ImmutableBytesWritable, Put> call(Tuple2<String, MobileUserProfileInfo> original) throws Exception {
				NumberMap preferenceWithViewHour = original._2().getPreferenceWithViewHour();
				byte[] userID = Bytes.toBytes(original._1());
				ImmutableBytesWritable rowkey = new ImmutableBytesWritable(userID);
				
				Put put = new Put(userID);
				put.add(FAMILY, PREFERENCE_WITH_VIEW_HOUR, Bytes.toBytes(preferenceWithViewHour.getMapString()));
				return new Tuple2<ImmutableBytesWritable, Put>(rowkey, put);
			}
		});
		
		SparkUtils.logRDDcount("preferenceWithViewHour", result, logSwitch);
		result.saveAsHadoopDataset(outJobConfig);
	}

	private static void saveBookingDistanceTag(JavaPairRDD<String, MobileUserProfileInfo> mobileUserProfile, JobConf outJobConfig) {
		JavaPairRDD<ImmutableBytesWritable, Put> result = mobileUserProfile.filter(new Function<Tuple2<String,MobileUserProfileInfo>, Boolean>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<String, MobileUserProfileInfo> original) throws Exception {
				MobileUserBehaviorDto behavior = original._2().getBehavior();
				boolean flag = behavior.getType() == BehaviorType.BUSINESS;
				if(flag){
					JSONObject json = JSONObject.fromObject(behavior.getDescript());
					String behaviorName = json.getString("behaviorName");
					flag = "预录入酒店订单".equals(behaviorName);
				}
				return flag;
			}
		}).reduceByKey(new Function2<MobileUserProfileInfo, MobileUserProfileInfo, MobileUserProfileInfo>() {
			private static final long serialVersionUID = 1L;

			@Override
			public MobileUserProfileInfo call(MobileUserProfileInfo userProfileInfo1, MobileUserProfileInfo userProfileInfo2) throws Exception {
				NumberMap result = new NumberMap();
				result.putAll(userProfileInfo1.getBookingDistance());
				result.putAll(userProfileInfo2.getBookingDistance());
				userProfileInfo1.setBookingDistance(result);
				return userProfileInfo1;
			}
		}).filter(new Function<Tuple2<String,MobileUserProfileInfo>, Boolean>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<String, MobileUserProfileInfo> original) throws Exception {
				return original._2().getBookingDistance()!=null;
			}
		}).map(new PairFunction<Tuple2<String, MobileUserProfileInfo>, ImmutableBytesWritable, Put>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<ImmutableBytesWritable, Put> call(Tuple2<String, MobileUserProfileInfo> original) throws Exception {
				NumberMap bookingDistance = original._2().getBookingDistance();
				byte[] userID = Bytes.toBytes(original._1());
				ImmutableBytesWritable rowkey = new ImmutableBytesWritable(userID);
				
				Put put = new Put(userID);
				put.add(FAMILY, BOOKING_DISTANCE, Bytes.toBytes(bookingDistance.getMapString()));
				return new Tuple2<ImmutableBytesWritable, Put>(rowkey, put);
			}
		});

		SparkUtils.logRDDcount("bookingDiscount", result, logSwitch);
		result.saveAsHadoopDataset(outJobConfig);
	}

	private static void saveQueryWordsTag(JavaPairRDD<String, MobileUserProfileInfo> mobileUserProfile, JobConf outJobConfig) {
		JavaPairRDD<ImmutableBytesWritable, Put> result = mobileUserProfile.reduceByKey(new Function2<MobileUserProfileInfo, MobileUserProfileInfo, MobileUserProfileInfo>() {
			private static final long serialVersionUID = 1L;

			@Override
			public MobileUserProfileInfo call(MobileUserProfileInfo userProfileInfo1, MobileUserProfileInfo userProfileInfo2) throws Exception {
				NumberMap result = new NumberMap();
				result.putAll(userProfileInfo1.getQueryWords());
				result.putAll(userProfileInfo2.getQueryWords());
				userProfileInfo1.setQueryWords(result);
				return userProfileInfo1;
			}
		}).map(new PairFunction<Tuple2<String, MobileUserProfileInfo>, ImmutableBytesWritable, Put>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<ImmutableBytesWritable, Put> call(Tuple2<String, MobileUserProfileInfo> original) throws Exception {
				NumberMap queryWords = original._2().getQueryWords();
				byte[] userID = Bytes.toBytes(original._1());
				ImmutableBytesWritable rowkey = new ImmutableBytesWritable(userID);
				
				Put put = new Put(userID);
				put.add(FAMILY, QUERY_WORDS, Bytes.toBytes(queryWords.getMapString()));
				return new Tuple2<ImmutableBytesWritable, Put>(rowkey, put);
			}
		});
		
		SparkUtils.logRDDcount("QueryWords", result, logSwitch);
		result.saveAsHadoopDataset(outJobConfig);
	}

	private static void saveZoneScreenTag(JavaPairRDD<String, MobileUserProfileInfo> mobileUserProfile, JobConf outJobConfig) {
		JavaPairRDD<ImmutableBytesWritable, Put> result = mobileUserProfile.reduceByKey(new Function2<MobileUserProfileInfo, MobileUserProfileInfo, MobileUserProfileInfo>() {
			private static final long serialVersionUID = 1L;

			@Override
			public MobileUserProfileInfo call(MobileUserProfileInfo userProfileInfo1, MobileUserProfileInfo userProfileInfo2) throws Exception {
				NumberMap result = new NumberMap();
				result.putAll(userProfileInfo1.getZoneScreen());
				result.putAll(userProfileInfo2.getZoneScreen());
				userProfileInfo1.setZoneScreen(result);
				return userProfileInfo1;
			}
		}).map(new PairFunction<Tuple2<String, MobileUserProfileInfo>, ImmutableBytesWritable, Put>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<ImmutableBytesWritable, Put> call(Tuple2<String, MobileUserProfileInfo> original) throws Exception {
				NumberMap zoneScreen = original._2().getZoneScreen();
				byte[] userID = Bytes.toBytes(original._1());

				Put put = new Put(userID);
				put.add(FAMILY, ZONE_SCREEN, Bytes.toBytes(zoneScreen.getMapString()));
				return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(userID), put);
			}
		});

		SparkUtils.logRDDcount("ZoneScreen", result, logSwitch);
		result.saveAsHadoopDataset(outJobConfig);
	}

	private static void saveBrandScreenTag(JavaPairRDD<String, MobileUserProfileInfo> mobileUserProfile, JobConf outJobConfig) {
		JavaPairRDD<ImmutableBytesWritable, Put> result = mobileUserProfile.reduceByKey(new Function2<MobileUserProfileInfo, MobileUserProfileInfo, MobileUserProfileInfo>() {
			private static final long serialVersionUID = 1L;

			@Override
			public MobileUserProfileInfo call(MobileUserProfileInfo userProfileInfo1, MobileUserProfileInfo userProfileInfo2) throws Exception {
				NumberMap result = new NumberMap();
				result.putAll(userProfileInfo1.getBrandScreen());
				result.putAll(userProfileInfo2.getBrandScreen());
				userProfileInfo1.setBrandScreen(result);
				return userProfileInfo1;
			}
		}).map(new PairFunction<Tuple2<String, MobileUserProfileInfo>, ImmutableBytesWritable, Put>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<ImmutableBytesWritable, Put> call(Tuple2<String, MobileUserProfileInfo> original) throws Exception {
				NumberMap brandScreen = original._2().getBrandScreen();
				byte[] userID = Bytes.toBytes(original._1());

				Put put = new Put(userID);
				put.add(FAMILY, BRAND_SCREEN, Bytes.toBytes(brandScreen.getMapString()));
				return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(userID), put);
			}
		});

		SparkUtils.logRDDcount("BrandScreen", result, logSwitch);
		result.saveAsHadoopDataset(outJobConfig);
	}

	private static void saveSortTypeTag(JavaPairRDD<String, MobileUserProfileInfo> mobileUserProfile, JobConf outJobConfig) {
		JavaPairRDD<ImmutableBytesWritable, Put> result = mobileUserProfile.reduceByKey(new Function2<MobileUserProfileInfo, MobileUserProfileInfo, MobileUserProfileInfo>() {
			private static final long serialVersionUID = 1L;

			@Override
			public MobileUserProfileInfo call(MobileUserProfileInfo userProfileInfo1, MobileUserProfileInfo userProfileInfo2) throws Exception {
				NumberMap result = new NumberMap();
				result.putAll(userProfileInfo1.getSortType());
				result.putAll(userProfileInfo2.getSortType());
				userProfileInfo1.setSortType(result);
				return userProfileInfo1;
			}
		}).map(new PairFunction<Tuple2<String, MobileUserProfileInfo>, ImmutableBytesWritable, Put>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<ImmutableBytesWritable, Put> call(Tuple2<String, MobileUserProfileInfo> original) throws Exception {
				NumberMap sortType = original._2().getSortType();
				byte[] userID = Bytes.toBytes(original._1());

				Put put = new Put(userID);
				put.add(FAMILY, SORT_TYPE, Bytes.toBytes(sortType.getMapString()));
				return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(userID), put);
			}
		});

		SparkUtils.logRDDcount("SortType", result, logSwitch);
		result.saveAsHadoopDataset(outJobConfig);
	}

	private static void savePreferenceCityTag(JavaPairRDD<String, MobileUserProfileInfo> mobileUserProfile, JobConf outJobConfig) {
		JavaPairRDD<ImmutableBytesWritable, Put> result = mobileUserProfile.reduceByKey(new Function2<MobileUserProfileInfo, MobileUserProfileInfo, MobileUserProfileInfo>() {
			private static final long serialVersionUID = 1L;

			@Override
			public MobileUserProfileInfo call(MobileUserProfileInfo userProfileInfo1, MobileUserProfileInfo userProfileInfo2) throws Exception {
				NumberMap result = new NumberMap();
				result.putAll(userProfileInfo1.getPreferenceCity());
				result.putAll(userProfileInfo2.getPreferenceCity());
				userProfileInfo1.setPreferenceCity(result);
				return userProfileInfo1;
			}
		}).map(new PairFunction<Tuple2<String, MobileUserProfileInfo>, ImmutableBytesWritable, Put>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<ImmutableBytesWritable, Put> call(Tuple2<String, MobileUserProfileInfo> original) throws Exception {
				NumberMap preferenceCity = original._2().getPreferenceCity();
				byte[] userID = Bytes.toBytes(original._1());

				Put put = new Put(userID);
				put.add(FAMILY, PREFERENCE_CITY, Bytes.toBytes(preferenceCity.getMapString()));
				return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(userID), put);
			}
		});

		SparkUtils.logRDDcount("SortType", result, logSwitch);
		result.saveAsHadoopDataset(outJobConfig);
	}

	private static void saveLBSInfoTag(JavaPairRDD<String, MobileUserProfileInfo> mobileUserProfile, JobConf outJobConfig) {
		JavaPairRDD<ImmutableBytesWritable, Put> result = mobileUserProfile.reduceByKey(new Function2<MobileUserProfileInfo, MobileUserProfileInfo, MobileUserProfileInfo>() {
			private static final long serialVersionUID = 1L;

			@Override
			public MobileUserProfileInfo call(MobileUserProfileInfo userProfileInfo1, MobileUserProfileInfo userProfileInfo2) throws Exception {
				NumberMap result = new NumberMap();
				result.putAll(userProfileInfo1.getLBSInfo());
				result.putAll(userProfileInfo2.getLBSInfo());
				userProfileInfo1.setLBSInfo(result);
				return userProfileInfo1;
			}
		}).map(new PairFunction<Tuple2<String, MobileUserProfileInfo>, ImmutableBytesWritable, Put>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<ImmutableBytesWritable, Put> call(Tuple2<String, MobileUserProfileInfo> original) throws Exception {
				NumberMap LBSInfo = original._2().getLBSInfo();
				byte[] userID = Bytes.toBytes(original._1());
				ImmutableBytesWritable rowkey = new ImmutableBytesWritable(userID);
				
				Put put = new Put(userID);
				put.add(FAMILY, LBS_INFO, Bytes.toBytes(LBSInfo.getMapKeyString()));
				return new Tuple2<ImmutableBytesWritable, Put>(rowkey, put);
			}
		});
		
		SparkUtils.logRDDcount("LBSInfo", result, logSwitch);
		result.saveAsHadoopDataset(outJobConfig);
	}

	private static void saveLastOpenTimeTag(JavaPairRDD<String, MobileUserProfileInfo> mobileUserProfile, JobConf outJobConfig) {
		JavaPairRDD<ImmutableBytesWritable, Put> result = mobileUserProfile.reduceByKey(new Function2<MobileUserProfileInfo, MobileUserProfileInfo, MobileUserProfileInfo>() {
			private static final long serialVersionUID = 1L;

			@Override
			public MobileUserProfileInfo call(MobileUserProfileInfo userProfileInfo1, MobileUserProfileInfo userProfileInfo2) throws Exception {
				Date date1 = userProfileInfo1.getLastOpenTime();
				Date date2 = userProfileInfo2.getLastOpenTime();
				return date1.after(date2) ? userProfileInfo1 : userProfileInfo2;
			}
		}).map( new PairFunction<Tuple2<String, MobileUserProfileInfo>, ImmutableBytesWritable, Put>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<ImmutableBytesWritable, Put> call(Tuple2<String, MobileUserProfileInfo> original) throws Exception {
				Date lastOpenTime = original._2().getLastOpenTime();
				byte[] userID = Bytes.toBytes(original._1());
				ImmutableBytesWritable rowkey = new ImmutableBytesWritable(userID);
				
				Put put = new Put(userID);
				long now = System.currentTimeMillis();
				
				long times = lastOpenTime.getTime();
				
				long between_days=(now-times)/(1000*3600*24); 
				
				put.add(FAMILY, LAST_OPEN_TIME, Bytes.toBytes(ThreadSafeDateUtils.dayFormat(lastOpenTime)));
				put.add(FAMILY, LAST_OPEN_TIME_BETWEEN_DAYS, Bytes.toBytes(between_days + ""));
				return new Tuple2<ImmutableBytesWritable, Put>(rowkey, put);
			}
		});
		
		SparkUtils.logRDDcount("LastOpenTime", result, logSwitch);
		result.saveAsHadoopDataset(outJobConfig);
	}

	private static void saveMobileSystemTag(JavaPairRDD<String, MobileUserProfileInfo> mobileUserProfile, JobConf outJobConfig) {
		JavaPairRDD<ImmutableBytesWritable, Put> result = mobileUserProfile.reduceByKey(new Function2<MobileUserProfileInfo, MobileUserProfileInfo, MobileUserProfileInfo>() {
			private static final long serialVersionUID = 1L;

			@Override
			public MobileUserProfileInfo call(MobileUserProfileInfo userProfileInfo1, MobileUserProfileInfo userProfileInfo2) throws Exception {
				NumberMap result = new NumberMap();
				result.putAll(userProfileInfo1.getSystemOS());
				result.putAll(userProfileInfo2.getSystemOS());
				userProfileInfo1.setSystemOS(result);
				return userProfileInfo1;
			}
		}).map(new PairFunction<Tuple2<String, MobileUserProfileInfo>, ImmutableBytesWritable, Put>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<ImmutableBytesWritable, Put> call(Tuple2<String, MobileUserProfileInfo> original) throws Exception {
				NumberMap systemOS = original._2().getSystemOS();
				byte[] userID = Bytes.toBytes(original._1());
				ImmutableBytesWritable rowkey = new ImmutableBytesWritable(userID);
				
				Put put = new Put(userID);
				put.add(FAMILY, SYSTEM_OS, Bytes.toBytes(systemOS.getMapKeyString()));
				return new Tuple2<ImmutableBytesWritable, Put>(rowkey, put);
			}
		});
		
		SparkUtils.logRDDcount("MobileSystemTag", result, logSwitch);
		result.saveAsHadoopDataset(outJobConfig);
	}
	
	private static void saveMobileAdvertTag(JavaPairRDD<String, MobileUserProfileInfo> mobileUserProfile, JobConf outJobConfig) {
		JavaPairRDD<ImmutableBytesWritable, Put> result = mobileUserProfile.reduceByKey(new Function2<MobileUserProfileInfo, MobileUserProfileInfo, MobileUserProfileInfo>() {
			private static final long serialVersionUID = 1L;

			@Override
			public MobileUserProfileInfo call(MobileUserProfileInfo userProfileInfo1, MobileUserProfileInfo userProfileInfo2) throws Exception {
				NumberMap result = new NumberMap();
				result.putAll(userProfileInfo1.getAdvertID());
				result.putAll(userProfileInfo2.getAdvertID());
				userProfileInfo1.setAdvertID(result);
				return userProfileInfo1;
			}
		}).map(new PairFunction<Tuple2<String, MobileUserProfileInfo>, ImmutableBytesWritable, Put>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<ImmutableBytesWritable, Put> call(Tuple2<String, MobileUserProfileInfo> original) throws Exception {
				NumberMap advertID = original._2().getAdvertID();
				byte[] userID = Bytes.toBytes(original._1());
				ImmutableBytesWritable rowkey = new ImmutableBytesWritable(userID);
				
				Put put = new Put(userID);
				put.add(FAMILY, ADVERT_ID, Bytes.toBytes(advertID.getMapKeyString()));
				return new Tuple2<ImmutableBytesWritable, Put>(rowkey, put);
			}
		});
		
		SparkUtils.logRDDcount("MobileAdvertTag", result, logSwitch);
		result.saveAsHadoopDataset(outJobConfig);
	}
}
