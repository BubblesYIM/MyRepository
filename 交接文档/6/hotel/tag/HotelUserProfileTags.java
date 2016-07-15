package com.jje.bigdata.userProfile.hotel.tag;

import java.io.IOException;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;

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

import scala.Tuple2;

import com.jje.bigdata.census.ThreadSafeDateUtils;
import com.jje.bigdata.userProfile.hotel.domain.HotelOrderInfo;
import com.jje.bigdata.userProfile.hotel.domain.HotelUserProfileInfo;
import com.jje.bigdata.userProfile.travel.domain.BigdataMemberInfo;
import com.jje.bigdata.userProfile.travel.domain.NumberMap;
import com.jje.bigdata.util.BigdataMemberInfoUtils;
import com.jje.bigdata.util.SparkUtils;
import com.jje.bigdata.util.WritableTableOutputFormat;

public class HotelUserProfileTags {

	private static JavaSparkContext sc = null;
	private static Configuration inConf = null;

	private static boolean debugFlag = false;
	private static boolean testFlag = false;
	
	private static byte[] FAMILY = "info".getBytes();
	private static byte[] LAST_CHECKIN_TIME = "LAST_CHECKIN_TIME".getBytes();
	private static byte[] BE_TWEEN_WITH_LAST_CHECKIN_DAYS = "BE_TWEEN_WITH_LAST_CHECKIN_DAYS".getBytes();
	private static byte[] HOTEl_STARS = "HOTEl_STARS".getBytes();
	private static byte[] HOTEL_BRANDS = "HOTEL_BRANDS".getBytes();
	private static byte[] HOTEL_NAMES = "HOTEL_NAMES".getBytes();
	private static byte[] HOTEL_CITYS = "HOTEL_CITYS".getBytes();
	private static byte[] MONTH_AND_HOTEL_CITYS = "MONTH_AND_HOTEL_CITYS".getBytes();
	private static byte[] CHECK_IN_MONTH = "CHECK_IN_MONTH".getBytes();
	private static byte[] HOLIDAYS = "HOLIDAYS".getBytes();
	private static byte[] CHECKIN_MONTH_AND_HOTEL = "CHECKIN_MONTH_AND_HOTEL".getBytes();
	private static byte[] USER_REQUIRE = "USER_REQUIRE".getBytes();
	private static byte[] AVG_CONSUMPTION = "AVG_CONSUMPTION".getBytes();
	private static byte[] LAST_CONSUMPTION = "LAST_CONSUMPTION".getBytes();
	private static byte[] BOOKING_CHANNEL = "BOOKING_CHANNEL".getBytes();
	private static byte[] BOOKING_COUNT = "BOOKING_COUNT".getBytes();
	private static byte[] CHECKIN_COUNT = "CHECKIN_COUNT".getBytes();
	private static byte[] LAST_SIX_MONTH_BOOKING_COUNT = "LAST_SIX_MONTH_BOOKING_COUNT".getBytes();
	private static byte[] LAST_SIX_MONTH_CHECKIN_COUNT = "LAST_SIX_MONTH_CHECKIN_COUNT".getBytes();
	private static byte[] RATE_TYPE = "RATE_TYPE".getBytes();
	private static byte[] AVG_CHECKIN_DAYS = "AVG_CHECKIN_DAYS".getBytes();
	private static byte[] PAY_RESOURCE = "PAY_RESOURCE".getBytes();

	static {
		try {
			inConf = HBaseConfiguration.create();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws Exception {

		System.out.println("============================HotelUserProfileTags Main开始" + new Date() + "=============================");
		if (args.length < 2) {
			System.out.println("/usr/lib/ngmr/run_app lexus-1.0-SNAPSHOT.jar com.jje.bigdata.userProfile.hotel.tag.HotelUserProfileTags ngmr-yarn-client ./lexus-1.0-SNAPSHOT.jar true true");
			System.exit(1);
		}
		SparkConf sparkConf = new SparkConf();
		sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		sparkConf.set("spark.kryo.registrator", "com.jje.bigdata.util.MyKryoRegistrator");
		sc = new JavaSparkContext(args[0], "HotelUserProfileTags", sparkConf);

		sc.addJar(args[1]);

		System.out.println("#############args:" + Arrays.asList(args));

		if (args.length > 3) {
			debugFlag = Boolean.parseBoolean(args[2]);
			System.out.println("###############debugFlag = " + debugFlag);
		}

		if (args.length > 4) {
			testFlag = Boolean.parseBoolean(args[3]);
			System.out.println("###############testFlag = " + testFlag);
		}

		run();

		System.out.println("============================HotelUserProfileTags Main结束" + new Date() + "=============================");
	}

	private static void run() throws IOException {
		Configuration outConfig = HBaseConfiguration.create();
		outConfig.set(TableOutputFormat.OUTPUT_TABLE, "TB_HOTEL_USER_PROFILE");
		JobConf outJobConfig = new JobConf(outConfig, HotelUserProfileTags.class);
		outJobConfig.setOutputFormat(WritableTableOutputFormat.class);
		
		JavaPairRDD<HotelOrderInfo, BigdataMemberInfo> bigdataHotelMemberInfoRDD = BigdataMemberInfoUtils.getBigdataHotelMemberInfo(sc, inConf, 0);
		SparkUtils.logRDDcount("bigdataHotelMemberInfoRDD count:", bigdataHotelMemberInfoRDD, debugFlag);
		JavaPairRDD<String, HotelUserProfileInfo> userProfileInfoRDD = getUserProfileInfo(bigdataHotelMemberInfoRDD);

		saveHotelStarsTag(userProfileInfoRDD, outJobConfig);
		saveHotelBrandsTag(userProfileInfoRDD, outJobConfig);
		saveHotelNamesTag(userProfileInfoRDD, outJobConfig);
		saveHotelCitysTag(userProfileInfoRDD, outJobConfig);
		saveLastCheckInTag(userProfileInfoRDD, outJobConfig);
		saveCheckInMonthTag(userProfileInfoRDD, outJobConfig);
		saveMothAndHotelCitysTag(userProfileInfoRDD, outJobConfig);
		saveCheckInMonthAndHotelNameTag(userProfileInfoRDD, outJobConfig);
		saveHolidaysTag(userProfileInfoRDD, outJobConfig);
		saveUserRequire(userProfileInfoRDD, outJobConfig);
		saveAVGComsumptionTag(userProfileInfoRDD, outJobConfig);
		saveBookingChannelTag(userProfileInfoRDD, outJobConfig);
		saveBookingCountTag(userProfileInfoRDD, outJobConfig);
//		saveCheckInCountTag(userProfileInfoRDD, outJobConfig);
		saveLastSixMonthBookingCountTag(userProfileInfoRDD, outJobConfig);
		saveLastSixMonthCheckInCountTag(userProfileInfoRDD, outJobConfig);
		saveRateTypeTag(userProfileInfoRDD, outJobConfig);
		savePayResourceTag(userProfileInfoRDD, outJobConfig);
	}

	private static void savePayResourceTag(JavaPairRDD<String, HotelUserProfileInfo> userProfileInfoRDD, JobConf outJobConfig) {
		JavaPairRDD<ImmutableBytesWritable, Put> result = userProfileInfoRDD.reduceByKey(new Function2<HotelUserProfileInfo, HotelUserProfileInfo, HotelUserProfileInfo>() {
			private static final long serialVersionUID = 1L;
			
			@Override
			public HotelUserProfileInfo call(HotelUserProfileInfo arg0, HotelUserProfileInfo arg1) throws Exception {
				NumberMap result = new NumberMap();
				result.putAll(arg0.getPayResource());
				result.putAll(arg1.getPayResource());
				arg0.setPayResource(result);
				return arg0;
			}
		}).map(new PairFunction<Tuple2<String, HotelUserProfileInfo>, ImmutableBytesWritable, Put>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<ImmutableBytesWritable, Put> call(Tuple2<String, HotelUserProfileInfo> original) throws Exception {
				byte[] bytes = Bytes.toBytes(original._1());
				NumberMap payResource = original._2().getPayResource();
				
				ImmutableBytesWritable rowkey = new ImmutableBytesWritable(bytes);
				Put put = new Put(bytes);
				put.add(FAMILY, PAY_RESOURCE, Bytes.toBytes(payResource.getMapString()));
				return new Tuple2<ImmutableBytesWritable, Put>(rowkey, put);
			}
		});

		SparkUtils.logRDDcount("Pay Resource", result, debugFlag);
		result.saveAsHadoopDataset(outJobConfig);
	}

	private static void saveRateTypeTag(JavaPairRDD<String, HotelUserProfileInfo> userProfileInfoRDD, JobConf outJobConfig) {
		JavaPairRDD<ImmutableBytesWritable, Put> result = userProfileInfoRDD.reduceByKey(new Function2<HotelUserProfileInfo, HotelUserProfileInfo, HotelUserProfileInfo>() {
			private static final long serialVersionUID = 1L;
			
			@Override
			public HotelUserProfileInfo call(HotelUserProfileInfo arg0, HotelUserProfileInfo arg1) throws Exception {
				NumberMap result = new NumberMap();
				result.putAll(arg0.getRateType());
				result.putAll(arg1.getRateType());
				arg0.setRateType(result);
				return arg0;
			}
		}).map(new PairFunction<Tuple2<String, HotelUserProfileInfo>, ImmutableBytesWritable, Put>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<ImmutableBytesWritable, Put> call(Tuple2<String, HotelUserProfileInfo> original) throws Exception {
				byte[] bytes = Bytes.toBytes(original._1());
				NumberMap rateType = original._2().getRateType();
				
				ImmutableBytesWritable rowkey = new ImmutableBytesWritable(bytes);
				Put put = new Put(bytes);
				put.add(FAMILY, RATE_TYPE, Bytes.toBytes(rateType.getMapString()));
				return new Tuple2<ImmutableBytesWritable, Put>(rowkey, put);
			}
		});

		SparkUtils.logRDDcount("Rate Type", result, debugFlag);
		result.saveAsHadoopDataset(outJobConfig);
	}

	private static void saveLastSixMonthCheckInCountTag(JavaPairRDD<String, HotelUserProfileInfo> userProfileInfoRDD, JobConf outJobConfig) {
		JavaPairRDD<ImmutableBytesWritable, Put> result = userProfileInfoRDD.filter(new Function<Tuple2<String,HotelUserProfileInfo>, Boolean>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<String, HotelUserProfileInfo> original) throws Exception {
				Date checkInTime = original._2().getLastCheckInTime();
				if(checkInTime == null){
					return false;
				}
				double daySpace = ThreadSafeDateUtils.getDaySpace(checkInTime, new Date());
				if(daySpace < 180){
					return true;
				}
				return false;
			}
		}).reduceByKey(new Function2<HotelUserProfileInfo, HotelUserProfileInfo, HotelUserProfileInfo>() {
			private static final long serialVersionUID = 1L;
			
			@Override
			public HotelUserProfileInfo call(HotelUserProfileInfo arg0, HotelUserProfileInfo arg1) throws Exception {
				int checkInCount1 = arg0.getCheckInCount();
				int checkInCount2 = arg1.getCheckInCount();
				arg0.setCheckInCount(checkInCount1 + checkInCount2);
				return arg0;
			}
		}).map(new PairFunction<Tuple2<String, HotelUserProfileInfo>, ImmutableBytesWritable, Put>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<ImmutableBytesWritable, Put> call(Tuple2<String, HotelUserProfileInfo> original) throws Exception {
				byte[] bytes = Bytes.toBytes(original._1());
				int checkInCount = original._2().getCheckInCount();
				
				ImmutableBytesWritable rowkey = new ImmutableBytesWritable(bytes);
				Put put = new Put(bytes);
				put.add(FAMILY, LAST_SIX_MONTH_CHECKIN_COUNT, Bytes.toBytes(checkInCount + ""));
				return new Tuple2<ImmutableBytesWritable, Put>(rowkey, put);
			}
		});

		SparkUtils.logRDDcount("Last Six Month CheckIn Count", result, debugFlag);
		result.saveAsHadoopDataset(outJobConfig);
	}

	private static void saveLastSixMonthBookingCountTag(JavaPairRDD<String, HotelUserProfileInfo> userProfileInfoRDD, JobConf outJobConfig) {
		JavaPairRDD<ImmutableBytesWritable, Put> result = userProfileInfoRDD.filter(new Function<Tuple2<String,HotelUserProfileInfo>, Boolean>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<String, HotelUserProfileInfo> original) throws Exception {
				Date bookingTime = original._2().getBookingTime();
				if(bookingTime==null){
					return false;
				}
				double daySpace = ThreadSafeDateUtils.getDaySpace(bookingTime, new Date());
				if(daySpace < 180){
					return true;
				}
				return false;
			}
		}).reduceByKey(new Function2<HotelUserProfileInfo, HotelUserProfileInfo, HotelUserProfileInfo>() {
			private static final long serialVersionUID = 1L;
			
			@Override
			public HotelUserProfileInfo call(HotelUserProfileInfo arg0, HotelUserProfileInfo arg1) throws Exception {
				int bookingCount1 = arg0.getBookingCount();
				int bookingCount2 = arg1.getBookingCount();
				arg0.setBookingCount(bookingCount1 + bookingCount2);
				return arg0;
			}
		}).map(new PairFunction<Tuple2<String, HotelUserProfileInfo>, ImmutableBytesWritable, Put>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<ImmutableBytesWritable, Put> call(Tuple2<String, HotelUserProfileInfo> original) throws Exception {
				byte[] bytes = Bytes.toBytes(original._1());
				int bookingCount = original._2().getBookingCount();
				
				ImmutableBytesWritable rowkey = new ImmutableBytesWritable(bytes);
				Put put = new Put(bytes);
				put.add(FAMILY, LAST_SIX_MONTH_BOOKING_COUNT, Bytes.toBytes(bookingCount + ""));
				return new Tuple2<ImmutableBytesWritable, Put>(rowkey, put);
			}
		});

		SparkUtils.logRDDcount("Last Six Month Booking Count", result, debugFlag);
		result.saveAsHadoopDataset(outJobConfig);
	}

	private static void saveCheckInCountTag(JavaPairRDD<String, HotelUserProfileInfo> userProfileInfoRDD, JobConf outJobConfig) {
		JavaPairRDD<ImmutableBytesWritable, Put> result = userProfileInfoRDD.reduceByKey(new Function2<HotelUserProfileInfo, HotelUserProfileInfo, HotelUserProfileInfo>() {
			private static final long serialVersionUID = 1L;
			
			@Override
			public HotelUserProfileInfo call(HotelUserProfileInfo arg0, HotelUserProfileInfo arg1) throws Exception {
				int checkInCount1 = arg0.getCheckInCount();
				int checkInCount2 = arg1.getCheckInCount();
				arg0.setCheckInCount(checkInCount1 + checkInCount2);
				return arg0;
			}
		}).map(new PairFunction<Tuple2<String, HotelUserProfileInfo>, ImmutableBytesWritable, Put>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<ImmutableBytesWritable, Put> call(Tuple2<String, HotelUserProfileInfo> original) throws Exception {
				byte[] bytes = Bytes.toBytes(original._1());
//				NumberMap bookingChannel = original._2().getBookingChannel();
				int checkInCount = original._2().getCheckInCount();
				
				ImmutableBytesWritable rowkey = new ImmutableBytesWritable(bytes);
				Put put = new Put(bytes);
				put.add(FAMILY, CHECKIN_COUNT, Bytes.toBytes(checkInCount + ""));
				return new Tuple2<ImmutableBytesWritable, Put>(rowkey, put);
			}
		});

		SparkUtils.logRDDcount("checkIn Count", result, debugFlag);
		result.saveAsHadoopDataset(outJobConfig);
	}

	private static void saveBookingCountTag(JavaPairRDD<String, HotelUserProfileInfo> userProfileInfoRDD, JobConf outJobConfig) {
		JavaPairRDD<ImmutableBytesWritable, Put> result = userProfileInfoRDD.reduceByKey(new Function2<HotelUserProfileInfo, HotelUserProfileInfo, HotelUserProfileInfo>() {
			private static final long serialVersionUID = 1L;
			
			@Override
			public HotelUserProfileInfo call(HotelUserProfileInfo arg0, HotelUserProfileInfo arg1) throws Exception {
				int bookingCount1 = arg0.getBookingCount();
				int bookingCount2 = arg1.getBookingCount();
				arg0.setBookingCount(bookingCount1 + bookingCount2);
				return arg0;
			}
		}).map(new PairFunction<Tuple2<String, HotelUserProfileInfo>, ImmutableBytesWritable, Put>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<ImmutableBytesWritable, Put> call(Tuple2<String, HotelUserProfileInfo> original) throws Exception {
				byte[] bytes = Bytes.toBytes(original._1());
//				NumberMap bookingChannel = original._2().getBookingChannel();
				int bookingCount = original._2().getBookingCount();
				
				ImmutableBytesWritable rowkey = new ImmutableBytesWritable(bytes);
				Put put = new Put(bytes);
				put.add(FAMILY, BOOKING_COUNT, Bytes.toBytes(bookingCount + ""));
				return new Tuple2<ImmutableBytesWritable, Put>(rowkey, put);
			}
		});

		SparkUtils.logRDDcount("Booking Count", result, debugFlag);
		result.saveAsHadoopDataset(outJobConfig);
	}

	private static void saveBookingChannelTag(JavaPairRDD<String, HotelUserProfileInfo> userProfileInfoRDD, JobConf outJobConfig) {
		JavaPairRDD<ImmutableBytesWritable, Put> result = userProfileInfoRDD.reduceByKey(new Function2<HotelUserProfileInfo, HotelUserProfileInfo, HotelUserProfileInfo>() {
			private static final long serialVersionUID = 1L;
			
			@Override
			public HotelUserProfileInfo call(HotelUserProfileInfo arg0, HotelUserProfileInfo arg1) throws Exception {
				NumberMap result = new NumberMap();
				result.putAll(arg0.getBookingChannel());
				result.putAll(arg1.getBookingChannel());
				arg0.setBookingChannel(result);
				return arg0;
			}
		}).map(new PairFunction<Tuple2<String, HotelUserProfileInfo>, ImmutableBytesWritable, Put>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<ImmutableBytesWritable, Put> call(Tuple2<String, HotelUserProfileInfo> original) throws Exception {
				byte[] bytes = Bytes.toBytes(original._1());
				NumberMap bookingChannel = original._2().getBookingChannel();
				
				ImmutableBytesWritable rowkey = new ImmutableBytesWritable(bytes);
				Put put = new Put(bytes);
				put.add(FAMILY, BOOKING_CHANNEL, Bytes.toBytes(bookingChannel.getMapString()));
				return new Tuple2<ImmutableBytesWritable, Put>(rowkey, put);
			}
		});

		SparkUtils.logRDDcount("Booking Channel", result, debugFlag);
		result.saveAsHadoopDataset(outJobConfig);
	}

	private static void saveAVGComsumptionTag(JavaPairRDD<String, HotelUserProfileInfo> userProfileInfoRDD, JobConf outJobConfig) {
		JavaPairRDD<ImmutableBytesWritable, Put> result = userProfileInfoRDD.reduceByKey(new Function2<HotelUserProfileInfo, HotelUserProfileInfo, HotelUserProfileInfo>() {
			private static final long serialVersionUID = 1L;
			
			@Override
			public HotelUserProfileInfo call(HotelUserProfileInfo arg0, HotelUserProfileInfo arg1) throws Exception {
				int checkInCount1 = arg0.getCheckInCount();
				int checkInCount2 = arg1.getCheckInCount();
				
				arg0.setCheckInCount(checkInCount1 + checkInCount2);

				Double sumConsumption1 = arg0.getSumConsumption();
				Double sumConsumption2 = arg1.getSumConsumption();
				arg0.setSumConsumption(sumConsumption1 + sumConsumption2);

				Double checkInDays1 = arg0.getCheckInDays();
				Double checkInDays2 = arg1.getCheckInDays();
				arg0.setCheckInDays(checkInDays1 + checkInDays2);
			
				return arg0;
			}
		}).filter(new Function<Tuple2<String,HotelUserProfileInfo>, Boolean>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<String, HotelUserProfileInfo> original) throws Exception {
				return original._2().getCheckInCount() > 0;
			}
		}).map(new PairFunction<Tuple2<String, HotelUserProfileInfo>, ImmutableBytesWritable, Put>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<ImmutableBytesWritable, Put> call(Tuple2<String, HotelUserProfileInfo> original) throws Exception {
				byte[] bytes = Bytes.toBytes(original._1());
				
				Double sumConsumption = original._2().getSumConsumption();
				int checkInCount = original._2().getCheckInCount();
				double avgConsumption = Math.floor(sumConsumption/checkInCount*100)/100;
				
				Double checkInDays = original._2().getCheckInDays();
				double avgCheckInDays = Math.floor(checkInDays/checkInCount*100)/100;
				
				ImmutableBytesWritable rowkey = new ImmutableBytesWritable(bytes);
				Put put = new Put(bytes);
				put.add(FAMILY, AVG_CONSUMPTION, Bytes.toBytes(avgConsumption + ""));
				put.add(FAMILY, CHECKIN_COUNT, Bytes.toBytes(checkInCount + ""));
				put.add(FAMILY, AVG_CHECKIN_DAYS, Bytes.toBytes(avgCheckInDays + ""));
				
				return new Tuple2<ImmutableBytesWritable, Put>(rowkey, put);
			}
		});

		SparkUtils.logRDDcount("AVG Comsumption", result, debugFlag);
		result.saveAsHadoopDataset(outJobConfig);
	}

	private static void saveUserRequire(JavaPairRDD<String, HotelUserProfileInfo> userProfileInfoRDD, JobConf outJobConfig) {
		JavaPairRDD<ImmutableBytesWritable, Put> result = userProfileInfoRDD.reduceByKey(new Function2<HotelUserProfileInfo, HotelUserProfileInfo, HotelUserProfileInfo>() {
			private static final long serialVersionUID = 1L;
			
			@Override
			public HotelUserProfileInfo call(HotelUserProfileInfo arg0, HotelUserProfileInfo arg1) throws Exception {
				NumberMap result = new NumberMap();
				result.putAll(arg0.getUserRequire());
				result.putAll(arg1.getUserRequire());
				arg0.setCheckInMonthAndHotel(result);
				return arg0;
			}
		}).map(new PairFunction<Tuple2<String, HotelUserProfileInfo>, ImmutableBytesWritable, Put>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<ImmutableBytesWritable, Put> call(Tuple2<String, HotelUserProfileInfo> original) throws Exception {
				byte[] bytes = Bytes.toBytes(original._1());
				NumberMap userRequire = original._2().getUserRequire();
				
				ImmutableBytesWritable rowkey = new ImmutableBytesWritable(bytes);
				Put put = new Put(bytes);
				put.add(FAMILY, USER_REQUIRE, Bytes.toBytes(userRequire.getMapString()));
				return new Tuple2<ImmutableBytesWritable, Put>(rowkey, put);
			}
		});

		SparkUtils.logRDDcount("User Require", result, debugFlag);
		result.saveAsHadoopDataset(outJobConfig);
	}

	private static void saveCheckInMonthAndHotelNameTag(JavaPairRDD<String, HotelUserProfileInfo> userProfileInfoRDD, JobConf outJobConfig) {
		JavaPairRDD<ImmutableBytesWritable, Put> result = userProfileInfoRDD.reduceByKey(new Function2<HotelUserProfileInfo, HotelUserProfileInfo, HotelUserProfileInfo>() {
			private static final long serialVersionUID = 1L;
			
			@Override
			public HotelUserProfileInfo call(HotelUserProfileInfo arg0, HotelUserProfileInfo arg1) throws Exception {
				NumberMap result = new NumberMap();
				result.putAll(arg0.getCheckInMonthAndHotel());
				result.putAll(arg1.getCheckInMonthAndHotel());
				arg0.setCheckInMonthAndHotel(result);
				return arg0;
			}
		}).map(new PairFunction<Tuple2<String, HotelUserProfileInfo>, ImmutableBytesWritable, Put>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<ImmutableBytesWritable, Put> call(Tuple2<String, HotelUserProfileInfo> original) throws Exception {
				byte[] bytes = Bytes.toBytes(original._1());
				NumberMap checkInMonthAndHotel = original._2().getCheckInMonthAndHotel();
				
				ImmutableBytesWritable rowkey = new ImmutableBytesWritable(bytes);
				Put put = new Put(bytes);
				put.add(FAMILY, CHECKIN_MONTH_AND_HOTEL, Bytes.toBytes(checkInMonthAndHotel.getMapString()));
				return new Tuple2<ImmutableBytesWritable, Put>(rowkey, put);
			}
		});

		SparkUtils.logRDDcount("Check In Month And Hotel", result, debugFlag);
		result.saveAsHadoopDataset(outJobConfig);
	}

	private static void saveCheckInMonthTag(JavaPairRDD<String, HotelUserProfileInfo> userProfileInfoRDD, JobConf outJobConfig) {
		JavaPairRDD<ImmutableBytesWritable, Put> result = userProfileInfoRDD.reduceByKey(new Function2<HotelUserProfileInfo, HotelUserProfileInfo, HotelUserProfileInfo>() {
			private static final long serialVersionUID = 1L;
			
			@Override
			public HotelUserProfileInfo call(HotelUserProfileInfo arg0, HotelUserProfileInfo arg1) throws Exception {
				NumberMap result = new NumberMap();
				result.putAll(arg0.getCheckInMonth());
				result.putAll(arg1.getCheckInMonth());
				arg0.setCheckInMonth(result);
				return arg0;
			}
		}).map(new PairFunction<Tuple2<String, HotelUserProfileInfo>, ImmutableBytesWritable, Put>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<ImmutableBytesWritable, Put> call(Tuple2<String, HotelUserProfileInfo> original) throws Exception {
				byte[] bytes = Bytes.toBytes(original._1());
				NumberMap checkInMonth = original._2().getCheckInMonth();
				
				ImmutableBytesWritable rowkey = new ImmutableBytesWritable(bytes);
				Put put = new Put(bytes);
				put.add(FAMILY, CHECK_IN_MONTH, Bytes.toBytes(checkInMonth.getMapString()));
				return new Tuple2<ImmutableBytesWritable, Put>(rowkey, put);
			}
		});

		SparkUtils.logRDDcount("Check In Month", result, debugFlag);
		result.saveAsHadoopDataset(outJobConfig);
	}

	private static void saveHolidaysTag(JavaPairRDD<String, HotelUserProfileInfo> userProfileInfoRDD, JobConf outJobConfig) {
		JavaPairRDD<ImmutableBytesWritable, Put> result = userProfileInfoRDD.reduceByKey(new Function2<HotelUserProfileInfo, HotelUserProfileInfo, HotelUserProfileInfo>() {
			private static final long serialVersionUID = 1L;

			@Override
			public HotelUserProfileInfo call(HotelUserProfileInfo arg0, HotelUserProfileInfo arg1) throws Exception {
				NumberMap result = new NumberMap();
				result.putAll(arg0.getHolidays());
				result.putAll(arg1.getHolidays());
				arg0.setHolidays(result);
				return arg0;
			}
		}).map(new PairFunction<Tuple2<String, HotelUserProfileInfo>, ImmutableBytesWritable, Put>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<ImmutableBytesWritable, Put> call(Tuple2<String, HotelUserProfileInfo> original) throws Exception {
				byte[] bytes = Bytes.toBytes(original._1());
				NumberMap holidays = original._2().getHolidays();
				
				ImmutableBytesWritable rowkey = new ImmutableBytesWritable(bytes);
				Put put = new Put(bytes);
				put.add(FAMILY, HOLIDAYS, Bytes.toBytes(holidays.getMapString()));
				return new Tuple2<ImmutableBytesWritable, Put>(rowkey, put);
			}
		});

		SparkUtils.logRDDcount("Holidays", result, debugFlag);
		result.saveAsHadoopDataset(outJobConfig);
	}

	private static void saveMothAndHotelCitysTag(JavaPairRDD<String, HotelUserProfileInfo> userProfileInfoRDD, JobConf outJobConfig) {
		JavaPairRDD<ImmutableBytesWritable, Put> result = userProfileInfoRDD.reduceByKey(new Function2<HotelUserProfileInfo, HotelUserProfileInfo, HotelUserProfileInfo>() {
			private static final long serialVersionUID = 1L;

			@Override
			public HotelUserProfileInfo call(HotelUserProfileInfo arg0, HotelUserProfileInfo arg1) throws Exception {
				NumberMap result = new NumberMap();
				result.putAll(arg0.getMonthAndHotelCity());
				result.putAll(arg1.getMonthAndHotelCity());
				arg0.setMonthAndHotelCity(result);
				return arg0;
			}
		}).map(new PairFunction<Tuple2<String, HotelUserProfileInfo>, ImmutableBytesWritable, Put>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<ImmutableBytesWritable, Put> call(Tuple2<String, HotelUserProfileInfo> original) throws Exception {
				byte[] bytes = Bytes.toBytes(original._1());
				NumberMap monthAndHotelCity = original._2().getMonthAndHotelCity();
				
				ImmutableBytesWritable rowkey = new ImmutableBytesWritable(bytes);
				Put put = new Put(bytes);
				put.add(FAMILY, MONTH_AND_HOTEL_CITYS, Bytes.toBytes(monthAndHotelCity.getMapString()));
				return new Tuple2<ImmutableBytesWritable, Put>(rowkey, put);
			}
		});

		SparkUtils.logRDDcount("Moth And Hotel Citys", result, debugFlag);
		result.saveAsHadoopDataset(outJobConfig);
	}

	private static void saveHotelCitysTag(JavaPairRDD<String, HotelUserProfileInfo> userProfileInfoRDD, JobConf outJobConfig) {
		JavaPairRDD<ImmutableBytesWritable, Put> result = userProfileInfoRDD.reduceByKey(new Function2<HotelUserProfileInfo, HotelUserProfileInfo, HotelUserProfileInfo>() {
			private static final long serialVersionUID = 1L;

			@Override
			public HotelUserProfileInfo call(HotelUserProfileInfo arg0, HotelUserProfileInfo arg1) throws Exception {
				NumberMap result = new NumberMap();
				result.putAll(arg0.getHotelCity());
				result.putAll(arg1.getHotelCity());
				arg0.setHotelCity(result);
				return arg0;
			}
		}).map(new PairFunction<Tuple2<String, HotelUserProfileInfo>, ImmutableBytesWritable, Put>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<ImmutableBytesWritable, Put> call(Tuple2<String, HotelUserProfileInfo> original) throws Exception {
				byte[] bytes = Bytes.toBytes(original._1());
				NumberMap hoteCity = original._2().getHotelCity();
				
				ImmutableBytesWritable rowkey = new ImmutableBytesWritable(bytes);
				Put put = new Put(bytes);
				put.add(FAMILY, HOTEL_CITYS, Bytes.toBytes(hoteCity.getMapString()));
				return new Tuple2<ImmutableBytesWritable, Put>(rowkey, put);
			}
		});

		SparkUtils.logRDDcount("Hotel Citys", result, debugFlag);
		result.saveAsHadoopDataset(outJobConfig);
	}
	
	private static void saveHotelNamesTag(JavaPairRDD<String, HotelUserProfileInfo> userProfileInfoRDD, JobConf outJobConfig) {
		JavaPairRDD<ImmutableBytesWritable, Put> result = userProfileInfoRDD.reduceByKey(new Function2<HotelUserProfileInfo, HotelUserProfileInfo, HotelUserProfileInfo>() {
			private static final long serialVersionUID = 1L;

			@Override
			public HotelUserProfileInfo call(HotelUserProfileInfo arg0, HotelUserProfileInfo arg1) throws Exception {
				NumberMap result = new NumberMap();
				result.putAll(arg0.getHotelName());
				result.putAll(arg1.getHotelName());
				arg0.setHotelName(result);
				return arg0;
			}
		}).map(new PairFunction<Tuple2<String, HotelUserProfileInfo>, ImmutableBytesWritable, Put>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<ImmutableBytesWritable, Put> call(Tuple2<String, HotelUserProfileInfo> original) throws Exception {
				byte[] bytes = Bytes.toBytes(original._1());
				NumberMap hotelName = original._2().getHotelName();
				
				ImmutableBytesWritable rowkey = new ImmutableBytesWritable(bytes);
				Put put = new Put(bytes);
				put.add(FAMILY, HOTEL_NAMES, Bytes.toBytes(hotelName.getMapString()));
				return new Tuple2<ImmutableBytesWritable, Put>(rowkey, put);
			}
		});

		SparkUtils.logRDDcount("Hotel Names", result, debugFlag);
		result.saveAsHadoopDataset(outJobConfig);
	}

	private static void saveHotelBrandsTag(JavaPairRDD<String, HotelUserProfileInfo> userProfileInfoRDD, JobConf outJobConfig) {
		JavaPairRDD<ImmutableBytesWritable, Put> result = userProfileInfoRDD.reduceByKey(new Function2<HotelUserProfileInfo, HotelUserProfileInfo, HotelUserProfileInfo>() {
			private static final long serialVersionUID = 1L;

			@Override
			public HotelUserProfileInfo call(HotelUserProfileInfo arg0, HotelUserProfileInfo arg1) throws Exception {
				NumberMap result = new NumberMap();
				result.putAll(arg0.getHotelBrand());
				result.putAll(arg1.getHotelBrand());
				arg0.setHotelBrand(result);
				return arg0;
			}
		}).map(new PairFunction<Tuple2<String, HotelUserProfileInfo>, ImmutableBytesWritable, Put>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<ImmutableBytesWritable, Put> call(Tuple2<String, HotelUserProfileInfo> original) throws Exception {
				byte[] bytes = Bytes.toBytes(original._1());
				NumberMap hotelBrand = original._2().getHotelBrand();
				
				ImmutableBytesWritable rowkey = new ImmutableBytesWritable(bytes);
				Put put = new Put(bytes);
				put.add(FAMILY, HOTEL_BRANDS, Bytes.toBytes(hotelBrand.getMapString()));
				return new Tuple2<ImmutableBytesWritable, Put>(rowkey, put);
			}
		});

		SparkUtils.logRDDcount("Hotel Brands", result, debugFlag);
		result.saveAsHadoopDataset(outJobConfig);
	}

	private static void saveHotelStarsTag(JavaPairRDD<String, HotelUserProfileInfo> userProfileInfoRDD, JobConf outJobConfig) {
		JavaPairRDD<ImmutableBytesWritable, Put> result = userProfileInfoRDD.reduceByKey(new Function2<HotelUserProfileInfo, HotelUserProfileInfo, HotelUserProfileInfo>() {
			private static final long serialVersionUID = 1L;

			@Override
			public HotelUserProfileInfo call(HotelUserProfileInfo arg0, HotelUserProfileInfo arg1) throws Exception {
				NumberMap result = new NumberMap();
				result.putAll(arg0.getHotelStar());
				result.putAll(arg1.getHotelStar());
				arg0.setHotelStar(result);
				return arg0;
			}
		}).map(new PairFunction<Tuple2<String, HotelUserProfileInfo>, ImmutableBytesWritable, Put>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<ImmutableBytesWritable, Put> call(Tuple2<String, HotelUserProfileInfo> original) throws Exception {
				byte[] bytes = Bytes.toBytes(original._1());
				NumberMap hotelStars = original._2().getHotelStar();
				
				ImmutableBytesWritable rowkey = new ImmutableBytesWritable(bytes);
				Put put = new Put(bytes);
				put.add(FAMILY, HOTEl_STARS, Bytes.toBytes(hotelStars.getMapString()));
				return new Tuple2<ImmutableBytesWritable, Put>(rowkey, put);
			}
		});

		SparkUtils.logRDDcount("Hotel Stars", result, debugFlag);
		result.saveAsHadoopDataset(outJobConfig);
	}

	private static void saveLastCheckInTag(JavaPairRDD<String, HotelUserProfileInfo> userProfileInfoRDD, JobConf outJobConfig) {
		JavaPairRDD<ImmutableBytesWritable, Put> result = userProfileInfoRDD.reduceByKey(new Function2<HotelUserProfileInfo, HotelUserProfileInfo, HotelUserProfileInfo>() {
			private static final long serialVersionUID = 1L;

			@Override
			public HotelUserProfileInfo call(HotelUserProfileInfo arg0, HotelUserProfileInfo arg1) throws Exception {
				Date lastCheckInTime1 = arg0.getLastCheckInTime();
				Date lastCheckInTime2 = arg1.getLastCheckInTime();
				
				if (lastCheckInTime1 == null && lastCheckInTime2 == null) {
					return arg0;
				} else if (lastCheckInTime1 != null && lastCheckInTime2 == null) {
					return arg0;
				} else if (lastCheckInTime1 == null && lastCheckInTime2 != null) {
					return arg1;
				} else {
					return lastCheckInTime1.after(lastCheckInTime2) ? arg0 : arg1;
				}
			}
		}).filter(new Function<Tuple2<String,HotelUserProfileInfo>, Boolean>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<String, HotelUserProfileInfo> original) throws Exception {
				return original._2().getLastCheckInTime()!=null;
			}
		}).map(new PairFunction<Tuple2<String, HotelUserProfileInfo>, ImmutableBytesWritable, Put>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<ImmutableBytesWritable, Put> call(Tuple2<String, HotelUserProfileInfo> original) throws Exception {
				byte[] bytes = Bytes.toBytes(original._1());
				Double lastConsumption = original._2().getLastConsumption();
				Date lastCheckInTime = original._2().getLastCheckInTime();
				long checkInTime = lastCheckInTime.getTime();
				long nowTime = System.currentTimeMillis();
				
				String beTweenWithDays = (Math.ceil((nowTime - checkInTime) / 86400000)) + "";
				ImmutableBytesWritable rowkey = new ImmutableBytesWritable(bytes);
				Put put = new Put(bytes);
				
				put.add(FAMILY, LAST_CHECKIN_TIME, Bytes.toBytes(ThreadSafeDateUtils.secFormat(lastCheckInTime)));
				put.add(FAMILY, BE_TWEEN_WITH_LAST_CHECKIN_DAYS, Bytes.toBytes(beTweenWithDays));
				put.add(FAMILY, LAST_CONSUMPTION, Bytes.toBytes(lastConsumption.toString()));
				return new Tuple2<ImmutableBytesWritable, Put>(rowkey, put);
			}
		});
		
		SparkUtils.logRDDcount("Last CheckIn Time", result, debugFlag);
		result.saveAsHadoopDataset(outJobConfig);
	}

	private static JavaPairRDD<String, HotelUserProfileInfo> getUserProfileInfo(JavaPairRDD<HotelOrderInfo, BigdataMemberInfo> bigdataHotelMemberInfoRDD) {
		JavaPairRDD<String, HotelUserProfileInfo> result = bigdataHotelMemberInfoRDD.map(new PairFunction<Tuple2<HotelOrderInfo, BigdataMemberInfo>, String, HotelUserProfileInfo>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, HotelUserProfileInfo> call(Tuple2<HotelOrderInfo, BigdataMemberInfo> original) throws Exception {
				HotelOrderInfo order = original._1();
				BigdataMemberInfo member = original._2();
				HotelUserProfileInfo hotelUserProfileInfo = new HotelUserProfileInfo(order, member);
				
				String memberID = member.getROWKEY();
				
				//酒店品牌
				String hotelBrand = order.getHotelBrand();
				hotelBrand = SparkUtils.isNotBlank(hotelBrand) ? hotelBrand : "unknow";
				hotelUserProfileInfo.setHotelBrand(new NumberMap(hotelBrand, 1));
				//酒店星级 
				String hotelStar = order.getHotelStar();
				hotelStar = SparkUtils.isNotBlank(hotelStar) ? hotelStar : "unknow";
				hotelUserProfileInfo.setHotelStar(new NumberMap(hotelStar, 1));
				
				//酒店名称
				String hotelName = order.getHotelName();
				hotelName = SparkUtils.isNotBlank(hotelName) ? hotelName : "unknow";
				hotelUserProfileInfo.setHotelName(new NumberMap(hotelName, 1));
				
				//酒店城市
				String hotelCity = order.getHotelCity();
				hotelCity = SparkUtils.isNotBlank(hotelCity) ? hotelCity : "unknow";
				hotelUserProfileInfo.setHotelCity(new NumberMap(hotelCity, 1));
				
				//最后一次入住时间
				Date checkInTime = order.getActualStoreTime();
				hotelUserProfileInfo.setLastCheckInTime(checkInTime);
				//预定时间
				Date bookingTime = order.getBookingDate();
				hotelUserProfileInfo.setBookingTime(bookingTime);
				
				//入住月份
				String month = checkInTime!=null?ThreadSafeDateUtils.getTimeField(checkInTime, Calendar.MONTH) + 1 + "月":"unknow";
				hotelUserProfileInfo.setCheckInMonth(new NumberMap(month, 1));
				
				//入住月份和酒店城市
				String monthAndHotelCity = month + "/" + hotelCity;
				hotelUserProfileInfo.setMonthAndHotelCity(new NumberMap(monthAndHotelCity, 1));
				
				//入住月份和酒店
				String checkInMonthAndHotel = month + "/" + hotelName;
				hotelUserProfileInfo.setCheckInMonthAndHotel(new NumberMap(checkInMonthAndHotel, 1));
				
				//用户偏好备注
				String userRequire = order.getDescript();
				userRequire = SparkUtils.isNotBlank(userRequire) ? userRequire : "None";
				hotelUserProfileInfo.setUserRequire(new NumberMap(userRequire, 1));
				
				String payResource = order.getPayResource();
				payResource = SparkUtils.isNotBlank(payResource) ? payResource : "Other";
				hotelUserProfileInfo.setPayResource(new NumberMap(payResource, 1));
				
				//预定渠道
				String channel = order.getChannel();
				channel = SparkUtils.isNotBlank(channel) ? channel : "unknow";
				hotelUserProfileInfo.setBookingChannel(new NumberMap(channel, 1));
				
				//预定次数
				hotelUserProfileInfo.setBookingCount(1);
				
				if(SparkUtils.isNotBlank(order.getCRM_TXN_ROWKEY())){
					//消费金额
					String actualPrice = order.getActualPrice();
					double price = SparkUtils.isNotBlank(actualPrice) ? Double.parseDouble(actualPrice) : 0.0d;
					hotelUserProfileInfo.setSumConsumption(price);
					hotelUserProfileInfo.setLastConsumption(price);
					//入住天数
					String days = order.getDays();
					if(SparkUtils.isNotBlank(days)){
						hotelUserProfileInfo.setCheckInDays(Double.parseDouble(days));
					}
					
					//实际入住次数
					hotelUserProfileInfo.setCheckInCount(1);
				}
				
				//价格类型
				String rateType = order.getRateType();
				rateType = SparkUtils.isNotBlank(rateType) ? rateType : "unknow";
				hotelUserProfileInfo.setRateType(new NumberMap(rateType, 1));
				
				//入住节假日
				{
					String holidays = order.getHolidays();
					NumberMap result = new NumberMap();
					if(SparkUtils.isNotBlank(holidays)){
						String[] split = holidays.split("\\|");
						if(split.length > 1){
							for(String holiday : split){
								if(!"common".equals(holiday)) result.putAll(new NumberMap(holiday, 1));
							}
						}else{
							result.putAll(new NumberMap(holidays, 1));
						}
					}
					hotelUserProfileInfo.setHolidays(result);
				}
				
				return new Tuple2<String, HotelUserProfileInfo>(memberID, hotelUserProfileInfo);
			}
		});
		
		return result;
	}
	
}
