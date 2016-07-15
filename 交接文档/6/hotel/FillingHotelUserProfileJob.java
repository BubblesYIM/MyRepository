package com.jje.bigdata.userProfile.hotel;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import scala.Tuple2;

import com.jje.bigdata.census.ThreadSafeDateUtils;
import com.jje.bigdata.travel.bigdata.domain.Hotel;
import com.jje.bigdata.travel.bigdata.service.HotelService;
import com.jje.bigdata.userProfile.hotel.domain.HotelOrderInfo;
import com.jje.bigdata.userProfile.travel.domain.BigdataMemberInfo;
import com.jje.bigdata.util.SparkUtils;
import com.jje.bigdata.util.WritableTableOutputFormat;

public class FillingHotelUserProfileJob {

	private static JavaSparkContext sc = null;
	private static Configuration inConf = null;
	private static Configuration outConf = null;

	private static boolean debugFlag = false;
	private static boolean testFlag = false;
	private static int partitionSize;

	static {
		try {
			inConf = HBaseConfiguration.create();
			outConf = HBaseConfiguration.create();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws Exception {

		System.out.println("============================FillingHotelUserProfileJob Main开始" + new Date() + "=============================");
		if (args.length < 3) {
			System.out.println("/usr/lib/ngmr/run_app lexus-1.0-SNAPSHOT.jar com.jje.bigdata.userProfile.hotel.FillingHotelUserProfileJob ngmr-yarn-client ./lexus-1.0-SNAPSHOT.jar 3000 true true");
			System.exit(1);
		}
		SparkConf sparkConf = new SparkConf();
//		sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
//		sparkConf.set("spark.kryo.registrator", "com.jje.bigdata.util.MyKryoRegistrator");
		sc = new JavaSparkContext(args[0], "FillingHotelUserProfileJob", sparkConf);

		sc.addJar(args[1]);

		System.out.println("#############args:" + Arrays.asList(args));

		if (args.length >= 4) {
			partitionSize = Integer.parseInt(args[2]);
			System.out.println("###############partitionSize = " + partitionSize);
		}

		if (args.length >= 4) {
			debugFlag = Boolean.parseBoolean(args[3]);
			System.out.println("###############debugFlag = " + debugFlag);
		}

		if (args.length >= 5) {
			testFlag = Boolean.parseBoolean(args[4]);
			System.out.println("###############testFlag = " + testFlag);
		}

		run();

		System.out.println("============================FillingHotelUserProfileJob Main结束" + new Date() + "=============================");
	}

	private static void run() throws IOException {

		JavaPairRDD<HotelOrderInfo, BigdataMemberInfo> orderMemberRDD = getData(sc, inConf);

		SparkUtils.logRDDcount("orderMemberRDD", orderMemberRDD, debugFlag);

		orderMemberRDD = fillOrderData(orderMemberRDD);

		saveDataToHbase(orderMemberRDD);
		// save中间数据至hbase
	}

	private static JavaPairRDD<HotelOrderInfo, BigdataMemberInfo> getData(JavaSparkContext sc, Configuration conf) {
		JavaRDD<String> textFileRDD = sc.textFile("hdfs:///tmp/jarvis/" + "TB_HOTEL_USER_PROFILE_DATA_TMP", partitionSize);

		JavaPairRDD<HotelOrderInfo, BigdataMemberInfo> orderRDD = textFileRDD.map(new PairFunction<String, HotelOrderInfo, BigdataMemberInfo>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<HotelOrderInfo, BigdataMemberInfo> call(String input) throws Exception {
				int index = StringUtils.indexOf(input, "{\"");
				if (index <= 0) {
					return new Tuple2<HotelOrderInfo, BigdataMemberInfo>(null, null);
				}
				String json = input.substring(index, input.length()).trim();

				ObjectMapper objectMapper = new ObjectMapper();
				Map<String, String> dataMap = objectMapper.readValue(json, new TypeReference<HashMap<String, String>>() {
				});

				HotelOrderInfo order = SparkUtils.fillFieldDataFormObject(dataMap, "O", HotelOrderInfo.class);
				BigdataMemberInfo member = SparkUtils.fillFieldDataFormObject(dataMap, "M", BigdataMemberInfo.class);

				return new Tuple2<HotelOrderInfo, BigdataMemberInfo>(order, member);
			}

		}).filter(new Function<Tuple2<HotelOrderInfo, BigdataMemberInfo>, Boolean>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<HotelOrderInfo, BigdataMemberInfo> original) throws Exception {
				return original._1() != null && original._2() != null;
			}
		});
		return orderRDD;
	}

	private static void saveDataToHbase(JavaPairRDD<HotelOrderInfo, BigdataMemberInfo> orderMemberRDD) {
		outConf.set(TableOutputFormat.OUTPUT_TABLE, "TB_HOTEL_USER_PROFILE_DATA");
		JobConf jobConf = new JobConf(outConf, FillingHotelUserProfileJob.class);
		jobConf.setOutputFormat(WritableTableOutputFormat.class);

		orderMemberRDD.map(new PairFunction<Tuple2<HotelOrderInfo, BigdataMemberInfo>, ImmutableBytesWritable, Put>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<ImmutableBytesWritable, Put> call(Tuple2<HotelOrderInfo, BigdataMemberInfo> input) throws Exception {
				try {
					Map<String, String> fieldMap = new HashMap<String, String>();
					addField(input._1(), HotelOrderInfo.class, "O_", fieldMap);
					addField(input._2(), BigdataMemberInfo.class, "M_", fieldMap);

					ImmutableBytesWritable row = new ImmutableBytesWritable(new byte[0]);
					if (fieldMap.isEmpty()) {
						return new Tuple2<ImmutableBytesWritable, Put>(row, null);
					}

					StringBuilder rowkey = new StringBuilder();
					rowkey.append(input._1() == null ? "NULL" : input._1().getROWKEY() == null ? "NULL" : input._1().getROWKEY());
					rowkey.append("#");
					rowkey.append(input._2() == null ? "NULL" : input._2().getROWKEY() == null ? "NULL" : input._2().getROWKEY());

					Put put = new Put(Bytes.toBytes(rowkey.toString()));

					for (Map.Entry<String, String> entry : fieldMap.entrySet()) {
						put.add(Bytes.toBytes("info"), Bytes.toBytes(entry.getKey()), Bytes.toBytes(entry.getValue()));
					}

					return new Tuple2<ImmutableBytesWritable, Put>(row, put);
				} catch (Exception e) {
					StringWriter sw = new StringWriter();
					PrintWriter pw = new PrintWriter(sw);
					e.printStackTrace(pw);
					throw new RuntimeException(e.getMessage() + "::::::" + input.toString() + "\n" + pw, e);
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
					if ("serialVersionUID".equals(fieldName) || dataObj == null) {
						continue;
					}
					if (dataObj instanceof Date) {
						Date date = (Date) dataObj;
						dataStr = ThreadSafeDateUtils.secFormat(date);
					} else {
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

	private static JavaPairRDD<HotelOrderInfo, BigdataMemberInfo> fillOrderData(JavaPairRDD<HotelOrderInfo, BigdataMemberInfo> orderMemberRDD) throws IOException {
		Map<String, Hotel> hotelMap = HotelService.getHotelMap(sc, SparkUtils.getConfig());
		System.out.println("==========hotel Map size: " + hotelMap.size());
		final Broadcast<ConcurrentHashMap<String, Hotel>> bHotelMap = sc.broadcast(new ConcurrentHashMap<String, Hotel>(hotelMap));
		Map<String, Map<String, String>> hotelNameCross = getHotelNameCross(sc, inConf);
		final Broadcast<ConcurrentHashMap<String, Map<String, String>>> bHotelNameCross = sc.broadcast(new ConcurrentHashMap<String, Map<String, String>>(hotelNameCross));
		Map<String, Map<String, String>> holiDays = getHolidays(sc, inConf);
		System.out.println("==========holiDays Map size: " + holiDays.size());
		final Broadcast<ConcurrentHashMap<String, Map<String, String>>> bHolidays = sc.broadcast(new ConcurrentHashMap<String, Map<String, String>>(holiDays));

		JavaPairRDD<String, List<String>> parttners = getParttners(orderMemberRDD);
		JavaPairRDD<HotelOrderInfo, BigdataMemberInfo> resultRDD = leftoutJoinParttner(orderMemberRDD, parttners);
		SparkUtils.logRDDcount("parttnerOrderMemberRDD", resultRDD, debugFlag);
		
		JavaPairRDD<String, Map<String, String>> txnRDD = getTxnRDD(sc, inConf);
		SparkUtils.logRDDcount("txnRDD", txnRDD, debugFlag);
		
		resultRDD = joinTxnRDD(resultRDD, txnRDD, bHotelMap, bHotelNameCross, bHolidays);
		SparkUtils.logRDDcount("resultRDD", resultRDD, debugFlag);

		JavaPairRDD<String, Map<String, String>> paylogsRDD = getPaylog(partitionSize);
		SparkUtils.logRDDcount("paylogsRDD", paylogsRDD, debugFlag);

		resultRDD = leftOutJoin(resultRDD, paylogsRDD);
		SparkUtils.logRDDcount("paylogsOrderMemberRDD", resultRDD, debugFlag);

		JavaPairRDD<String, Map<String, String>> resvRDD = getResvRDD(sc, inConf);
		SparkUtils.logRDDcount("resvRDD", resvRDD, debugFlag);
		resultRDD = joinResv(resultRDD, resvRDD);
		SparkUtils.logRDDcount("joinResv", resultRDD, debugFlag);

		JavaPairRDD<String, Map<String, String>> resvDetailRDD = getResvDetail(sc, inConf);
		SparkUtils.logRDDcount("resvDetailRDD", resvDetailRDD, debugFlag);
		resultRDD = joinResvDetail(resultRDD, resvDetailRDD);
		SparkUtils.logRDDcount("joinResvDetail", resultRDD, debugFlag);

		JavaPairRDD<String, Map<String, String>> receptionRDD = getReceptionRDD(sc, inConf);
		SparkUtils.logRDDcount("receptionRDD", receptionRDD, debugFlag);
		resultRDD = joinReception(resultRDD, receptionRDD);
		SparkUtils.logRDDcount("joinReception", resultRDD, debugFlag);

		return resultRDD;
	}

	private static Map<String, Map<String, String>> getHolidays(JavaSparkContext sc, Configuration conf) throws IOException {
		return SparkUtils.getHbaseMapRDD(sc, conf, "TB_CLENDAR_HOLIDAY_MAP", "f").collectAsMap();
	}

	private static Map<String, Map<String, String>> getHotelNameCross(JavaSparkContext sc, Configuration conf) throws IOException {
		Map<String, Map<String, String>> result = SparkUtils.getHbaseMapRDD(sc, conf, "TB_REC_HOTEL_NAME_CODES_MAPPING", "info").collectAsMap();
		return result;
	}

	private static JavaPairRDD<HotelOrderInfo, BigdataMemberInfo> joinTxnRDD(JavaPairRDD<HotelOrderInfo, BigdataMemberInfo> resultRDD, JavaPairRDD<String, Map<String, String>> txnRDD, final Broadcast<ConcurrentHashMap<String, Hotel>> bHotelMap, final Broadcast<ConcurrentHashMap<String, Map<String, String>>> bHotelNameCross, final Broadcast<ConcurrentHashMap<String, Map<String, String>>> bHolidays) {
		JavaPairRDD<String, Tuple2<HotelOrderInfo, BigdataMemberInfo>> leftRDD = resultRDD.map(new PairFunction<Tuple2<HotelOrderInfo, BigdataMemberInfo>, String, Tuple2<HotelOrderInfo, BigdataMemberInfo>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Tuple2<HotelOrderInfo, BigdataMemberInfo>> call(Tuple2<HotelOrderInfo, BigdataMemberInfo> original) throws Exception {
				String rowkey = original._1().getCRM_TXN_ROWKEY();
				if (SparkUtils.isBlank(rowkey)) {
					rowkey = UUID.randomUUID().toString();
				}
				return new Tuple2<String, Tuple2<HotelOrderInfo, BigdataMemberInfo>>(rowkey, original);
			}
		}).partitionBy(new HashPartitioner(partitionSize));
		SparkUtils.logRDDcount("txn leftRDD", leftRDD);

		JavaPairRDD<String, Tuple2<Tuple2<HotelOrderInfo, BigdataMemberInfo>, Map<String, String>>> joinRDD = SparkUtils.leftOutJoin(leftRDD, txnRDD, partitionSize);

		SparkUtils.logRDDcount("txn joinRDD", joinRDD);

		JavaPairRDD<HotelOrderInfo, BigdataMemberInfo> result = joinRDD.map(new PairFunction<Tuple2<String, Tuple2<Tuple2<HotelOrderInfo, BigdataMemberInfo>, Map<String, String>>>, HotelOrderInfo, BigdataMemberInfo>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<HotelOrderInfo, BigdataMemberInfo> call(Tuple2<String, Tuple2<Tuple2<HotelOrderInfo, BigdataMemberInfo>, Map<String, String>>> original) throws Exception {
				Tuple2<HotelOrderInfo, BigdataMemberInfo> leftInfo = original._2()._1();
				HotelOrderInfo hotelInfo = leftInfo._1();

				Map<String, String> txnInfo = original._2()._2();

				hotelInfo.setOrderNo("CRM_" + original._1());
				String bookingDT = txnInfo.get("BOOKING_DT");
				if (SparkUtils.isNotBlank(bookingDT)) {
					hotelInfo.setBookingDate(ThreadSafeDateUtils.secParse(bookingDT));
				}

				Date storeTime = null;
				Date departTime = null;
				String arrDT = txnInfo.get("START_DT");
				if (SparkUtils.isNotBlank(arrDT)) {
					storeTime = ThreadSafeDateUtils.secParse(arrDT);
					hotelInfo.setActualStoreTime(storeTime);
				}
				String departDT = txnInfo.get("END_DT");
				if (SparkUtils.isNotBlank(departDT)) {
					departTime = ThreadSafeDateUtils.secParse(departDT);
					hotelInfo.setActualDepartTime(departTime);
				}

				if (hotelInfo.getActualStoreTime() != null) {
					ConcurrentHashMap<String, Map<String, String>> holiDays = bHolidays.value();
					if (hotelInfo.getActualDepartTime() != null) {
						Calendar date1 = new GregorianCalendar();
						Calendar date2 = new GregorianCalendar();
						date1.setTime(hotelInfo.getActualStoreTime());
						date2.setTime(hotelInfo.getActualDepartTime());

						Set<String> holidaySet = new java.util.HashSet<String>();

						for (; date1.compareTo(date2) <= 0;) {
							String day = date1.get(Calendar.YEAR) + "-" + String.format("%02d", date1.get(Calendar.MONTH) + 1) + "-" + String.format("%02d", date1.get(Calendar.DATE));
							Set<String> holiday = getHoliday(holiDays, day);
							holidaySet.addAll(holiday);
							date1.add(Calendar.DAY_OF_YEAR, 1);
						}
						hotelInfo.setHolidays(StringUtils.join(holidaySet, "|"));
					} else {
						String day = ThreadSafeDateUtils.dayFormat(hotelInfo.getActualStoreTime());
						Set<String> holiday = getHoliday(holiDays, day);
						hotelInfo.setHolidays(StringUtils.join(holiday, "|"));
					}
				} else {
					hotelInfo.setHolidays("common");
				}

				if (storeTime != null && departTime != null) {
					double days = ThreadSafeDateUtils.getDaySpace(storeTime, departTime);
					hotelInfo.setDays(days + "");
					if (days > 0) {
						String roomNights = txnInfo.get("X_ROOM_NIGHTS");
						if (SparkUtils.isNotBlank(roomNights)) {
							double roomNum = (Double.parseDouble(roomNights) < 1 ? 1 : Math.floor(Double.parseDouble(roomNights))) / days;
							hotelInfo.setRoomNum(roomNum + "");
						}
					}
				}

				String ordersource = txnInfo.get("TXN_CHANNEL_CD");
				if ("网站".equals(ordersource) || "Web".equals(ordersource)) {
					hotelInfo.setChannel("网站");
				} else if ("CallCenter".equals(ordersource) || "呼叫中心".equals(ordersource)) {
					hotelInfo.setChannel("订房中心");
				} else if ("Shop".equals(ordersource) || "门店".equals(ordersource)) {
					hotelInfo.setChannel("门店前台");
				} else {
					hotelInfo.setChannel("其他");
				}
				String roomNights = txnInfo.get("X_ROOM_NIGHTS");
				if (SparkUtils.isNotBlank(roomNights)) {
					hotelInfo.setRoomNights(roomNights);
				}
				String actualPrice = txnInfo.get("AMT_VAL");
				if (SparkUtils.isNotBlank(actualPrice) && !"0".equals(actualPrice)) {
					hotelInfo.setActualPrice(actualPrice);
				}
				String hotelName = txnInfo.get("X_HOTEL_NAME");
				if (SparkUtils.isNotBlank(hotelName)) {
					ConcurrentHashMap<String, Map<String, String>> hotelNameCross = bHotelNameCross.value();
					Map<String, String> hotelCross = hotelNameCross.get(hotelName);
					if (hotelCross != null && !hotelCross.isEmpty()) {
						String hotelID = hotelCross.get("HBP_HOTEL_ID");
						if (SparkUtils.isNotBlank(hotelID)) {
							Map<String, Hotel> hotels = bHotelMap.value();
							Hotel hotel = hotels.get(hotelID);
							if (hotel != null) {
								hotelInfo.setHotelName(hotel.getName());
								hotelInfo.setHotelID(hotelID);
								hotelInfo.setHotelBrand(hotel.getBrand());
								hotelInfo.setHotelStar(hotel.getBusinessStarRating());
								hotelInfo.setHotelCity(hotel.getCity());
							}
						}
					}
				}
				return leftInfo;
			}

			private Set<String> getHoliday(ConcurrentHashMap<String, Map<String, String>> holiDays, String day) {
				Map<String, String> holiday = holiDays.get(day);
				if (holiday != null) {
					return holiday.keySet();
				}
				Set<String> result = new HashSet<String>();
				result.add("common");
				return result;
			}
		});

		return result;
	}

	private static JavaPairRDD<String, Map<String, String>> getTxnRDD(JavaSparkContext sc, Configuration conf) throws IOException {
		return SparkUtils.getMapRDDFromHdfs(sc, "JJ000_SIEBEL_S_LOY_TXN_MAP", partitionSize);
	}

	private static JavaPairRDD<HotelOrderInfo, BigdataMemberInfo> joinResvDetail(JavaPairRDD<HotelOrderInfo, BigdataMemberInfo> resultRDD, JavaPairRDD<String, Map<String, String>> resvDetailRDD) {
		JavaPairRDD<String, Tuple2<HotelOrderInfo, BigdataMemberInfo>> leftRDD = resultRDD.map(new PairFunction<Tuple2<HotelOrderInfo, BigdataMemberInfo>, String, Tuple2<HotelOrderInfo, BigdataMemberInfo>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Tuple2<HotelOrderInfo, BigdataMemberInfo>> call(Tuple2<HotelOrderInfo, BigdataMemberInfo> original) throws Exception {
				String rowkey = original._1().getPMS_RESV_ROWKEY();
				if (SparkUtils.isBlank(rowkey)) {
					rowkey = UUID.randomUUID().toString();
				}
				return new Tuple2<String, Tuple2<HotelOrderInfo, BigdataMemberInfo>>(rowkey, original);
			}
		}).partitionBy(new HashPartitioner(partitionSize));

		JavaPairRDD<String, Tuple2<Tuple2<HotelOrderInfo, BigdataMemberInfo>, Map<String, String>>> joinRDD = SparkUtils.leftOutJoin(leftRDD, resvDetailRDD, partitionSize);

		JavaPairRDD<HotelOrderInfo, BigdataMemberInfo> result = joinRDD.map(new PairFunction<Tuple2<String, Tuple2<Tuple2<HotelOrderInfo, BigdataMemberInfo>, Map<String, String>>>, HotelOrderInfo, BigdataMemberInfo>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<HotelOrderInfo, BigdataMemberInfo> call(Tuple2<String, Tuple2<Tuple2<HotelOrderInfo, BigdataMemberInfo>, Map<String, String>>> original) throws Exception {
				Tuple2<HotelOrderInfo, BigdataMemberInfo> leftInfo = original._2()._1();
				HotelOrderInfo hotelInfo = leftInfo._1();

				Map<String, String> resvDetailInfo = original._2()._2();
				if (resvDetailInfo != null && !resvDetailInfo.isEmpty()) {
					String days = resvDetailInfo.get("Days");
					hotelInfo.setDays(days);
					String rateType = resvDetailInfo.get("RateCodeDescript");
					hotelInfo.setRateType(rateType);
					String rateGroupType = resvDetailInfo.get("RateCodeGroupDescript");
					hotelInfo.setRateGroupType(rateGroupType);
					String descript = resvDetailInfo.get("Descript");
					hotelInfo.setDescript(descript);
					String webfee = resvDetailInfo.get("webfee");
					hotelInfo.setWebFee(webfee);
				}
				return leftInfo;
			}
		});

		return result;
	}

	private static JavaPairRDD<String, Map<String, String>> getResvDetail(JavaSparkContext sc, Configuration conf) throws IOException {
		return SparkUtils.getMapRDDFromHdfs(sc, "PMS_HT_ResvDetail_MAP", partitionSize);
	}

	private static JavaPairRDD<HotelOrderInfo, BigdataMemberInfo> joinResv(JavaPairRDD<HotelOrderInfo, BigdataMemberInfo> resultRDD, JavaPairRDD<String, Map<String, String>> resvRDD) {
		JavaPairRDD<String, Tuple2<HotelOrderInfo, BigdataMemberInfo>> leftRDD = resultRDD.map(new PairFunction<Tuple2<HotelOrderInfo, BigdataMemberInfo>, String, Tuple2<HotelOrderInfo, BigdataMemberInfo>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Tuple2<HotelOrderInfo, BigdataMemberInfo>> call(Tuple2<HotelOrderInfo, BigdataMemberInfo> original) throws Exception {
				String rowkey = original._1().getPMS_RESV_ROWKEY();
				if (SparkUtils.isBlank(rowkey)) {
					rowkey = UUID.randomUUID().toString();
				}
				return new Tuple2<String, Tuple2<HotelOrderInfo, BigdataMemberInfo>>(rowkey, original);
			}
		}).partitionBy(new HashPartitioner(partitionSize));

		JavaPairRDD<String, Tuple2<Tuple2<HotelOrderInfo, BigdataMemberInfo>, Map<String, String>>> joinRDD = SparkUtils.leftOutJoin(leftRDD, resvRDD, partitionSize);

		JavaPairRDD<HotelOrderInfo, BigdataMemberInfo> result = joinRDD.map(new PairFunction<Tuple2<String, Tuple2<Tuple2<HotelOrderInfo, BigdataMemberInfo>, Map<String, String>>>, HotelOrderInfo, BigdataMemberInfo>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<HotelOrderInfo, BigdataMemberInfo> call(Tuple2<String, Tuple2<Tuple2<HotelOrderInfo, BigdataMemberInfo>, Map<String, String>>> original) throws Exception {
				Tuple2<HotelOrderInfo, BigdataMemberInfo> leftInfo = original._2()._1();
				HotelOrderInfo hotelInfo = leftInfo._1();

				Map<String, String> resvInfo = original._2()._2();
				if (resvInfo != null && !resvInfo.isEmpty()) {
					hotelInfo.setOrderNo("PMS_" + original._1());
					String strBookingDate = resvInfo.get("DT");
					hotelInfo.setBookingDate(ThreadSafeDateUtils.secParse(strBookingDate));
					String strStoreTime = resvInfo.get("OldArrDT");
					hotelInfo.setStoreTime(ThreadSafeDateUtils.day2Parse(strStoreTime));
					String strDepartTime = resvInfo.get("OldDepDT");
					hotelInfo.setDepartTime(ThreadSafeDateUtils.day2Parse(strDepartTime));
					String paymentType = resvInfo.get("CRSPaymentType");
					if ("0".equals(paymentType)) {
						hotelInfo.setPaymentType("现付");
					} else if ("1".equals(paymentType)) {
						hotelInfo.setPaymentType("月结");
					} else if ("2".equals(paymentType)) {
						hotelInfo.setPaymentType("预付");
					} else if ("3".equals(paymentType)) {
						hotelInfo.setPaymentType("担保");
					} else {
						hotelInfo.setPaymentType("其他");
					}
					
					String status = resvInfo.get("Status");
					if ("C".equals(status)) {
						hotelInfo.setStatus("取消");
					} else {
						hotelInfo.setStatus("其他");
					}
					
					String updateTime = resvInfo.get("UpdateDT");
					hotelInfo.setUpdateTime(ThreadSafeDateUtils.secParse(updateTime));
					String channelCode = resvInfo.get("ChannelCode");
					if ("17U".equals(channelCode)) {
						hotelInfo.setChannel("同程");
					} else if ("ANDROID".equals(channelCode)) {
						hotelInfo.setChannel("锦江之星安卓手机客户端");
					} else if ("APP".equals(channelCode)) {
						hotelInfo.setChannel("锦江之星IOS手机客户端");
					} else if ("BSCRO".equals(channelCode)) {
						hotelInfo.setChannel("订房中心");
					} else if ("BSFO".equals(channelCode)) {
						hotelInfo.setChannel("门店前台");
					} else if ("BSWEB".equals(channelCode)) {
						hotelInfo.setChannel("网站");
					} else if ("COL".equals(channelCode)) {
						hotelInfo.setChannel("畅联");
					} else if ("CRO".equals(channelCode)) {
						hotelInfo.setChannel("订房中心");
					} else if ("CTRIP".equals(channelCode)) {
						hotelInfo.setChannel("携程");
					} else if ("DEER".equals(channelCode)) {
						hotelInfo.setChannel("德尔");
					} else if ("DERBY".equals(channelCode)) {
						hotelInfo.setChannel("德比");
					} else if ("DX".equals(channelCode)) {
						hotelInfo.setChannel("点行");
					} else if ("ELONG".equals(channelCode)) {
						hotelInfo.setChannel("艺龙");
					} else if ("FO".equals(channelCode)) {
						hotelInfo.setChannel("门店前台");
					} else if ("GOLD".equals(channelCode)) {
						hotelInfo.setChannel("金界");
					} else if ("JCAS".equals(channelCode)) {
						hotelInfo.setChannel("网站");
					} else if ("KJJD".equals(channelCode)) {
						hotelInfo.setChannel("快捷手机客户端");
					} else if ("QQ".equals(channelCode)) {
						hotelInfo.setChannel("腾讯");
					} else if ("TAOBAO".equals(channelCode)) {
						hotelInfo.setChannel("天猫官方旗舰店");
					} else if ("WAP".equals(channelCode)) {
						hotelInfo.setChannel("手机网站");
					} else if ("WEB".equals(channelCode)) {
						hotelInfo.setChannel("网站");
					} else if ("ZGHX".equals(channelCode)) {
						hotelInfo.setChannel("中国航信");
					} else {
						hotelInfo.setChannel("其他");
					}
				}
				return leftInfo;
			}
		});

		return result;
	}

	private static JavaPairRDD<String, Map<String, String>> getResvRDD(JavaSparkContext sc, Configuration conf) throws IOException {
		return SparkUtils.getMapRDDFromHdfs(sc, "PMS_HT_Resv_MAP", partitionSize);
	}

	private static JavaPairRDD<HotelOrderInfo, BigdataMemberInfo> joinReception(JavaPairRDD<HotelOrderInfo, BigdataMemberInfo> resultRDD, JavaPairRDD<String, Map<String, String>> receptionRDD) {
		JavaPairRDD<String, Tuple2<HotelOrderInfo, BigdataMemberInfo>> leftRDD = resultRDD.map(new PairFunction<Tuple2<HotelOrderInfo, BigdataMemberInfo>, String, Tuple2<HotelOrderInfo, BigdataMemberInfo>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Tuple2<HotelOrderInfo, BigdataMemberInfo>> call(Tuple2<HotelOrderInfo, BigdataMemberInfo> original) throws Exception {
				String rowkey = original._1().getPMS_REC_ROWKEY();
				if (SparkUtils.isBlank(rowkey)) {
					rowkey = UUID.randomUUID().toString();
				}
				return new Tuple2<String, Tuple2<HotelOrderInfo, BigdataMemberInfo>>(StringUtils.reverse(rowkey), original);
			}
		}).partitionBy(new HashPartitioner(partitionSize));
		SparkUtils.logRDDcount("reception leftRDD", leftRDD);

		JavaPairRDD<String, Tuple2<Tuple2<HotelOrderInfo, BigdataMemberInfo>, Map<String, String>>> joinRDD = SparkUtils.leftOutJoin(leftRDD, receptionRDD, partitionSize);

		SparkUtils.logRDDcount("joinRDD", joinRDD);

		JavaPairRDD<HotelOrderInfo, BigdataMemberInfo> result = joinRDD.map(new PairFunction<Tuple2<String, Tuple2<Tuple2<HotelOrderInfo, BigdataMemberInfo>, Map<String, String>>>, HotelOrderInfo, BigdataMemberInfo>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<HotelOrderInfo, BigdataMemberInfo> call(Tuple2<String, Tuple2<Tuple2<HotelOrderInfo, BigdataMemberInfo>, Map<String, String>>> original) throws Exception {
				Map<String, String> receptionInfo = original._2()._2();

				Tuple2<HotelOrderInfo, BigdataMemberInfo> leftInfo = original._2()._1();
				HotelOrderInfo hotelInfo = leftInfo._1();
				if (receptionInfo != null && !receptionInfo.isEmpty()) {
					String arrDT = receptionInfo.get("ArrDT");
					hotelInfo.setActualStoreTime(ThreadSafeDateUtils.secParse(arrDT));
					String depDT = receptionInfo.get("DepDT");
					hotelInfo.setActualDepartTime(ThreadSafeDateUtils.secParse(depDT));
				}
				return leftInfo;
			}
		});

		return result;
	}

	private static JavaPairRDD<String, Map<String, String>> getReceptionRDD(JavaSparkContext sc, Configuration conf) throws IOException {
		return SparkUtils.getMapRDDFromHdfs(sc, "PMS_HT_Reception_MAP", partitionSize).map(new PairFunction<Tuple2<String, Map<String, String>>, String, Map<String, String>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Map<String, String>> call(Tuple2<String, Map<String, String>> original) throws Exception {
				return new Tuple2<String, Map<String, String>>(StringUtils.reverse(original._1()), original._2());
			}

		});
	}

	private static JavaPairRDD<HotelOrderInfo, BigdataMemberInfo> leftoutJoinParttner(JavaPairRDD<HotelOrderInfo, BigdataMemberInfo> orderMemberRDD, JavaPairRDD<String, List<String>> parttners) {

		JavaPairRDD<String, Tuple2<Tuple2<HotelOrderInfo, BigdataMemberInfo>, List<String>>> leftRDD = orderMemberRDD.map(new PairFunction<Tuple2<HotelOrderInfo, BigdataMemberInfo>, String, Tuple2<Tuple2<HotelOrderInfo, BigdataMemberInfo>, List<String>>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Tuple2<Tuple2<HotelOrderInfo, BigdataMemberInfo>, List<String>>> call(Tuple2<HotelOrderInfo, BigdataMemberInfo> original) throws Exception {
				String resvID = original._1().getPMS_REC_ROWKEY();
				if (SparkUtils.isBlank(resvID)) {
					resvID = UUID.randomUUID().toString();
				}
				return new Tuple2<String, Tuple2<Tuple2<HotelOrderInfo, BigdataMemberInfo>, List<String>>>(resvID, new Tuple2<Tuple2<HotelOrderInfo, BigdataMemberInfo>, List<String>>(original, null));
			}
		}).partitionBy(new HashPartitioner(partitionSize));

		JavaPairRDD<String, Tuple2<Tuple2<HotelOrderInfo, BigdataMemberInfo>, List<String>>> rightRDD = parttners.map(new PairFunction<Tuple2<String, List<String>>, String, Tuple2<Tuple2<HotelOrderInfo, BigdataMemberInfo>, List<String>>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Tuple2<Tuple2<HotelOrderInfo, BigdataMemberInfo>, List<String>>> call(Tuple2<String, List<String>> original) throws Exception {
				String rowkey = original._1();
				return new Tuple2<String, Tuple2<Tuple2<HotelOrderInfo, BigdataMemberInfo>, List<String>>>(rowkey, new Tuple2<Tuple2<HotelOrderInfo, BigdataMemberInfo>, List<String>>(null, original._2()));
			}
		}).partitionBy(new HashPartitioner(partitionSize));

		JavaPairRDD<String, Tuple2<Tuple2<HotelOrderInfo, BigdataMemberInfo>, List<String>>> unionRDD = leftRDD.union(rightRDD).partitionBy(new HashPartitioner(partitionSize));

		JavaPairRDD<String, List<Tuple2<Tuple2<HotelOrderInfo, BigdataMemberInfo>, List<String>>>> groupByKey = unionRDD.groupByKey(new HashPartitioner(partitionSize));

		JavaPairRDD<String, List<Tuple2<Tuple2<HotelOrderInfo, BigdataMemberInfo>, List<String>>>> merge = groupByKey.map(new PairFunction<Tuple2<String, List<Tuple2<Tuple2<HotelOrderInfo, BigdataMemberInfo>, List<String>>>>, String, List<Tuple2<Tuple2<HotelOrderInfo, BigdataMemberInfo>, List<String>>>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, List<Tuple2<Tuple2<HotelOrderInfo, BigdataMemberInfo>, List<String>>>> call(Tuple2<String, List<Tuple2<Tuple2<HotelOrderInfo, BigdataMemberInfo>, List<String>>>> original) throws Exception {
				List<Tuple2<Tuple2<HotelOrderInfo, BigdataMemberInfo>, List<String>>> datas = original._2();

				List<Tuple2<Tuple2<HotelOrderInfo, BigdataMemberInfo>, List<String>>> left = new ArrayList<Tuple2<Tuple2<HotelOrderInfo, BigdataMemberInfo>, List<String>>>();
				List<Tuple2<Tuple2<HotelOrderInfo, BigdataMemberInfo>, List<String>>> right = new ArrayList<Tuple2<Tuple2<HotelOrderInfo, BigdataMemberInfo>, List<String>>>();

				List<Tuple2<Tuple2<HotelOrderInfo, BigdataMemberInfo>, List<String>>> result = new ArrayList<Tuple2<Tuple2<HotelOrderInfo, BigdataMemberInfo>, List<String>>>();

				for (Tuple2<Tuple2<HotelOrderInfo, BigdataMemberInfo>, List<String>> data : datas) {
					if (data._1() == null) {
						right.add(data);
					} else if (data._2() == null) {
						left.add(data);
					}
				}

				if (right.isEmpty()) {
					return new Tuple2<String, List<Tuple2<Tuple2<HotelOrderInfo, BigdataMemberInfo>, List<String>>>>(original._1(), left);
				}

				Tuple2<Tuple2<HotelOrderInfo, BigdataMemberInfo>, List<String>> someRight = right.get(0);
				for (Tuple2<Tuple2<HotelOrderInfo, BigdataMemberInfo>, List<String>> data : left) {
					Tuple2<HotelOrderInfo, BigdataMemberInfo> leftData = data._1();

					List<String> rightData = someRight._2();

					result.add(new Tuple2<Tuple2<HotelOrderInfo, BigdataMemberInfo>, List<String>>(leftData, rightData));
				}

				return new Tuple2<String, List<Tuple2<Tuple2<HotelOrderInfo, BigdataMemberInfo>, List<String>>>>(original._1(), result);
			}
		});

		JavaPairRDD<HotelOrderInfo, BigdataMemberInfo> result = merge.flatMap(new PairFlatMapFunction<Tuple2<String, List<Tuple2<Tuple2<HotelOrderInfo, BigdataMemberInfo>, List<String>>>>, HotelOrderInfo, BigdataMemberInfo>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<Tuple2<HotelOrderInfo, BigdataMemberInfo>> call(Tuple2<String, List<Tuple2<Tuple2<HotelOrderInfo, BigdataMemberInfo>, List<String>>>> original) throws Exception {
				List<Tuple2<Tuple2<HotelOrderInfo, BigdataMemberInfo>, List<String>>> datas = original._2();
				List<Tuple2<HotelOrderInfo, BigdataMemberInfo>> result = new ArrayList<Tuple2<HotelOrderInfo, BigdataMemberInfo>>();
				for (Tuple2<Tuple2<HotelOrderInfo, BigdataMemberInfo>, List<String>> data : datas) {
					Tuple2<HotelOrderInfo, BigdataMemberInfo> left = data._1();
					BigdataMemberInfo memberInfo = left._2();
					HotelOrderInfo hotelInfo = left._1();
					List<String> right = data._2();
					if (right != null && !right.isEmpty()) {
						right.remove(memberInfo.getIdentify());
						hotelInfo.setParttners(StringUtils.join(right, ","));
					}
					result.add(left);
				}
				return result;
			}

		});

		return result;
	}

	private static JavaPairRDD<HotelOrderInfo, BigdataMemberInfo> leftOutJoin(JavaPairRDD<HotelOrderInfo, BigdataMemberInfo> parttnerOrderMemberRDD, JavaPairRDD<String, Map<String, String>> paylogsRDD) {
		JavaPairRDD<String, Tuple2<Tuple2<HotelOrderInfo, BigdataMemberInfo>, Map<String, String>>> leftRDD = parttnerOrderMemberRDD.map(new PairFunction<Tuple2<HotelOrderInfo, BigdataMemberInfo>, String, Tuple2<Tuple2<HotelOrderInfo, BigdataMemberInfo>, Map<String, String>>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Tuple2<Tuple2<HotelOrderInfo, BigdataMemberInfo>, Map<String, String>>> call(Tuple2<HotelOrderInfo, BigdataMemberInfo> original) throws Exception {
				String resvID = original._1().getPMS_RESV_ROWKEY();
				if (SparkUtils.isBlank(resvID)) {
					resvID = UUID.randomUUID().toString();
				}
				return new Tuple2<String, Tuple2<Tuple2<HotelOrderInfo, BigdataMemberInfo>, Map<String, String>>>(resvID, new Tuple2<Tuple2<HotelOrderInfo, BigdataMemberInfo>, Map<String, String>>(original, null));
			}
		}).partitionBy(new HashPartitioner(partitionSize));

		JavaPairRDD<String, Tuple2<Tuple2<HotelOrderInfo, BigdataMemberInfo>, Map<String, String>>> rightRDD = paylogsRDD.map(new PairFunction<Tuple2<String, Map<String, String>>, String, Tuple2<Tuple2<HotelOrderInfo, BigdataMemberInfo>, Map<String, String>>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Tuple2<Tuple2<HotelOrderInfo, BigdataMemberInfo>, Map<String, String>>> call(Tuple2<String, Map<String, String>> original) throws Exception {
				String rowkey = original._1();
				return new Tuple2<String, Tuple2<Tuple2<HotelOrderInfo, BigdataMemberInfo>, Map<String, String>>>(rowkey, new Tuple2<Tuple2<HotelOrderInfo, BigdataMemberInfo>, Map<String, String>>(null, original._2()));
			}
		}).partitionBy(new HashPartitioner(partitionSize));

		JavaPairRDD<String, Tuple2<Tuple2<HotelOrderInfo, BigdataMemberInfo>, Map<String, String>>> unionRDD = leftRDD.union(rightRDD).partitionBy(new HashPartitioner(partitionSize));

		JavaPairRDD<String, List<Tuple2<Tuple2<HotelOrderInfo, BigdataMemberInfo>, Map<String, String>>>> groupByKey = unionRDD.groupByKey(new HashPartitioner(partitionSize));

		JavaPairRDD<String, List<Tuple2<Tuple2<HotelOrderInfo, BigdataMemberInfo>, Map<String, String>>>> merge = groupByKey.map(new PairFunction<Tuple2<String, List<Tuple2<Tuple2<HotelOrderInfo, BigdataMemberInfo>, Map<String, String>>>>, String, List<Tuple2<Tuple2<HotelOrderInfo, BigdataMemberInfo>, Map<String, String>>>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, List<Tuple2<Tuple2<HotelOrderInfo, BigdataMemberInfo>, Map<String, String>>>> call(Tuple2<String, List<Tuple2<Tuple2<HotelOrderInfo, BigdataMemberInfo>, Map<String, String>>>> original) throws Exception {
				List<Tuple2<Tuple2<HotelOrderInfo, BigdataMemberInfo>, Map<String, String>>> datas = original._2();

				List<Tuple2<Tuple2<HotelOrderInfo, BigdataMemberInfo>, Map<String, String>>> left = new ArrayList<Tuple2<Tuple2<HotelOrderInfo, BigdataMemberInfo>, Map<String, String>>>();
				List<Tuple2<Tuple2<HotelOrderInfo, BigdataMemberInfo>, Map<String, String>>> right = new ArrayList<Tuple2<Tuple2<HotelOrderInfo, BigdataMemberInfo>, Map<String, String>>>();

				List<Tuple2<Tuple2<HotelOrderInfo, BigdataMemberInfo>, Map<String, String>>> result = new ArrayList<Tuple2<Tuple2<HotelOrderInfo, BigdataMemberInfo>, Map<String, String>>>();

				for (Tuple2<Tuple2<HotelOrderInfo, BigdataMemberInfo>, Map<String, String>> data : datas) {
					if (data._1() == null) {
						right.add(data);
					} else if (data._2() == null) {
						left.add(data);
					}
				}

				if (right.isEmpty()) {
					return new Tuple2<String, List<Tuple2<Tuple2<HotelOrderInfo, BigdataMemberInfo>, Map<String, String>>>>(original._1(), left);
				}

				Tuple2<Tuple2<HotelOrderInfo, BigdataMemberInfo>, Map<String, String>> someRight = right.get(0);
				for (Tuple2<Tuple2<HotelOrderInfo, BigdataMemberInfo>, Map<String, String>> data : left) {
					Tuple2<HotelOrderInfo, BigdataMemberInfo> leftData = data._1();

					Map<String, String> rightData = someRight._2();

					result.add(new Tuple2<Tuple2<HotelOrderInfo, BigdataMemberInfo>, Map<String, String>>(leftData, rightData));
				}

				return new Tuple2<String, List<Tuple2<Tuple2<HotelOrderInfo, BigdataMemberInfo>, Map<String, String>>>>(original._1(), result);
			}
		});

		JavaPairRDD<HotelOrderInfo, BigdataMemberInfo> result = merge.flatMap(new PairFlatMapFunction<Tuple2<String, List<Tuple2<Tuple2<HotelOrderInfo, BigdataMemberInfo>, Map<String, String>>>>, HotelOrderInfo, BigdataMemberInfo>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<Tuple2<HotelOrderInfo, BigdataMemberInfo>> call(Tuple2<String, List<Tuple2<Tuple2<HotelOrderInfo, BigdataMemberInfo>, Map<String, String>>>> original) throws Exception {
				List<Tuple2<Tuple2<HotelOrderInfo, BigdataMemberInfo>, Map<String, String>>> datas = original._2();
				List<Tuple2<HotelOrderInfo, BigdataMemberInfo>> result = new ArrayList<Tuple2<HotelOrderInfo, BigdataMemberInfo>>();
				for (Tuple2<Tuple2<HotelOrderInfo, BigdataMemberInfo>, Map<String, String>> data : datas) {
					Tuple2<HotelOrderInfo, BigdataMemberInfo> left = data._1();
					Map<String, String> right = data._2();
					if (right != null && !right.isEmpty()) {
						HotelOrderInfo hotelInfo = left._1();
						String paymentSourceID = right.get("PaymentSourceID");
						if ("X".equals(paymentSourceID)) {
							hotelInfo.setPayResource("支付宝支付");
						} else if ("V".equals(paymentSourceID)) {
							hotelInfo.setPayResource("快钱支付");
						} else if ("V".equals(paymentSourceID)) {
							hotelInfo.setPayResource("银联支付");
						} else {
							hotelInfo.setPayResource("其他支付");
						}

						String paymentChannelID = right.get("PaymentChannelID");
						hotelInfo.setPayChannel(paymentChannelID);
					}
					result.add(left);
				}
				return result;
			}

		});

		return result;
	}

	private static JavaPairRDD<String, Map<String, String>> getPaylog(int partitionSize) throws IOException {
		if (partitionSize <= 0) {
			partitionSize = 1000;
		}
		return SparkUtils.getHbaseMapRDD(sc, inConf, "PMS_HT_PaymentLog_MAP", "info").partitionBy(new HashPartitioner(partitionSize)).map(new PairFunction<Tuple2<String, Map<String, String>>, String, Map<String, String>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Map<String, String>> call(Tuple2<String, Map<String, String>> original) throws Exception {
				Map<String, String> map = original._2();
				String rowkey = map.get("ResvId");
				return new Tuple2<String, Map<String, String>>(rowkey, map);
			}
		});
	}

	private static JavaPairRDD<String, List<String>> getParttners(JavaPairRDD<HotelOrderInfo, BigdataMemberInfo> orderMemberRDD) {
		JavaPairRDD<String, List<String>> result = orderMemberRDD.filter(new Function<Tuple2<HotelOrderInfo, BigdataMemberInfo>, Boolean>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<HotelOrderInfo, BigdataMemberInfo> original) throws Exception {
				String recordID = original._1().getPMS_TXN_RECORD_ID();
				return SparkUtils.isNotBlank(recordID);
			}
		}).map(new PairFunction<Tuple2<HotelOrderInfo, BigdataMemberInfo>, String, List<String>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, List<String>> call(Tuple2<HotelOrderInfo, BigdataMemberInfo> original) throws Exception {
				String recordID = original._1().getPMS_TXN_RECORD_ID();
				List<String> arrayList = new ArrayList<String>();
				arrayList.add(original._2().getIdentify());
				return new Tuple2<String, List<String>>(recordID, arrayList);
			}
		}).reduceByKey(new Function2<List<String>, List<String>, List<String>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public List<String> call(List<String> arg0, List<String> arg1) throws Exception {
				List<String> result = new ArrayList<String>();
				result.addAll(arg0);
				result.addAll(arg1);
				return result;
			}
		}).filter(new Function<Tuple2<String, List<String>>, Boolean>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<String, List<String>> arg0) throws Exception {
				int size = arg0._2().size();
				return size > 1;
			}
		});

		SparkUtils.logRDDcount("parttners count", result, debugFlag);
		return result;
	}
}
