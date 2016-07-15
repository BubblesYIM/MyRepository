package com.jje.bigdata.userProfile.travel.tag;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
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

import com.jje.bigdata.census.ThreadSafeDateUtils;
import com.jje.bigdata.userProfile.travel.domain.BigdataMemberInfo;
import com.jje.bigdata.userProfile.travel.domain.NumberMap;
import com.jje.bigdata.userProfile.travel.domain.TravelOrderInfo;
import com.jje.bigdata.userProfile.travel.domain.TravelUserProfileInfo;
import com.jje.bigdata.util.BigdataMemberInfoUtils;
import com.jje.bigdata.util.SparkUtils;
import com.jje.bigdata.util.WritableTableOutputFormat;

public class TravelUserConsumptionProfile implements Serializable {
	private static final long serialVersionUID = 1L;
	private static JavaSparkContext sc;
	private static SparkConf sparkConf;
	private static Configuration conf;

	private static final byte[] FAMILY = Bytes.toBytes("info");
	private static final byte[] EVERY_YEAR_CONSUMPTION_NUMBER = Bytes.toBytes("EVERY_YEAR_CONSUMPTION_NUMBER");
	private static final byte[] EVERY_YEAR_BOOKING_NUMBER = Bytes.toBytes("EVERY_YEAR_BOOKING_NUMBER");
	private static final byte[] EVERY_YEAR_CONSUMPTION_AND_BOOKING_PROPORTION = Bytes.toBytes("EVERY_YEAR_CONSUMPTION_AND_BOOKING_PROPORTION");
	private static final byte[] CONSUMPTION_AND_BOOKING_PROPORTION = Bytes.toBytes("CONSUMPTION_AND_BOOKING_PROPORTION");
	private static final byte[] TOTAL_CONSUMPTION_NUMBER = Bytes.toBytes("TOTAL_CONSUMPTION_NUMBER");
	private static final byte[] TOTAL_BOOKING_NUMBER = Bytes.toBytes("TOTAL_BOOKING_NUMBER");
	private static final byte[] LAST_TRAVEL_DATE = Bytes.toBytes("LAST_TRAVEL_DATE");
	private static final byte[] BE_TWEEN_WITH_LAST_TRAVEL_DAYS = Bytes.toBytes("BE_TWEEN_WITH_LAST_TRAVEL_DAYS");
	private static final byte[] TRAVEL_BOOKING_RESOURCE = Bytes.toBytes("TRAVEL_BOOKING_RESOURCE");
	private static final byte[] TRAVEL_FREQUENCY = Bytes.toBytes("TRAVEL_FREQUENCY");
	private static final byte[] TRAVEL_AIR_LINES = Bytes.toBytes("TRAVEL_AIR_LINES");

	static {
		try {
			conf = HBaseConfiguration.create();
			conf.set(TableInputFormat.SCAN, SparkUtils.convertScanToString(new Scan()));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		Date startDate = new Date();
		Date endDate;
		System.out.println(startDate);
		System.out.println(args.length);
		if (args.length < 2) {
			System.exit(-1);
		}
		System.out.println("/usr/lib/ngmr/run_app_min lexus-1.0-SNAPSHOT.jar com.jje.bigdata.userProfile.travel.tag.TravelUserConsumptionProfile ngmr-yarn-client lexus-1.0-SNAPSHOT.jar");
		try {
			sparkConf = new SparkConf();

			sc = new JavaSparkContext(args[0], "TravelUserConsumptionProfile", sparkConf);
			sc.addJar(args[1]);

			Configuration outConfig = HBaseConfiguration.create();
			outConfig.set(TableOutputFormat.OUTPUT_TABLE, "TB_TRAVEL_USER_PROFILE");
			JobConf outJobConfig = new JobConf(outConfig, TravelUserConsumptionProfile.class);
			outJobConfig.setOutputFormat(WritableTableOutputFormat.class);

			JavaPairRDD<String, TravelUserProfileInfo> bigdataMemberInfo = BigdataMemberInfoUtils.getBigdataTravelMemberInfo(sc, conf).filter(new Function<Tuple2<TravelOrderInfo, BigdataMemberInfo>, Boolean>() {
				private static final long serialVersionUID = 1L;

				@Override
				public Boolean call(Tuple2<TravelOrderInfo, BigdataMemberInfo> original) throws Exception {
					BigdataMemberInfo memberInfo = original._2();
					TravelOrderInfo orderInfo = original._1();
					return memberInfo != null && orderInfo != null && SparkUtils.isNotBlank(memberInfo.getROWKEY()) && orderInfo.getBookingDate() != null;
				}
			}).map(new PairFunction<Tuple2<TravelOrderInfo, BigdataMemberInfo>, String, TravelUserProfileInfo>() {
				private static final long serialVersionUID = 1L;

				@Override
				public Tuple2<String, TravelUserProfileInfo> call(Tuple2<TravelOrderInfo, BigdataMemberInfo> original) throws Exception {
					BigdataMemberInfo memberInfo = original._2();
					TravelOrderInfo orderInfo = original._1();
					String memberID = memberInfo.getROWKEY();
					Date reservationDate = orderInfo.getBookingDate();
					String year = String.valueOf(reservationDate.getYear() + 1900);

					Date departDate = orderInfo.getDepartDate();

					TravelUserProfileInfo data = new TravelUserProfileInfo();
					data.setDates(new ArrayList<Date>());
					if(departDate!=null){
						data.setLastTravelDate(departDate);
						data.getDates().add(departDate);
					}

					data.setRowkey(memberID);
					if ("已出游".equals(orderInfo.getcStatus())) {
						data.setEveryYearConsumptionCount(new NumberMap(year, 1));
						data.setTotalConsumptionCount(1);
					}
					data.setTotalBookingCount(1);
					data.setEveryYearBookingCount(new NumberMap(year, 1));
					data.setStatus(orderInfo.getcStatus());
					data.setConsumptionAndBookingCount(new NumberMap(orderInfo.getcStatus(), 1));
					data.setBookingResourceCount(new NumberMap(orderInfo.getResource(), 1));
					String airLines = orderInfo.getAirLines();
					NumberMap airLinesMap = new NumberMap();
					if(SparkUtils.isNotBlank(airLines)){
//						throw new RuntimeException(airLines);
						String[] split = airLines.split(",");
						for (String str : split) {
							airLinesMap.put(str, new BigDecimal(1));
						}
					}
					data.setAirLinesMap(airLinesMap);
					return new Tuple2<String, TravelUserProfileInfo>(memberID, data);
				}
			}).cache();

			logRDDcount("Total", bigdataMemberInfo);

			saveConsumptionTag(bigdataMemberInfo, outJobConfig);
			saveBookingTag(bigdataMemberInfo, outJobConfig);
			saveConsumptionAndBookingProportionTag(bigdataMemberInfo, outJobConfig);
			saveBookingResourceTag(bigdataMemberInfo, outJobConfig);
			saveAirLinesTag(bigdataMemberInfo, outJobConfig);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		} finally {
			endDate = new Date();
			System.out.println(endDate);
			long durion = endDate.getTime() - startDate.getTime();
			System.out.println("耗时:" + durion + "毫秒");
		}
	}

	private static void saveAirLinesTag(JavaPairRDD<String, TravelUserProfileInfo> bigdataMemberInfo, JobConf outJobConfig) {
		JavaPairRDD<ImmutableBytesWritable, Put> result = bigdataMemberInfo.reduceByKey(new Function2<TravelUserProfileInfo, TravelUserProfileInfo, TravelUserProfileInfo>() {
			private static final long serialVersionUID = 1L;

			@Override
			public TravelUserProfileInfo call(TravelUserProfileInfo arg0, TravelUserProfileInfo arg1) throws Exception {
				NumberMap airLineMap = arg0.getAirLinesMap();

				airLineMap.putAll(arg1.getAirLinesMap());

				arg0.setAirLinesMap(airLineMap);
				return arg0;
			}
		}).filter(new Function<Tuple2<String,TravelUserProfileInfo>, Boolean>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<String, TravelUserProfileInfo> original) throws Exception {
				NumberMap airLines = original._2().getAirLinesMap();
				return airLines!=null && !airLines.isEmpty();
			}
		}).map(new PairFunction<Tuple2<String, TravelUserProfileInfo>, ImmutableBytesWritable, Put>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<ImmutableBytesWritable, Put> call(Tuple2<String, TravelUserProfileInfo> original) throws Exception {
				byte[] bytes = Bytes.toBytes(original._1());
				NumberMap airLinesMap = original._2().getAirLinesMap();
				ImmutableBytesWritable rowkey = new ImmutableBytesWritable(bytes);
				Put put = new Put(bytes);
				put.add(FAMILY, TRAVEL_AIR_LINES, Bytes.toBytes(airLinesMap.getMapString()));
				return new Tuple2<ImmutableBytesWritable, Put>(rowkey, put);
			}
		});
		
		logRDDcount("Air Lines", result);
		result.saveAsHadoopDataset(outJobConfig);
	}

	private static void saveBookingResourceTag(JavaPairRDD<String, TravelUserProfileInfo> bigdataMemberInfo, JobConf outJobConfig) {
		JavaPairRDD<ImmutableBytesWritable, Put> result = bigdataMemberInfo.reduceByKey(new Function2<TravelUserProfileInfo, TravelUserProfileInfo, TravelUserProfileInfo>() {
			private static final long serialVersionUID = 1L;

			@Override
			public TravelUserProfileInfo call(TravelUserProfileInfo arg0, TravelUserProfileInfo arg1) throws Exception {
				NumberMap bookingResourceNumber = arg0.getBookingResourceCount();

				bookingResourceNumber.putAll(arg1.getBookingResourceCount());

				arg0.setBookingResourceCount(bookingResourceNumber);
				return arg0;
			}
		}).map(new PairFunction<Tuple2<String, TravelUserProfileInfo>, ImmutableBytesWritable, Put>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<ImmutableBytesWritable, Put> call(Tuple2<String, TravelUserProfileInfo> original) throws Exception {
				NumberMap bookingResourceCount = original._2().getBookingResourceCount();

				byte[] bytes = Bytes.toBytes(original._1());
				ImmutableBytesWritable rowkey = new ImmutableBytesWritable(bytes);
				Put put = new Put(bytes);
				put.add(FAMILY, TRAVEL_BOOKING_RESOURCE, Bytes.toBytes(bookingResourceCount.getMapString()));
				return new Tuple2<ImmutableBytesWritable, Put>(rowkey, put);
			}
		});

		logRDDcount("Booking Resource", result);
		result.saveAsHadoopDataset(outJobConfig);
	}

	private static void saveConsumptionAndBookingProportionTag(JavaPairRDD<String, TravelUserProfileInfo> bigdataMemberInfo, JobConf outJobConfig) {
		JavaPairRDD<ImmutableBytesWritable, Put> result = bigdataMemberInfo.reduceByKey(new Function2<TravelUserProfileInfo, TravelUserProfileInfo, TravelUserProfileInfo>() {

			private static final long serialVersionUID = 1L;

			@Override
			public TravelUserProfileInfo call(TravelUserProfileInfo arg0, TravelUserProfileInfo arg1) throws Exception {
				NumberMap everyYearConsumptionResult = new NumberMap();
				everyYearConsumptionResult.putAll(arg0.getEveryYearConsumptionCount());
				everyYearConsumptionResult.putAll(arg1.getEveryYearConsumptionCount());
				arg0.setEveryYearConsumptionCount(everyYearConsumptionResult);

				NumberMap everyYearBookingResult = new NumberMap();
				everyYearBookingResult.putAll(arg0.getEveryYearBookingCount());
				everyYearBookingResult.putAll(arg1.getEveryYearBookingCount());
				arg0.setEveryYearBookingCount(everyYearBookingResult);

				NumberMap consumptionAndBookingCountResult = new NumberMap();
				consumptionAndBookingCountResult.putAll(arg0.getConsumptionAndBookingCount());
				consumptionAndBookingCountResult.putAll(arg1.getConsumptionAndBookingCount());
				arg0.setConsumptionAndBookingCount(consumptionAndBookingCountResult);

				return arg0;
			}
		}).map(new PairFunction<Tuple2<String, TravelUserProfileInfo>, ImmutableBytesWritable, Put>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<ImmutableBytesWritable, Put> call(Tuple2<String, TravelUserProfileInfo> original) throws Exception {
				NumberMap everyYearConsumptionCount = original._2().getEveryYearConsumptionCount();
				NumberMap everyYearBookingCount = original._2().getEveryYearBookingCount();


					NumberMap total = new NumberMap();
					for (String key : everyYearBookingCount.keySet()) {
						if (everyYearConsumptionCount != null) {
							BigDecimal consumption = everyYearConsumptionCount.get(key) == null ? new BigDecimal("0") : everyYearConsumptionCount.get(key);
							BigDecimal booking = everyYearBookingCount.get(key) == null ? new BigDecimal("0") : everyYearBookingCount.get(key);
							BigDecimal proportion = consumption.divide(booking, 2, RoundingMode.HALF_UP);
							total.put(key, proportion);
						}else{
							total.put(key, new BigDecimal("0"));
						}
					}

				original._2().setEveryYearConsumptionAndBookingProportion(total);

				NumberMap consumptionAndBookingCount = original._2().getConsumptionAndBookingCount();
				BigDecimal consumptionNumber = consumptionAndBookingCount.get("已出游") == null ? new BigDecimal("0") : consumptionAndBookingCount.get("已出游");
				BigDecimal bookingCount = consumptionAndBookingCount.get("预定") == null ? new BigDecimal("0") : consumptionAndBookingCount.get("预定");
				BigDecimal totalNumber = consumptionNumber.add(bookingCount);
				if (totalNumber.intValue() > 0) {
					BigDecimal totalProportion = consumptionNumber.divide(totalNumber, 2, RoundingMode.HALF_UP);
					original._2().setTotalConsumptionAndBookingProportion(totalProportion);
				} else {
					original._2().setTotalConsumptionAndBookingProportion(totalNumber);
				}

				byte[] bytes = Bytes.toBytes(original._1());
				ImmutableBytesWritable rowkey = new ImmutableBytesWritable(bytes);
				Put put = new Put(bytes);
				put.add(FAMILY, EVERY_YEAR_CONSUMPTION_AND_BOOKING_PROPORTION, Bytes.toBytes(original._2().getEveryYearConsumptionAndBookingProportion().getMapString()));
				put.add(FAMILY, CONSUMPTION_AND_BOOKING_PROPORTION, Bytes.toBytes(original._2().getTotalConsumptionAndBookingProportion().toString()));
				return new Tuple2<ImmutableBytesWritable, Put>(rowkey, put);
			}
		});

		logRDDcount("Consumption And Booking Proportion", result);
		result.saveAsHadoopDataset(outJobConfig);
		;
	}

	private static void saveConsumptionTag(JavaPairRDD<String, TravelUserProfileInfo> bigdataMemberInfo, JobConf outJobConfig) {
		JavaPairRDD<String, TravelUserProfileInfo> consumptionRDD = bigdataMemberInfo.filter(new Function<Tuple2<String, TravelUserProfileInfo>, Boolean>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<String, TravelUserProfileInfo> original) throws Exception {
				TravelUserProfileInfo res = original._2();
				String status = res.getStatus();
				return "已出游".equals(status);
			}
		}).cache();
		
		logRDDcount("Consumption", consumptionRDD);

		saveTravelFrequencyTag(outJobConfig, consumptionRDD);

		saveTotalConsumptionNumberTag(outJobConfig, consumptionRDD);
		saveEveryYearConsumptionNumberTag(outJobConfig, consumptionRDD);
		saveLastConsumptionTravelDateTag(outJobConfig, consumptionRDD);
	}

	private static void saveTravelFrequencyTag(JobConf outJobConfig, JavaPairRDD<String, TravelUserProfileInfo> consumptionRDD) {
		JavaPairRDD<ImmutableBytesWritable, Put> result = consumptionRDD.reduceByKey(new Function2<TravelUserProfileInfo, TravelUserProfileInfo, TravelUserProfileInfo>() {
			private static final long serialVersionUID = 1L;

			@Override
			public TravelUserProfileInfo call(TravelUserProfileInfo info1, TravelUserProfileInfo info2) throws Exception {
				List<Date> dates = info2.getDates();
				List<Date> resultDates = info1.getDates();
				for (Date date : dates) {
					resultDates.add(date);
				}
				return info1;
			}
		}).filter(new Function<Tuple2<String, TravelUserProfileInfo>, Boolean>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<String, TravelUserProfileInfo> original) throws Exception {
				return original._2().getDates().size() > 1;
			}
		}).map(new PairFunction<Tuple2<String, TravelUserProfileInfo>, ImmutableBytesWritable, Put>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<ImmutableBytesWritable, Put> call(Tuple2<String, TravelUserProfileInfo> original) throws Exception {
				TravelUserProfileInfo profileInfo = original._2();
				List<Date> dates = profileInfo.getDates();
				try{
					Collections.sort(dates);
				}catch(Exception e){
					throw new RuntimeException("rowkey: " + original._1() + "------------dates: " + dates.toString());
				}
				NumberMap travelFrequency = new NumberMap();

				Date preDate = null;
				for (Date date : dates) {
					if (preDate == null) {
						preDate = date;
					} else {
						long time = date.getTime();
						long preTime = preDate.getTime();

						long diff = time - preTime;

						if (diff < 0) {
							throw new RuntimeException("error date seq:" + original._1());
						} else if (diff >= 0 && diff < 7776000000l) {
							// 0-90天
							travelFrequency.put("[0-90)天", new BigDecimal("1"));
						} else if (diff >= 7776000000l && diff < 15552000000l) {
							// 90-180天
							travelFrequency.put("[90-180)天", new BigDecimal("1"));
						} else if (diff >= 15552000000l && diff < 31104000000l) {
							// 180-360天
							travelFrequency.put("[180-360)天", new BigDecimal("1"));
						} else if (diff >= 31104000000l && diff < 46656000000l) {
							// 360-540天
							travelFrequency.put("[360-540)天", new BigDecimal("1"));
						} else if (diff >= 46656000000l && diff < 62208000000l) {
							// 540-720天
							travelFrequency.put("[540-720)天", new BigDecimal("1"));
						} else {
							// 720天以上
							travelFrequency.put("720天以上", new BigDecimal("1"));
						}
						preDate = date;
					}
				}

				profileInfo.setTravelfrequency(travelFrequency);

				byte[] bytes = Bytes.toBytes(original._1());
				ImmutableBytesWritable rowkey = new ImmutableBytesWritable(bytes);
				Put put = new Put(bytes);
				put.add(FAMILY, TRAVEL_FREQUENCY, Bytes.toBytes(travelFrequency.getMapString()));
				return new Tuple2<ImmutableBytesWritable, Put>(rowkey, put);
			}
		});

		logRDDcount("Travel Frequency", result);

		result.saveAsHadoopDataset(outJobConfig);
	}

	private static void saveLastConsumptionTravelDateTag(JobConf outJobConfig, JavaPairRDD<String, TravelUserProfileInfo> consumptionRDD) {
		JavaPairRDD<ImmutableBytesWritable, Put> result = consumptionRDD.reduceByKey(new Function2<TravelUserProfileInfo, TravelUserProfileInfo, TravelUserProfileInfo>() {
			private static final long serialVersionUID = 1L;

			@Override
			public TravelUserProfileInfo call(TravelUserProfileInfo info1, TravelUserProfileInfo info2) throws Exception {
				Date lastTravelDate1 = info1.getLastTravelDate();
				Date lastTravelDate2 = info2.getLastTravelDate();
				if (lastTravelDate1 == null && lastTravelDate2 == null) {
					return info1;
				} else if (lastTravelDate1 != null && lastTravelDate2 == null) {
					return info1;
				} else if (lastTravelDate1 == null && lastTravelDate2 != null) {
					return info2;
				} else {
					return lastTravelDate1.after(lastTravelDate2) ? info1 : info2;
				}
			}
		}).filter(new Function<Tuple2<String, TravelUserProfileInfo>, Boolean>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<String, TravelUserProfileInfo> original) throws Exception {
				TravelUserProfileInfo info = original._2();
				return info.getLastTravelDate() != null;
			}
		}).map(new PairFunction<Tuple2<String, TravelUserProfileInfo>, ImmutableBytesWritable, Put>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<ImmutableBytesWritable, Put> call(Tuple2<String, TravelUserProfileInfo> original) throws Exception {
				byte[] bytes = Bytes.toBytes(original._1());
				Date lastTravelDate = original._2().getLastTravelDate();
				long lastTravelTime = lastTravelDate.getTime();
				long nowTime = System.currentTimeMillis();

				String beTweenWithDays = (Math.ceil((nowTime - lastTravelTime) / 86400000)) + "";
				ImmutableBytesWritable rowkey = new ImmutableBytesWritable(bytes);
				Put put = new Put(bytes);
				put.add(FAMILY, LAST_TRAVEL_DATE, Bytes.toBytes(ThreadSafeDateUtils.secFormat(lastTravelDate)));
				put.add(FAMILY, BE_TWEEN_WITH_LAST_TRAVEL_DAYS, Bytes.toBytes(beTweenWithDays));
				return new Tuple2<ImmutableBytesWritable, Put>(rowkey, put);
			}
		});

		logRDDcount("Last Consumption Travel Date", result);

		result.saveAsHadoopDataset(outJobConfig);
	}

	private static void saveTotalConsumptionNumberTag(JobConf outJobConfig, JavaPairRDD<String, TravelUserProfileInfo> consumptionRDD) {
		JavaPairRDD<ImmutableBytesWritable, Put> result = consumptionRDD.reduceByKey(new Function2<TravelUserProfileInfo, TravelUserProfileInfo, TravelUserProfileInfo>() {
			private static final long serialVersionUID = 1L;

			@Override
			public TravelUserProfileInfo call(TravelUserProfileInfo arg0, TravelUserProfileInfo arg1) throws Exception {
				arg0.setTotalConsumptionCount(arg0.getTotalConsumptionCount() + arg1.getTotalConsumptionCount());
				return arg0;
			}
		}).map(new PairFunction<Tuple2<String, TravelUserProfileInfo>, ImmutableBytesWritable, Put>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<ImmutableBytesWritable, Put> call(Tuple2<String, TravelUserProfileInfo> original) throws Exception {
				byte[] bytes = Bytes.toBytes(original._1());
				ImmutableBytesWritable rowkey = new ImmutableBytesWritable(bytes);
				Put put = new Put(bytes);
				put.add(FAMILY, TOTAL_CONSUMPTION_NUMBER, Bytes.toBytes(original._2().getTotalConsumptionCount() + ""));
				return new Tuple2<ImmutableBytesWritable, Put>(rowkey, put);
			}
		});

		logRDDcount("Total Consumption Count", result);

		result.saveAsHadoopDataset(outJobConfig);
	}

	private static void saveTotalBookingCountTag(JobConf outJobConfig, JavaPairRDD<String, TravelUserProfileInfo> consumptionRDD) {
		JavaPairRDD<ImmutableBytesWritable, Put> result = consumptionRDD.reduceByKey(new Function2<TravelUserProfileInfo, TravelUserProfileInfo, TravelUserProfileInfo>() {
			private static final long serialVersionUID = 1L;

			@Override
			public TravelUserProfileInfo call(TravelUserProfileInfo arg0, TravelUserProfileInfo arg1) throws Exception {
				Integer totalBookingCount1 = arg0.getTotalBookingCount() == null ? 0 : arg0.getTotalBookingCount();
				Integer totalBookingCount2 = arg1.getTotalBookingCount() == null ? 0 : arg1.getTotalBookingCount();

				arg0.setTotalBookingCount(totalBookingCount1 + totalBookingCount2);
				return arg0;
			}
		}).map(new PairFunction<Tuple2<String, TravelUserProfileInfo>, ImmutableBytesWritable, Put>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<ImmutableBytesWritable, Put> call(Tuple2<String, TravelUserProfileInfo> original) throws Exception {
				byte[] bytes = Bytes.toBytes(original._1());
				ImmutableBytesWritable rowkey = new ImmutableBytesWritable(bytes);
				Put put = new Put(bytes);
				put.add(FAMILY, TOTAL_BOOKING_NUMBER, Bytes.toBytes(original._2().getTotalBookingCount() + ""));
				return new Tuple2<ImmutableBytesWritable, Put>(rowkey, put);
			}
		});

		logRDDcount("Total Booking Count", result);

		result.saveAsHadoopDataset(outJobConfig);
	}

	private static void saveEveryYearConsumptionNumberTag(JobConf outJobConfig, JavaPairRDD<String, TravelUserProfileInfo> consumptionRDD) {
		JavaPairRDD<ImmutableBytesWritable, Put> result = consumptionRDD.reduceByKey(new Function2<TravelUserProfileInfo, TravelUserProfileInfo, TravelUserProfileInfo>() {

			private static final long serialVersionUID = 1L;

			@Override
			public TravelUserProfileInfo call(TravelUserProfileInfo arg0, TravelUserProfileInfo arg1) throws Exception {
				NumberMap result = new NumberMap();
				result.putAll(arg0.getEveryYearConsumptionCount());
				result.putAll(arg1.getEveryYearConsumptionCount());
				arg0.setEveryYearConsumptionCount(result);
				return arg0;
			}
		}).filter(new Function<Tuple2<String, TravelUserProfileInfo>, Boolean>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<String, TravelUserProfileInfo> original) throws Exception {
				return original._2().getEveryYearConsumptionCount() != null;
			}
		}).map(new PairFunction<Tuple2<String, TravelUserProfileInfo>, ImmutableBytesWritable, Put>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<ImmutableBytesWritable, Put> call(Tuple2<String, TravelUserProfileInfo> original) throws Exception {
				byte[] bytes = Bytes.toBytes(original._1());
				ImmutableBytesWritable rowkey = new ImmutableBytesWritable(bytes);
				Put put = new Put(bytes);

				NumberMap everyYearConsumptionNumber = original._2().getEveryYearConsumptionCount();

				put.add(FAMILY, EVERY_YEAR_CONSUMPTION_NUMBER, Bytes.toBytes(everyYearConsumptionNumber.getMapString()));
				return new Tuple2<ImmutableBytesWritable, Put>(rowkey, put);
			}
		});

		logRDDcount("Every Year Consumption", result);
		result.saveAsHadoopDataset(outJobConfig);
	}

	private static void saveBookingTag(JavaPairRDD<String, TravelUserProfileInfo> bigdataMemberInfo, JobConf outJobConfig) {
		JavaPairRDD<String, TravelUserProfileInfo> bookingRDD = bigdataMemberInfo;

		logRDDcount("Booking", bookingRDD);

		saveEveryYearBookingNumberTag(outJobConfig, bookingRDD);
		saveTotalBookingCountTag(outJobConfig, bookingRDD);
	}

	private static void saveEveryYearBookingNumberTag(JobConf outJobConfig, JavaPairRDD<String, TravelUserProfileInfo> BookingRDD) {
		JavaPairRDD<ImmutableBytesWritable, Put> result = BookingRDD.reduceByKey(new Function2<TravelUserProfileInfo, TravelUserProfileInfo, TravelUserProfileInfo>() {

			private static final long serialVersionUID = 1L;

			@Override
			public TravelUserProfileInfo call(TravelUserProfileInfo arg0, TravelUserProfileInfo arg1) throws Exception {
				NumberMap result = new NumberMap();
				result.putAll(arg0.getEveryYearBookingCount());
				result.putAll(arg1.getEveryYearBookingCount());
				arg0.setEveryYearBookingCount(result);
				return arg0;
			}
		}).filter(new Function<Tuple2<String, TravelUserProfileInfo>, Boolean>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<String, TravelUserProfileInfo> original) throws Exception {
				return original._2().getEveryYearBookingCount() != null;
			}
		}).map(new PairFunction<Tuple2<String, TravelUserProfileInfo>, ImmutableBytesWritable, Put>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<ImmutableBytesWritable, Put> call(Tuple2<String, TravelUserProfileInfo> original) throws Exception {
				byte[] bytes = Bytes.toBytes(original._1());
				ImmutableBytesWritable rowkey = new ImmutableBytesWritable(bytes);
				Put put = new Put(bytes);
				put.add(FAMILY, EVERY_YEAR_BOOKING_NUMBER, Bytes.toBytes(original._2().getEveryYearBookingCount().getMapString()));
				return new Tuple2<ImmutableBytesWritable, Put>(rowkey, put);
			}
		});
		logRDDcount("Every Year Booking", result);

		result.saveAsHadoopDataset(outJobConfig);
	}

	private static void logRDDcount(String name, JavaPairRDD<?, ?> rdd) {
		System.out.println(String.format("===========%s COUNT:%s", name, rdd.count()));
	}

}
