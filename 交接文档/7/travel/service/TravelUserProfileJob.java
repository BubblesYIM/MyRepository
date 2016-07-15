package com.jje.bigdata.userProfile.travel.service;

import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;

import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;

import com.jje.bigdata.travel.realRecommend.StreamingRealRecommendTravelLine;
import com.jje.bigdata.userProfile.travel.domain.BigdataMemberInfo;
import com.jje.bigdata.userProfile.travel.domain.NumberMap;
import com.jje.bigdata.userProfile.travel.domain.TravelOrderInfo;
import com.jje.bigdata.userProfile.travel.domain.TravelUserProfileInfo;
import com.jje.bigdata.util.BigdataMemberInfoUtils;
import com.jje.bigdata.util.WritableTableOutputFormat;

public class TravelUserProfileJob implements Serializable {
	private static final long serialVersionUID = 1L;

	private boolean debugFlag = false;
	private boolean testFlag = false;

	private String column = null;
	private TravelUserProfileReducer reducer = null;

	public TravelUserProfileJob() {
		super();
	}

	public TravelUserProfileJob(String column, TravelUserProfileReducer reducer) {
		this();
		this.column = column;
		this.reducer = reducer;
	}

	public boolean isDebugFlag() {
		return debugFlag;
	}

	public void setDebugFlag(boolean debugFlag) {
		this.debugFlag = debugFlag;
	}

	public boolean isTestFlag() {
		return testFlag;
	}

	public void setTestFlag(boolean testFlag) {
		this.testFlag = testFlag;
	}

	private void logRDDcount(String name, JavaPairRDD<?, ?> rdd) {
		if (debugFlag) {
			System.out.println(String.format("===========%s COUNT:%s", name, rdd.count()));
		}
	}

//	private void logRDDcount(String name, JavaRDD<?> rdd) {
//		if (debugFlag) {
//			System.out.println(String.format("===========%s COUNT:%s", name, rdd.count()));
//		}
//	}
//
//	private void logValue(String name, Object value) {
//		if (debugFlag) {
//			System.out.println(String.format("===========%s:%s", name, value));
//		}
//	}
//
//	private void saveValue(String name, JavaPairRDD<?, ?> rdd) {
//		if (testFlag) {
//			System.out.println(String.format("===========save to /tmp/CommonTravelUserProfileJob/" + name));
//			rdd.saveAsTextFile("/tmp/CommonGroupTravelUserProfileJob/" + name);
//		}
//	}
//
//	private void saveValue(String name, JavaRDD<?> rdd) {
//		if (testFlag) {
//			System.out.println(String.format("===========save to /tmp/CommonTravelUserProfileJob/" + name));
//			rdd.saveAsTextFile("/tmp/CommonGroupTravelUserProfileJob/" + name);
//		}
//	}

	public void run(JavaSparkContext sc) throws Exception {
		JavaPairRDD<TravelOrderInfo, BigdataMemberInfo> bigdataMemberInfo_RDD = BigdataMemberInfoUtils.getBigdataTravelMemberInfo(sc, HBaseConfiguration.create());
		final Broadcast<String> column_bd = sc.broadcast(StringUtils.defaultString(column));
		final Broadcast<TravelUserProfileReducer> reducer_bd = sc.broadcast(reducer);

		JavaPairRDD<String, TravelUserProfileInfo> keyFilter_RDD = bigdataMemberInfo_RDD
				.map(new PairFunction<Tuple2<TravelOrderInfo, BigdataMemberInfo>, String, TravelUserProfileInfo>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, TravelUserProfileInfo> call(Tuple2<TravelOrderInfo, BigdataMemberInfo> input) throws Exception {
						String column = column_bd.value();
						TravelOrderInfo order = input._1();
						BigdataMemberInfo member = input._2();
						TravelUserProfileInfo profileData = new TravelUserProfileInfo(member, order);
						if (order == null || StringUtils.isEmpty(order.getROWKEY()) || member == null || StringUtils.isEmpty(member.getROWKEY())) {
							return new Tuple2<String, TravelUserProfileInfo>(null, profileData);
						}
						String rowkey = member.getROWKEY();
						profileData.setRowkey(rowkey);
						Object data = PropertyUtils.getProperty(profileData, column);
						if (data == null) {
							return new Tuple2<String, TravelUserProfileInfo>(null, profileData);
						}
						return new Tuple2<String, TravelUserProfileInfo>(rowkey, profileData);
					}

				}).filter(new Function<Tuple2<String, TravelUserProfileInfo>, Boolean>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Boolean call(Tuple2<String, TravelUserProfileInfo> arg0) throws Exception {
						return arg0._1() != null;
					}
				});

		logRDDcount(column + "_keyFilter_RDD", keyFilter_RDD);

		JavaPairRDD<String, TravelUserProfileInfo> reduce_RDD = keyFilter_RDD.reduceByKey(
				new Function2<TravelUserProfileInfo, TravelUserProfileInfo, TravelUserProfileInfo>() {
					private static final long serialVersionUID = 1L;

					@Override
					public TravelUserProfileInfo call(TravelUserProfileInfo arg0, TravelUserProfileInfo arg1) throws Exception {
						return reducer_bd.value().process(arg0, arg1);
					}

				});

		logRDDcount(column + "_reduce_RDD", reduce_RDD);

		// JavaPairRDD<String, TravelUserProfileInfo> result_RDD =
		// reduce_RDD.filter(new Function<Tuple2<String, TravelUserProfileInfo>,
		// Boolean>() {
		//
		// @Override
		// public Boolean call(Tuple2<String, TravelUserProfileInfo> input) throws
		// Exception {
		// return reducer_bd.value().getValue(input._2()) != null;
		// }
		//
		// }).cache();
		// logRDDcount(column + "_result_RDD", result_RDD);

		Configuration outConf = HBaseConfiguration.create();
		outConf.set(TableOutputFormat.OUTPUT_TABLE, "TB_TRAVEL_USER_PROFILE");
		JobConf jobConf = new JobConf(outConf, StreamingRealRecommendTravelLine.class);
		jobConf.setOutputFormat(WritableTableOutputFormat.class);

		reduce_RDD.map(new PairFunction<Tuple2<String, TravelUserProfileInfo>, ImmutableBytesWritable, Put>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<ImmutableBytesWritable, Put> call(Tuple2<String, TravelUserProfileInfo> input) throws Exception {

				try {
					ImmutableBytesWritable row = new ImmutableBytesWritable(new byte[0]);
					TravelUserProfileInfo profileInfo = input._2();
					Put put = new Put(Bytes.toBytes(profileInfo.getRowkey()));
					String column = column_bd.value();
					Class<?> propertyType = PropertyUtils.getPropertyType(profileInfo, column);
					Object value = PropertyUtils.getProperty(profileInfo, column);
					if(value == null){
						return new Tuple2<ImmutableBytesWritable, Put>(row, null);
					}
					if (NumberMap.class.isAssignableFrom(propertyType)) {
						NumberMap dataMap = (NumberMap) value;
						String mapString = dataMap.getMapString();
						if(mapString == null){
							return new Tuple2<ImmutableBytesWritable, Put>(row, null);
						}
						put.add(Bytes.toBytes("info"), Bytes.toBytes(column), Bytes.toBytes(mapString));
//						for (Map.Entry<String, BigDecimal> entry : dataMap.entrySet()) {
//							put.add(Bytes.toBytes(column), Bytes.toBytes(entry.getKey()), Bytes.toBytes(String.valueOf(entry.getValue())));
//						}
					} else {
						put.add(Bytes.toBytes("info"), Bytes.toBytes(column), Bytes.toBytes(String.valueOf(value)));
					}
					return new Tuple2<ImmutableBytesWritable, Put>(row, put);
				} catch (Exception e) {
					StringWriter sw = new StringWriter();
					PrintWriter pw = new PrintWriter(sw);
					e.printStackTrace(pw);
					throw new RuntimeException("input[" + input + "] error:" + sw.toString(), e);
				}
			}

		}).filter(new Function<Tuple2<ImmutableBytesWritable,Put>, Boolean>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<ImmutableBytesWritable, Put> arg0) throws Exception {
				return arg0._2() != null;
			}
			
		}).saveAsHadoopDataset(jobConf);

	}

}
