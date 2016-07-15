/**
 * 统计用户时间序列维度频次信息
 */
package com.jje.bigdata.hotel.hotelanalysis;

import com.jje.bigdata.census.ThreadSafeDateUtils;
import com.jje.bigdata.travel.analysis2.HbaseHelper;
import com.jje.bigdata.util.SparkUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
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
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;
import scala.Tuple5;

import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.*;

public class SparkStatUserTimeSeriesFreqInfo {
	static JavaSparkContext sc = null;
	static Configuration conf = null;
	
	static HTable tb_user_time_info = null;
	static String[] timeSeries = {"month","week","holiday"};
	static String[] family = new String[]{"m","w","h"};

	static String outTableName = "TB_REC_CH_USER_TIME_SERIES_FREQ_INFO";
	
	static {
		conf = HBaseConfiguration.create();
		try {
			tb_user_time_info = new HTable(conf, outTableName);
			tb_user_time_info.setAutoFlush(false);
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
	
	static void run(Date nowDate) throws Exception {
		Scan scan = new Scan();
		String tableName = "TB_RE_HOTEL_TXN_ORDER_MERGE";
		conf.set(TableInputFormat.INPUT_TABLE, tableName);
		conf.set(TableInputFormat.SCAN, SparkUtils.convertScanToString(scan));
		JavaPairRDD<ImmutableBytesWritable,Result> txnOrderMergeRDD = sc.newAPIHadoopRDD(conf,TableInputFormat.class, ImmutableBytesWritable.class,Result.class).filter(new Function<Tuple2<ImmutableBytesWritable, Result>, Boolean>() {
			@Override
			public Boolean call(Tuple2<ImmutableBytesWritable, Result> original) throws Exception {
				String date = Bytes.toString(original._2().getValue("f".getBytes(), "date".getBytes()));
				String memberID = Bytes.toString(original._2().getValue("f".getBytes(), "member_id".getBytes()));
				return !SparkUtils.isNull(date) && !SparkUtils.isNull(memberID);
			}
		}).cache();

//		SparkUtils.logRDDcount("txnOrderMergeRDD", txnOrderMergeRDD);

		Configuration outConf = HBaseConfiguration.create();
		outConf.set(TableOutputFormat.OUTPUT_TABLE, outTableName);
		outConf.set(TableInputFormat.SCAN, SparkUtils.convertScanToString(scan));
		JobConf jobConf = new JobConf(outConf,SparkStatUserTimeSeriesFreqInfo.class);
		jobConf.setOutputFormat(org.apache.hadoop.hbase.mapred.TableOutputFormat.class);

		final Broadcast<Date> bdate = sc.broadcast(nowDate);
		for(int i = 0; i < timeSeries.length; i++){

			final Broadcast<String> btimeSeries = sc.broadcast(timeSeries[i]);
			final Broadcast<Integer> bi = sc.broadcast(i);

			JavaPairRDD<ImmutableBytesWritable, Put> userTimeInfo = txnOrderMergeRDD.flatMap(new PairFlatMapFunction<Tuple2<ImmutableBytesWritable, Result>, String, Tuple5<String, String, Long, Date, Date>>() {
				@Override
				public Iterable<Tuple2<String, Tuple5<String, String, Long, Date, Date>>> call(Tuple2<ImmutableBytesWritable, Result> original) throws Exception {
					Result data = original._2();

					String memberID = Bytes.toString(data.getValue("f".getBytes(), "member_id".getBytes()));
					String timeSeries = Bytes.toString(data.getValue("f".getBytes(), btimeSeries.value().getBytes()));
					Date date = ThreadSafeDateUtils.dayParse(Bytes.toString(data.getValue("f".getBytes(), "date".getBytes())));
					List<Tuple2<String, Tuple5<String, String, Long, Date, Date>>> resultList = new ArrayList<Tuple2<String, Tuple5<String, String, Long, Date, Date>>>();
					String[] split = timeSeries.split("_");
					for (String timeSery : split) {
						resultList.add(new Tuple2<String, Tuple5<String, String, Long, Date, Date>>(memberID + "_" + timeSeries, new Tuple5<String, String, Long, Date, Date>(memberID, timeSery, 1l, date, date)));
					}
					return resultList;
				}
			}).reduceByKey(new Function2<Tuple5<String, String, Long, Date, Date>, Tuple5<String, String, Long, Date, Date>, Tuple5<String, String, Long, Date, Date>>() {
				@Override
				public Tuple5<String, String, Long, Date, Date> call(Tuple5<String, String, Long, Date, Date> leftData, Tuple5<String, String, Long, Date, Date> rightData) throws Exception {
					long count = leftData._3() + rightData._3();
					Date minDate = leftData._4().before(rightData._4()) ? leftData._4() : rightData._4();
					Date maxDate = leftData._4().after(rightData._4()) ? leftData._4() : rightData._4();
					return new Tuple5<String, String, Long, Date, Date>(leftData._1(), leftData._2(), count, minDate, maxDate);
				}
			}).map(new PairFunction<Tuple2<String, Tuple5<String, String, Long, Date, Date>>, ImmutableBytesWritable, Put>() {
				@Override
				public Tuple2<ImmutableBytesWritable, Put> call(Tuple2<String, Tuple5<String, String, Long, Date, Date>> original) throws Exception {
					Tuple5<String, String, Long, Date, Date> data = original._2();
					String memberID = data._1();
					String timeSery = data._2();
					Long count = data._3();
					Date minDate = data._4();
					Date maxDate = data._5();
					int i = bi.value();

					double avgCount = getAvgCount(i, count, minDate, maxDate, bdate.value());
//					if (true)
//						throw new RuntimeException("================" + tb_user_time_info);
					byte[] bmemberID = memberID.getBytes();
					Result result = tb_user_time_info.get(new Get(bmemberID));
					String avg = Bytes.toString(result.getValue(family[i].getBytes(), timeSery.getBytes()));
					if (SparkUtils.isBlank(avg) || Math.abs(Double.parseDouble(avg) - avgCount) > 0) {
						ImmutableBytesWritable key = new ImmutableBytesWritable(bmemberID);
						Put put = new Put(bmemberID);
						put.add(family[i].getBytes(), timeSery.getBytes(), String.valueOf(avgCount).getBytes());
						return new Tuple2<ImmutableBytesWritable, Put>(key, put);
					}
					return null;
				}
			}).filter(new Function<Tuple2<ImmutableBytesWritable, Put>, Boolean>() {
				@Override
				public Boolean call(Tuple2<ImmutableBytesWritable, Put> original) throws Exception {
					return original != null;
				}
			});

			SparkUtils.logRDDcount("userTimeInfo", userTimeInfo);

			userTimeInfo.saveAsHadoopDataset(jobConf);
		}

	}
	
	static double getAvgCount(int type, double count, Date minDateStr, Date maxDateStr, Date nowDate) throws SQLException, ParseException {
		double avgCount = 0;

		long minSec = minDateStr.getTime();
		long maxSec = maxDateStr.getTime();
		
		double diffOfYear = 1f;
		if(count == 1) {
			long timeLess = nowDate.getTime() - minSec;
			diffOfYear = (int) (timeLess/31536000000.0) + 2; // 604800000 7天的毫秒数   31536000000l 365天的毫秒数
		}else {
			long timeLess = maxSec - minSec;
			diffOfYear = (int) (timeLess/31536000000.0) + 1;
		}
		
		if(type==0 || type==2) {
			avgCount = count / diffOfYear;
		}else if(type == 1) {
			avgCount = count / (diffOfYear*52);
		}
//		if(true)
//			throw new RuntimeException(minDateStr.getTime() + "=============" + maxDateStr.getTime() + "================" + diffOfYear + "===========" + count + "===============" + avgCount + "=============" + nowDate.getTime());
		return avgCount;
	}
	
	public static void main(String[] args) throws Exception {
		System.out.println("-------------------------startDate: " + new Date());
		if (args.length < 2) {
			System.out.println("/usr/lib/ngmr/run_app_min /home/member/bigdata/jarvis/lexus-1.0-SNAPSHOT.jar com.jje.bigdata.hotel.hotelanalysis.SparkStatUserTimeSeriesFreqInfo ngmr-yarn-client /home/member/bigdata/jarvis/lexus-1.0-SNAPSHOT.jar");
			System.exit(1);
		}
		sc = new JavaSparkContext(args[0], "SparkStatUserTimeSeriesFreqInfo",new SparkConf());
		sc.addJar(args[1]);
		Date nowDate = new Date();
		HBaseAdmin hBaseAdmin = null;
		try{
			hBaseAdmin = new HBaseAdmin(conf);
			if(!hBaseAdmin.tableExists(outTableName)){
				System.out.println("Already Re-Created Table " + outTableName);
				HbaseHelper.createTable(outTableName, family);
			}
			tb_user_time_info = new HTable(conf, outTableName);
			tb_user_time_info.setAutoFlush(false);
			run(nowDate);
		}finally{
			if(hBaseAdmin!=null)
				hBaseAdmin.close();
		}
		System.out.println("-------------------------endDate: " + new Date());
	}
}

