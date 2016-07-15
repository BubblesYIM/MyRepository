package com.jje.bigdata.userProfile.app;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import com.google.common.base.Optional;
import com.jje.bigdata.travel.bigdata.service.MobileService;
import com.jje.bigdata.travel.realRecommend.StreamingRealRecommendTravelLine;
import com.jje.bigdata.userProfile.app.domain.AddressDto;
import com.jje.bigdata.userProfile.app.utils.AddressUtils;
import com.jje.bigdata.util.SparkUtils;
import com.jje.bigdata.util.WritableTableOutputFormat;

public class InitMobileUserLocationJob implements Serializable {
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
			System.out.println("/usr/lib/ngmr/run_app_min lexus-1.0-SNAPSHOT.jar com.jje.bigdata.userProfile.app.InitMobileUserLocationJob ngmr-yarn-client ./lexus-1.0-SNAPSHOT.jar false false");
			System.exit(1);
		}
		sc = new JavaSparkContext(args[0], "InitMobileUserLocationJob", new SparkConf());

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
		outConf.set(TableOutputFormat.OUTPUT_TABLE, "TB_REC_BIGDATA_LOCALTION_DATA");
		JobConf jobConf = new JobConf(outConf, StreamingRealRecommendTravelLine.class);
		jobConf.setOutputFormat(WritableTableOutputFormat.class);
		JavaPairRDD<String, Map<String, String>> locationRDD = MobileService.getLocationRDD(sc, inConf);
		
		JavaRDD<String> mobileLocationRDD = MobileService.getMobileLocationRDD(sc, inConf).map(new PairFunction<String, String, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, String> call(String arg0) throws Exception {
				return new Tuple2<String, String>(arg0, null);
			}
		}).leftOuterJoin(locationRDD).filter(new Function<Tuple2<String,Tuple2<String,Optional<Map<String,String>>>>, Boolean>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<String, Tuple2<String, Optional<Map<String, String>>>> original) throws Exception {
				Map<String, String> map = original._2()._2().orNull();
				return map==null;
			}
		}).map(new Function<Tuple2<String,Tuple2<String,Optional<Map<String,String>>>>, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public String call(Tuple2<String, Tuple2<String, Optional<Map<String, String>>>> original) throws Exception {
				return original._1();
			}
		});
		
		SparkUtils.logRDDcount("mobileLocationRDD", mobileLocationRDD);
		
		mobileLocationRDD.map(new PairFunction<String, ImmutableBytesWritable, Put>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<ImmutableBytesWritable, Put> call(String location) {
				byte[] locations = Bytes.toBytes(location);
				ImmutableBytesWritable rowkey = new ImmutableBytesWritable(locations);
				Put put = new Put(locations);
				try{
					AddressDto address = AddressUtils.getCityByLocationString(location);
					SparkUtils.Object2Put(put, address, "info", null, AddressDto.class);
				}catch(Exception e){
					return null;
				}
				return new Tuple2<ImmutableBytesWritable, Put>(rowkey, put);
			}
		}).filter(new Function<Tuple2<ImmutableBytesWritable,Put>, Boolean>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<ImmutableBytesWritable, Put> original) throws Exception {
				return original!=null && !original._2().isEmpty();
			}
		}).saveAsHadoopDataset(jobConf);
	}
	
}
