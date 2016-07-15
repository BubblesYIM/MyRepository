package com.jje.bigdata.userProfile.travel;

import java.io.IOException;
import java.io.Serializable;
import java.util.Date;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.jje.bigdata.util.SparkUtils;

public class BigdataMemberMappings implements Serializable {
	private static final long serialVersionUID = 1L;
	
	private static JavaSparkContext sc;
	private static SparkConf sparkConf;
	private static Configuration conf;

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
		System.out.println("/usr/lib/ngmr/run_app_min lexus-1.0-SNAPSHOT.jar com.jje.bigdata.userProfile.travel.BigdataMemberMappings ngmr-yarn-client lexus-1.0-SNAPSHOT.jar");
		try {
			sparkConf = new SparkConf();

			sc = new JavaSparkContext(args[0], "TravelUserConsumptionProfile", sparkConf);
			sc.addJar(args[1]);
			
			JavaPairRDD<String, Map<String, String>> bigDataMemberRDD = SparkUtils.getHbaseMapRDD(sc, conf, "T_BIGDATA_MEMBER","info");
			SparkUtils.saveMappingRDD(bigDataMemberRDD, "T_TB_EMAIL_TO_BIGDATA_MEMBER_MAPPING", "EMAIL", "info", "BIGDATA_MEMBER_IDS", "[|]");
			SparkUtils.saveMappingRDD(bigDataMemberRDD, "T_TB_PHONE_TO_BIGDATA_MEMBER_MAPPING", "PHONE", "info", "BIGDATA_MEMBER_IDS", "[|]");
			SparkUtils.saveMappingRDD(bigDataMemberRDD, "T_TB_NAME_TO_BIGDATA_MEMBER_MAPPING", "NAME", "info", "BIGDATA_MEMBER_IDS", "[|]");
			SparkUtils.saveMappingRDD(bigDataMemberRDD, "T_TB_MEMBER_ID_TO_BIGDATA_MEMBER_MAPPING", "MEMBER_ID", "info", "BIGDATA_MEMBER_IDS", "[|]");
			SparkUtils.saveMappingRDD(bigDataMemberRDD, "T_TB_ID_TO_BIGDATA_MEMBER_MAPPING", "ID", "info", "BIGDATA_MEMBER_IDS", "[|]");
			SparkUtils.saveMappingRDD(bigDataMemberRDD, "T_TB_MC_CODE_TO_BIGDATA_MEMBER_MAPPING", "MC_CODE", "info", "BIGDATA_MEMBER_IDS", "[|]");
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

}
