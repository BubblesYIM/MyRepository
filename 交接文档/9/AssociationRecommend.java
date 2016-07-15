package com.jje.bigdata.travel.recommend;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;

import com.jje.bigdata.census.TXNHotelOrderUsersPropertiesPrint;
import com.jje.bigdata.census.ThreadSafeDateUtils;
import com.jje.bigdata.travel.behior.domain.UserBehaviorDto;
import com.jje.bigdata.travel.bigdata.service.UserBehaviorUtils;
import com.jje.bigdata.travel.thesaurus.TourismHbaseOp;
import com.jje.bigdata.util.SceneConfig;
import com.jje.bigdata.util.SparkUtils;
import com.jje.bigdata.util.WritableTableOutputFormat;

public class AssociationRecommend {
	private static final byte[] LINE_FAMILY_INFO = "line".getBytes();

	
	private static JavaSparkContext sc;
	

	public static void main(String[] args) {
		// 1420041600000 2015-01-01
		Date startDate = new Date();
		System.out.println("-------------------------startDate: " + startDate);
		try {

			if (args.length < 1) {
				for (String arg : args) {
					System.out.println("-------------------------------" + arg);
				}
				System.out.println("/usr/lib/ngmr/run_app lexus-1.0-SNAPSHOT.jar com.jje.bigdata.travel.recommend.AssociationRecommend ngmr-yarn-client lexus-1.0-SNAPSHOT.jar");
				System.exit(1);
			}

			Configuration inConf = SparkUtils.getConfig();

			sc = new JavaSparkContext(args[0], "CosineSimilarity", new SparkConf().setAppName("CosineSimilarity"));
			sc.addJar(args[1]);
			
			Properties properties = SceneConfig.create().getPara("association_recommend_config");
			Integer split_key = Integer.parseInt(properties.getProperty("split_key"));
			
			final Broadcast<Integer> splitMonth = sc.broadcast(split_key);
			
			
			JavaPairRDD<String, UserBehaviorDto> userBehavior = UserBehaviorUtils.getUserBehavior(sc, inConf);
			
			JavaRDD<String> data = userBehavior.map(new PairFunction<Tuple2<String, UserBehaviorDto>, String, String>() {
				private static final long serialVersionUID = 1L;

				@Override
				public Tuple2<String, String> call(Tuple2<String, UserBehaviorDto> original) throws Exception {
					String rowkey = original._1();
					Map<String, String> lines =  original._2().getMap();
					String[] split = rowkey.split("#");
					if(split==null || split.length!=2){
						throw new RuntimeException("rowkey error,rowkey is \"" + rowkey + "\"") ;
					}
					String month = split[0];
					String memberId = split[1];
					
					int monthSpace = ThreadSafeDateUtils.getMonthSpace(new Date(), ThreadSafeDateUtils.monthParse(month));
					
					Integer integer = splitMonth.value();
					
					int mark = (int)Math.floor(monthSpace / integer) + 1;
					
					StringBuffer value = new StringBuffer();
					
					for(String key : lines.keySet()){
						value.append(key);
						value.append(" ");
					}
					
					String newRowkey = mark + "#" + memberId;
					return new Tuple2<String, String>(newRowkey, value.toString().trim());
				}
			}).reduceByKey(new Function2<String, String, String>() {
				
				private static final long serialVersionUID = 1L;

				@Override
				public String call(String arg1, String arg2) throws Exception {
					return arg1 + " " + arg2;
				}
			}).map(new Function<Tuple2<String,String>, String>() {
				private static final long serialVersionUID = 1L;

				@Override
				public String call(Tuple2<String, String> original) throws Exception {
					return original._2().trim();
				}
			}).filter(new Function<String, Boolean>() {
				
				private static final long serialVersionUID = 1L;

				@Override
				public Boolean call(String arg0) throws Exception {
					String[] split = arg0.split(" ");
					if(split==null || split.length < 2){
						return false;
					}
					
//					double random = Math.random();
//					
//					if(random > 0.3){
//						return false;
//					}
					return true;
				}
			});	
			
			final int K = 2;
			
			JavaPairRDD<Tuple2<String, String>, Integer> flatMap = data.flatMap(new FlatMapFunction<String, Tuple2<String, String>>() {
				private static final long serialVersionUID = 1L;

				@Override
				public Iterable<Tuple2<String, String>> call(String original) throws Exception {
					List<Tuple2<String, String>> list = new ArrayList<Tuple2<String, String>>();
					String[] split = original.split(" ");
					for(int i=0; i<split.length; i++){
						for(int j=0; j<split.length; j++){
							Tuple2<String, String> tuple = new Tuple2<String, String>(split[i], split[j]);
							list.add(tuple);
						}
					}
					return list;
				}
			}).map(new PairFunction<Tuple2<String, String>, Tuple2<String, String>, Integer>() {
				private static final long serialVersionUID = 1L;

				@Override
				public Tuple2<Tuple2<String, String>, Integer> call(Tuple2<String, String> original) throws Exception {
					return new Tuple2<Tuple2<String, String>, Integer>(original, 1);
				}
			}).reduceByKey(new Function2<Integer, Integer, Integer>() {
				private static final long serialVersionUID = 1L;

				@Override
				public Integer call(Integer arg0, Integer arg1) throws Exception {
					return arg0 + arg1;
				}
			}).filter(new Function<Tuple2<Tuple2<String,String>,Integer>, Boolean>() {
				private static final long serialVersionUID = 1L;

				@Override
				public Boolean call(Tuple2<Tuple2<String, String>, Integer> original) throws Exception {
					if(original._1()._1().equals(original._1()._2())){
						return false;
					}
					return original._2() >= K;
				}
			});
			
			Configuration outconfig = HBaseConfiguration.create();
			outconfig.set(TableOutputFormat.OUTPUT_TABLE, "TB_REC_CH_TRAVEL_LINE_RELATIVITY");
			
			JobConf jconf = new JobConf(outconfig, TXNHotelOrderUsersPropertiesPrint.class);
			jconf.setOutputFormat(WritableTableOutputFormat.class);
			
			TourismHbaseOp.truncateTable("TB_REC_CH_TRAVEL_LINE_RELATIVITY", new String[]{"info", "line"});
			
			flatMap.map(new PairFunction<Tuple2<Tuple2<String, String>, Integer>, ImmutableBytesWritable, Put>() {
				private static final long serialVersionUID = 1L;

				@Override
				public Tuple2<ImmutableBytesWritable, Put> call(Tuple2<Tuple2<String, String>, Integer> original) throws Exception {
					byte[] key = original._1()._1().getBytes();
					byte[] val = original._1()._2().getBytes();
					byte[] count = original._2().toString().getBytes();
					ImmutableBytesWritable rowkey = new ImmutableBytesWritable(key);
					Put put = new Put(key);
					put.add(LINE_FAMILY_INFO, val, count);
					return new Tuple2<ImmutableBytesWritable, Put>(rowkey, put);
				}
			}).saveAsHadoopDataset(jconf);
			
//			.reduce(new Function2<MyCollectionMap, MyCollectionMap, MyCollectionMap>() {
//				private static final long serialVersionUID = 1L;
//
//				@Override
//				public MyCollectionMap call(MyCollectionMap arg0, MyCollectionMap arg1) throws Exception {
//					,
//					arg0.putAll(arg1);
//					return arg0;
//				}
//			});
			
			System.out.println("is done!");
		
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		} finally {
			Date endDate = new Date();
			long durion = endDate.getTime() - startDate.getTime();
			System.out.println("耗时:" + durion + "毫秒");
		}
	}
}
