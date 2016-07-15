package com.jje.bigdata.travel.similarity;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
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
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;

import com.jje.bigdata.census.TXNHotelOrderUsersPropertiesPrint;
import com.jje.bigdata.test.domain.DataCount;
import com.jje.bigdata.travel.thesaurus.TourismHbaseOp;
import com.jje.bigdata.util.Arithmetic;
import com.jje.bigdata.util.SparkUtils;
import com.jje.bigdata.util.WritableTableOutputFormat;


public class CosineSimilarity {

	private static final byte[] INFO_FARMLIY = "info".getBytes();
	private static final byte[] TAG_FARMLIY = "tag".getBytes();
	private static final byte[] LINE_FARMLIY = "line".getBytes();
	private static final byte[] EIGEN_VECTOR = "EIGEN_VECTOR".getBytes();
	
	private static JavaSparkContext sc;
	private static Map<String, String> vector1;
	
	
	public static void main(String[] args) {
		Date startDate = new Date();
		System.out.println("-------------------------startDate: " + startDate);
		try{
			
			if (args.length < 2) {
				for (String arg : args) {
					System.out.println("-------------------------------" + arg);
				}
				System.out.println("/usr/lib/ngmr/run_app lexus-1.0-SNAPSHOT.jar com.jje.bigdata.travel.similarity.CosineSimilarity ngmr-yarn-client lexus-1.0-SNAPSHOT.jar");
				System.exit(1);
			}

			String inputTable = "T_BIGDATA_TRAVEL_PRODUCT_EIGEN_VECTOR";
			String outputTable = "T_BIGDATA_TRAVEL_PRODUCT_COSINE_SIMILARITY";
			String dirctoryTable = "T_BIGDATA_TRAVEL_PRODUCT_DICTIONARY";
			Configuration inConf = SparkUtils.getConfig();
			
			TourismHbaseOp.truncateTable(outputTable, new String[]{"info","line"});
			
			Configuration outconfig = HBaseConfiguration.create();
			outconfig.set(TableOutputFormat.OUTPUT_TABLE, outputTable);
			
			JobConf jconf = new JobConf(outconfig, TXNHotelOrderUsersPropertiesPrint.class);
			jconf.setOutputFormat(WritableTableOutputFormat.class);
			
			sc = new JavaSparkContext(args[0], "CosineSimilarity", new SparkConf().setAppName("CosineSimilarity"));
			sc.addJar(args[1]);
			HTable dhtable = new HTable(inConf, dirctoryTable);
			Result result = dhtable.get(new Get(EIGEN_VECTOR));
			
			final Map<String, String> originalVectors = getOriginalMap(result.getFamilyMap(TAG_FARMLIY));
			
			
			final Broadcast<Map<String, String>> bOriginalVectors = sc.broadcast(originalVectors);
			
			dhtable.close();
			
			JavaPairRDD<ImmutableBytesWritable, Result> avaliableLineVectors = SparkUtils.getSparkRDD(sc, inConf, inputTable).filter(new Function<Tuple2<ImmutableBytesWritable,Result>, Boolean>() {
				private static final long serialVersionUID = 1L;

				@Override
				public Boolean call(Tuple2<ImmutableBytesWritable, Result> original) throws Exception {
					Result res = original._2();
					String avaliable = Bytes.toString(res.getValue(INFO_FARMLIY, "AVAILABLE".getBytes()));
					if("1".equals(avaliable)){
						return true;
					}
					return false;
				}
			});
			
			JavaRDD<LineObject> lineObjectRDD = avaliableLineVectors.map(new Function<Tuple2<ImmutableBytesWritable,Result>, LineObject>() {
				private static final long serialVersionUID = 1L;

				@Override
				public LineObject call(Tuple2<ImmutableBytesWritable, Result> original) throws Exception {
					String rowkey = Bytes.toString(original._1().get());
					NavigableMap<byte[], byte[]> familyMap = original._2().getFamilyMap(TAG_FARMLIY);
					return new LineObject(rowkey, getMap(familyMap));
				}
			});
			
			List<LineObject> lines = lineObjectRDD.collect();
			
			final Broadcast<List<LineObject>> blines = sc.broadcast(lines);
			
			
			
			avaliableLineVectors.map(new PairFunction<Tuple2<ImmutableBytesWritable, Result>, ImmutableBytesWritable, Put>() {
				private static final long serialVersionUID = 1L;

				@Override
				public Tuple2<ImmutableBytesWritable, Put> call(Tuple2<ImmutableBytesWritable, Result> original) throws Exception {
					ImmutableBytesWritable rowkey = original._1();
					String sRowKey = Bytes.toString(rowkey.get());
					Result res = original._2();
					List<LineObject> lines = blines.value();
					Map<String, String> originalMap = bOriginalVectors.value();
					
					Map<String, String> selfVectors = copyMap(originalMap);
					NavigableMap<byte[], byte[]> selfFamliy = res.getFamilyMap(TAG_FARMLIY);
					put2Map(selfVectors, selfFamliy);
					
					Put put = new Put(rowkey.get());
					List<DataCount> dataCounts = new ArrayList<DataCount>();
					
					for(LineObject dc : lines){
						String line = dc.getRowkey();
//						if("10140".equals(line)){
//							throw new RuntimeException("line: " + line + "----------sRowKey:" + sRowKey + "---------result:" + sRowKey.equals(line));
//						}
						if(!sRowKey.equals(line)){
							String lineId = dc.getRowkey();
							Map<String, String> lineFamliy = dc.getTags();
							Map<String, String> lineVectors = copyMap(originalMap);
							putString2Map(lineVectors, lineFamliy);
							
							
							double cosineSimilarity = Arithmetic.cosineSimilarity(selfVectors, lineVectors);
							DataCount dataCount = new DataCount(lineId, cosineSimilarity);
							dataCounts.add(dataCount);
						}
					}
					
					Collections.sort(dataCounts);
					
					for(int i=0; i< dataCounts.size() && i < 100; i++){
						DataCount dataCount = dataCounts.get(i);
						put.add(LINE_FARMLIY, dataCount.getData().getBytes(), dataCount.getCount().toString().getBytes());
					}
					
					return new Tuple2<ImmutableBytesWritable, Put>(rowkey, put);
				}

			}).saveAsHadoopDataset(jconf);
			
			System.out.println("avaliableLineVectors" + avaliableLineVectors.count());
			
		}catch(Exception e){
			e.printStackTrace();
			System.exit(-1);
		}finally{
			Date endDate = new Date();
			long durion = endDate.getTime() - startDate.getTime();
			System.out.println("耗时:" + durion + "毫秒");
		}
	}


	protected static void put2Map(Map<String, String> vector1, Map<byte[], byte[]> famliy) {
		for(Entry<byte[], byte[]> entry : famliy.entrySet()){
			String key = Bytes.toString(entry.getKey());
			String value = Bytes.toString(entry.getValue());
			vector1.put(key, value);
		}
	}
	
	protected static void putString2Map(Map<String, String> vector1, Map<String, String> famliy) {
		CosineSimilarity.vector1 = vector1;
		for(Entry<String, String> entry : famliy.entrySet()){
			String key = entry.getKey();
			String value = entry.getValue();
			vector1.put(key, value);
		}
	}


	private static Map<String, String> getMap(Map<byte[], byte[]> familyMap) {
		Map<String, String> result = new HashMap<String, String>();
		for(Entry<byte[], byte[]> entry : familyMap.entrySet()){
			String key = Bytes.toString(entry.getKey());
			String value = Bytes.toString(entry.getValue());
			result.put(key, value);
		}
		return result;
	}
	
	
	private static Map<String, String> getOriginalMap(Map<byte[], byte[]> familyMap) {
		Map<String, String> result = new HashMap<String, String>();
		for(Entry<byte[], byte[]> entry : familyMap.entrySet()){
			String key = Bytes.toString(entry.getKey());
			String value = Bytes.toString(entry.getValue());
			result.put(key, "0");
		}
		return result;
	}
	
	/**
	 * 复制全新的map
	 * @param original
	 * @return
	 */
	private static Map<String, String> copyMap(Map<String, String> original) {
		Map<String, String> result = new HashMap<String, String>();
		result.putAll(original);
		return result;
	}
}
