package com.jje.bigdata.userProfile.hotel;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

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
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import com.jje.bigdata.census.ThreadSafeDateUtils;
import com.jje.bigdata.travel.bigdata.service.HotelService;
import com.jje.bigdata.travel.bigdata.service.MemberService;
import com.jje.bigdata.travel.realRecommend.StreamingRealRecommendTravelLine;
import com.jje.bigdata.userProfile.hotel.domain.HotelOrderInfo;
import com.jje.bigdata.userProfile.hotel.domain.TempHotelMemberInfo;
import com.jje.bigdata.userProfile.travel.domain.BigdataMemberInfo;
import com.jje.bigdata.util.SparkUtils;
import com.jje.bigdata.util.WritableTableOutputFormat;

public class JoinHotelUserProfileJob {

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

		System.out.println("============================JoinHotelUserProfileJob Main开始" + new Date() + "=============================");
		if (args.length < 3) {
			System.out.println("/usr/lib/ngmr/run_app lexus-1.0-SNAPSHOT.jar com.jje.bigdata.userProfile.hotel.JoinHotelUserProfileJob ngmr-yarn-client ./lexus-1.0-SNAPSHOT.jar 1500 true true");
			System.exit(1);
		}
		SparkConf sparkConf = new SparkConf();
		// sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		sc = new JavaSparkContext(args[0], "JoinHotelUserProfileJob", sparkConf);

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

		System.out.println("============================JoinHotelUserProfileJob Main结束" + new Date() + "=============================");
	}

	private static void run() throws IOException {

		// order原始数据
		JavaPairRDD<String, HotelOrderInfo> orderRDD = HotelService.getBigdataOrderRDD(sc, inConf, partitionSize);

		SparkUtils.logRDDcount("orderRDD", orderRDD, debugFlag);

		// member数据
		JavaPairRDD<String, TempHotelMemberInfo> memberRDD = MemberService.getBigdataMemberPairRDD(sc, inConf, partitionSize).map(new PairFunction<Tuple2<String, BigdataMemberInfo>, String, TempHotelMemberInfo>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, TempHotelMemberInfo> call(Tuple2<String, BigdataMemberInfo> original) throws Exception {
				TempHotelMemberInfo result = new TempHotelMemberInfo();
				result.setMember(original._2());
				return new Tuple2<String, TempHotelMemberInfo>(original._1(), result);
			}
		});// .cache();

		SparkUtils.logRDDcount("memberRDD", memberRDD, debugFlag);

		// order数据
		JavaPairRDD<String, TempHotelMemberInfo> tmpOrderMemberRDD = orderRDD.map(new PairFunction<Tuple2<String, HotelOrderInfo>, String, TempHotelMemberInfo>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, TempHotelMemberInfo> call(Tuple2<String, HotelOrderInfo> original) throws Exception {
				TempHotelMemberInfo result = new TempHotelMemberInfo();
				result.setOrder(original._2());
				return new Tuple2<String, TempHotelMemberInfo>(original._1(), result);
			}
		});

		tmpOrderMemberRDD = joinTowRDD(tmpOrderMemberRDD, "CRM_TXN_MEMBER_ID", memberRDD, "MEMBER_ID", "\\|");
		tmpOrderMemberRDD = joinTowRDD(tmpOrderMemberRDD, "CRM_TXN_X_CONTACT_ID", memberRDD, "PERSON_UID", "\\|");

		SparkUtils.logRDDcount("tmpOrderMemberRDD count", tmpOrderMemberRDD, debugFlag);
		
		tmpOrderMemberRDD = tmpOrderMemberRDD.filter(new Function<Tuple2<String,TempHotelMemberInfo>, Boolean>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<String, TempHotelMemberInfo> original) throws Exception {
				BigdataMemberInfo member = original._2().getMember();
				return original!=null && member!=null;
			}
		});

		SparkUtils.logRDDcount("tmpOrderMemberRDD count", tmpOrderMemberRDD, debugFlag);
		JavaPairRDD<HotelOrderInfo, BigdataMemberInfo> orderMemberRDD = tmpOrderMemberRDD.map(new PairFunction<Tuple2<String, TempHotelMemberInfo>, HotelOrderInfo, BigdataMemberInfo>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<HotelOrderInfo, BigdataMemberInfo> call(Tuple2<String, TempHotelMemberInfo> original) throws Exception {
				return new Tuple2<HotelOrderInfo, BigdataMemberInfo>(original._2().getOrder(), original._2().getMember());
			}
		});
		
		
		SparkUtils.logRDDcount("orderMemberRDD count:", orderMemberRDD, debugFlag);
		
		saveDataToHbase(orderMemberRDD);
		// save中间数据至hbase
	}

	private static JavaPairRDD<String, TempHotelMemberInfo> joinTowRDD(JavaPairRDD<String, TempHotelMemberInfo> leftRDD, final String leftFieldName, JavaPairRDD<String, TempHotelMemberInfo> rightRDD, final String rightFieldName, String rightSpiltor) {
		JavaPairRDD<String, TempHotelMemberInfo> leftFieldRDD = getLeftFieldRDD(leftRDD, leftFieldName, null);
		
		SparkUtils.logRDDcount(String.format("leftRDD data count:[];args:leftFieldName[%s]", leftFieldName), leftFieldRDD, debugFlag);
		JavaPairRDD<String, TempHotelMemberInfo> rightFieldRDD = getRightFieldRDD(rightRDD, rightFieldName, rightSpiltor);
		SparkUtils.logRDDcount(String.format("rightRDD data count:[];args:leftFieldName[%s]:rightSpiltor[%s]", rightFieldName,rightSpiltor), rightFieldRDD, debugFlag);
		
		JavaPairRDD<String, TempHotelMemberInfo> distinctRightFieldRDD = rightFieldRDD.reduceByKey(new Function2<TempHotelMemberInfo, TempHotelMemberInfo, TempHotelMemberInfo>(){
			private static final long serialVersionUID = 1L;

			@Override
			public TempHotelMemberInfo call(TempHotelMemberInfo arg0, TempHotelMemberInfo arg1) throws Exception {
				return arg0;
			}
		}).partitionBy(new HashPartitioner(partitionSize*2));
		
		SparkUtils.logRDDcount("distinctRightFieldRDD data count:", distinctRightFieldRDD, debugFlag);
		
//		JavaRDD<String> rightKeys = rightFieldRDD.keys().distinct();
//		SparkUtils.logRDDcount("rightKeys data count:", rightKeys, debugFlag);
		
		
		JavaPairRDD<String, TempHotelMemberInfo> unionRDD = leftFieldRDD.union(distinctRightFieldRDD);
		SparkUtils.logRDDcount("unionRDD data count:", unionRDD, debugFlag);
		
		JavaPairRDD<String, List<TempHotelMemberInfo>> groupByKey = unionRDD.groupByKey(new HashPartitioner(partitionSize*2));
		SparkUtils.logRDDcount("groupByKey data count:", groupByKey, debugFlag);
		
		JavaPairRDD<String, List<TempHotelMemberInfo>> mergeRDD = groupByKey.map(new PairFunction<Tuple2<String, List<TempHotelMemberInfo>>, String, List<TempHotelMemberInfo>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, List<TempHotelMemberInfo>> call(Tuple2<String, List<TempHotelMemberInfo>> original) throws Exception {
				List<TempHotelMemberInfo> orderInfo = new ArrayList<TempHotelMemberInfo>();
				List<TempHotelMemberInfo> memberInfo = new ArrayList<TempHotelMemberInfo>();

				List<TempHotelMemberInfo> joined = new ArrayList<TempHotelMemberInfo>();
				
				List<TempHotelMemberInfo> result = new ArrayList<TempHotelMemberInfo>();
				
				List<TempHotelMemberInfo> originalGroupInfo = original._2();
				
				//将所有的数据按照时候连接上进行分割
				for(TempHotelMemberInfo originalInfo : originalGroupInfo){
					HotelOrderInfo order = originalInfo.getOrder();
					BigdataMemberInfo member = originalInfo.getMember();
					if(order!=null && member!=null){
						joined.add(originalInfo);
					}else if(order!=null){
						orderInfo.add(originalInfo);
					}else if(member!=null){
						memberInfo.add(originalInfo);
					}else{
						throw new RuntimeException(String.format("error data:[%s];args:leftFieldName[%s]:rightFieldName[%s]", original._1(), leftFieldName, rightFieldName));
					}
				}
				
				//查看所有数据中是否有用户信息
				TempHotelMemberInfo someMember = null;
				if(!memberInfo.isEmpty()){
					someMember = memberInfo.get(0);
				}else if(!joined.isEmpty()){
					someMember = joined.get(0);
				}
				
				//没有用户信息,直接把数据原样返回
				if(someMember == null){
					return original;
				}
				
				//有用户信息,将所有在同一个主键的值,数据进行拷贝
				result.addAll(joined);
				for(TempHotelMemberInfo order : orderInfo){
					order.setMember(someMember.getMember());
					result.add(order);
				}
				
				return new Tuple2<String, List<TempHotelMemberInfo>>(original._1(), result);
			}
		}).filter(new Function<Tuple2<String,List<TempHotelMemberInfo>>, Boolean>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<String, List<TempHotelMemberInfo>> original) throws Exception {
				return !original._2().isEmpty();
			}
		});
		
		SparkUtils.logRDDcount("mergeRDD data count:", mergeRDD, debugFlag);
		
		JavaPairRDD<String, TempHotelMemberInfo> flatMergeRDD = mergeRDD.flatMap(new PairFlatMapFunction<Tuple2<String, List<TempHotelMemberInfo>>, String, TempHotelMemberInfo>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<Tuple2<String, TempHotelMemberInfo>> call(Tuple2<String, List<TempHotelMemberInfo>> original) throws Exception {
				List<Tuple2<String, TempHotelMemberInfo>> list = new ArrayList<Tuple2<String, TempHotelMemberInfo>>();
				for (TempHotelMemberInfo info : original._2()) {
					list.add(new Tuple2<String, TempHotelMemberInfo>(original._1(), info));
				}
				return list;
			}
		}).partitionBy(new HashPartitioner(partitionSize));
		
		SparkUtils.logRDDcount("flatMergeRDD data count:", flatMergeRDD, debugFlag);
		return flatMergeRDD;
	}

	private static JavaPairRDD<String, TempHotelMemberInfo> getLeftFieldRDD(JavaPairRDD<String, TempHotelMemberInfo> leftRDD, final String leftFieldName, String spiltor) {
		return leftRDD.map(new PairFunction<Tuple2<String, TempHotelMemberInfo>, String, TempHotelMemberInfo>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, TempHotelMemberInfo> call(Tuple2<String, TempHotelMemberInfo> original) throws Exception {
				TempHotelMemberInfo info = original._2();
				HotelOrderInfo order = info.getOrder();
				String fieldValue = SparkUtils.getFieldData(order, leftFieldName);
				if (order == null || SparkUtils.isBlank(fieldValue)) {
					return new Tuple2<String , TempHotelMemberInfo>(UUID.randomUUID().toString(), info);
				}
				return new Tuple2<String, TempHotelMemberInfo>(StringUtils.reverse(fieldValue), info);
			}

		}).partitionBy(new HashPartitioner(partitionSize*2));
	}
	
	private static JavaPairRDD<String, TempHotelMemberInfo> getRightFieldRDD(JavaPairRDD<String, TempHotelMemberInfo> right, final String rightFieldName, final String spiltor) {
		
		return right.flatMap(new PairFlatMapFunction<Tuple2<String, TempHotelMemberInfo>, String, TempHotelMemberInfo>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<Tuple2<String, TempHotelMemberInfo>> call(Tuple2<String, TempHotelMemberInfo> original) throws Exception {
				TempHotelMemberInfo info = original._2();
				BigdataMemberInfo member = info.getMember();
				List<Tuple2<String, TempHotelMemberInfo>> result = new ArrayList<Tuple2<String,TempHotelMemberInfo>>();
				
				
				String fieldValue = SparkUtils.getFieldData(member, rightFieldName);
				if (member == null || SparkUtils.isBlank(fieldValue)) {
					result.add(new Tuple2<String , TempHotelMemberInfo>(UUID.randomUUID().toString(), info));
					return result;
				}
//				throw new RuntimeException("\"" + fieldValue + "\"");
				
				if(SparkUtils.isNotBlank(spiltor)){
					String[] fields = fieldValue.split(spiltor);
					for (String field : fields) {
						result.add(new Tuple2<String, TempHotelMemberInfo>(StringUtils.reverse(field), info));
					}
				}else{
					result.add(new Tuple2<String, TempHotelMemberInfo>(StringUtils.reverse(fieldValue), info));
				}
				return result;
			}
		}).partitionBy(new HashPartitioner(partitionSize*2));
		
//		return leftRDD.map(new PairFunction<Tuple2<String, TempHotelMemberInfo>, String, TempHotelMemberInfo>() {
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public Tuple2<String, TempHotelMemberInfo> call(Tuple2<String, TempHotelMemberInfo> original) throws Exception {
//				TempHotelMemberInfo info = original._2();
//				BigdataMemberInfo member = info.getMember();
//				String fieldValue = SparkUtils.getFieldData(member, rightFieldName);
//				if (member == null || SparkUtils.isBlank(fieldValue)) {
//					return new Tuple2<String , TempHotelMemberInfo>(UUID.randomUUID().toString(), info);
//				}
////				throw new RuntimeException("\"" + fieldValue + "\"");
//				return new Tuple2<String, TempHotelMemberInfo>(StringUtils.reverse(fieldValue), info);
//			}
//
//		}).partitionBy(new HashPartitioner(partitionSize));
	}

	private static void saveDataToHbase(JavaPairRDD<HotelOrderInfo, BigdataMemberInfo> orderMemberRDD) {
		outConf.set(TableOutputFormat.OUTPUT_TABLE, "TB_HOTEL_USER_PROFILE_DATA_TMP");
		JobConf jobConf = new JobConf(outConf, StreamingRealRecommendTravelLine.class);
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


	
}
