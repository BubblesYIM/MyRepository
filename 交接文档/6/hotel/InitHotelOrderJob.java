package com.jje.bigdata.userProfile.hotel;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.PageFilter;
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

import scala.Tuple2;

import com.google.common.base.Optional;
import com.jje.bigdata.travel.realRecommend.StreamingRealRecommendTravelLine;
import com.jje.bigdata.userProfile.hotel.domain.HotelOrderInfo;
import com.jje.bigdata.userProfile.travel.domain.BigdataMemberInfo;
import com.jje.bigdata.userProfile.travel.domain.TravelOrderInfo;
import com.jje.bigdata.util.SparkUtils;
import com.jje.bigdata.util.WritableTableOutputFormat;

public class InitHotelOrderJob {

	private static JavaSparkContext sc = null;
	private static Configuration inConf = null;
	private static Configuration outConf = null;

	private static boolean debugFlag = false;
	private static boolean testFlag = false;
	private static Integer partitionSize = null;
	private static Long pageSize = null;

	public static void main(String[] args) throws Exception {

		System.out.println("============================InitHotelOrderJob Main开始" + new Date() + "=============================");
		if (args.length < 3) {
			System.out.println("/usr/lib/ngmr/run_app lexus-1.0-SNAPSHOT.jar com.jje.bigdata.userProfile.hotel.InitHotelOrderJob ngmr-yarn-client ./lexus-1.0-SNAPSHOT.jar false false true");
			System.exit(1);
		}
		
		System.out.println("#############args:" + Arrays.asList(args));

		if (args.length >= 3) {
			debugFlag = Boolean.parseBoolean(args[2]);
			System.out.println("###############debugFlag = " + debugFlag);
		}

		if (args.length >= 4) {
			testFlag = Boolean.parseBoolean(args[3]);
			System.out.println("###############testFlag = " + testFlag);
		}
		
		if (args.length >= 5) {
			partitionSize = new Integer(args[4]);
			System.out.println("###############partitionSize = " + partitionSize);
		}
		
		if (args.length >= 6) {
			pageSize = new Long(args[5]);
			System.out.println("###############pageSize = " + pageSize);
		}
		
		inConf = HBaseConfiguration.create();
		outConf = HBaseConfiguration.create();
		
		SparkConf conf = new SparkConf();
		sc = new JavaSparkContext(args[0], "InitHotelOrderJob", conf);
		sc.addJar(args[1]);

		run();

		System.out.println("============================InitHotelOrderJob Main结束" + new Date() + "=============================");
	}

	private static void run() throws IOException {

		JavaPairRDD<String, HotelOrderInfo> receptionOrderRDD = getOrderRDDFromHdfs("PMS_HT_Reception_MAP", "PMS_REC_", false, "PMS_REC_ROWKEY");
		JavaPairRDD<String, HotelOrderInfo> resvOrderRDD = getOrderRDDFromHdfs("PMS_HT_Resv_MAP", "PMS_RESV_", false, "PMS_RESV_ROWKEY");
		JavaPairRDD<String, HotelOrderInfo> resvDetailOrderRDD = getOrderRDDFromHdfs("PMS_HT_ResvDetail_MAP", "PMS_DETAIL_", false, "PMS_DETAIL_ROWKEY");
		JavaPairRDD<String, HotelOrderInfo> hbpOrderRDD = getOrderRDDFromHdfs("JJ000_WEBSITE_T_HBP_ORDER_MAP", "HBP_ORDER_", false, "HBP_ORDER_ROWKEY");
		JavaPairRDD<String, HotelOrderInfo> resvIdOrderDataRDD = null;
		resvIdOrderDataRDD = getFieldOrderRDD(receptionOrderRDD, "PMS_REC_ResvId");
		resvIdOrderDataRDD = resvIdOrderDataRDD.union(getFieldOrderRDD(resvOrderRDD, "PMS_RESV_ROWKEY"));
		resvIdOrderDataRDD = resvIdOrderDataRDD.union(getFieldOrderRDD(resvDetailOrderRDD, "PMS_DETAIL_ResvId"));
		resvIdOrderDataRDD = resvIdOrderDataRDD.union(getFieldOrderRDD(hbpOrderRDD, "HBP_ORDER_ResvId"));
		JavaPairRDD<String, HotelOrderInfo> orderRDD = reduceRDD(resvIdOrderDataRDD);
		JavaPairRDD<String, HotelOrderInfo> recIdOrderDataRDD = getFieldOrderRDD(orderRDD, "PMS_REC_ROWKEY");

		JavaPairRDD<String, HotelOrderInfo> txnOrderRDD = getOrderRDDFromHdfs("TB_HOTEL_CRM_TXN_MEMBER_CONTACT_MAP", "CRM_", true, null);
		JavaPairRDD<String, HotelOrderInfo> pmsTxnOrderRDD = getOrderRDDFromHdfs("JJ000_SIEBEL_T_TXN_LIST_MAP", "PMS_TXN_", false, "PMS_TXN_ROWKEY");
		JavaPairRDD<String, HotelOrderInfo> fieldTxnDataRDD = null;
		fieldTxnDataRDD = getFieldOrderRDD(txnOrderRDD, "CRM_TXN_UNIQUE_KEY");
		fieldTxnDataRDD = fieldTxnDataRDD.union(getFieldOrderRDD(pmsTxnOrderRDD, "PMS_TXN_UNIQUE_KEY"));
		JavaPairRDD<String, HotelOrderInfo> txnRDD = reduceRDD(fieldTxnDataRDD);
		JavaPairRDD<String, HotelOrderInfo> recIdTXNDataRDD = getFieldOrderRDD(txnRDD, "PMS_TXN_RECORD_ID");
		
		JavaPairRDD<String, HotelOrderInfo> recIdOrderRDD = recIdOrderDataRDD.union(recIdTXNDataRDD);
		JavaPairRDD<String, List<HotelOrderInfo>> groupByRecIdOrderRDD = recIdOrderRDD.groupByKey();
		JavaPairRDD<String, HotelOrderInfo> resultOrderRDD = groupByRecIdOrderRDD.map(new PairFunction<Tuple2<String, List<HotelOrderInfo>>, String, List<HotelOrderInfo>>() {

			@Override
			public Tuple2<String, List<HotelOrderInfo>> call(Tuple2<String, List<HotelOrderInfo>> input) throws Exception {
				List<HotelOrderInfo> hotelOrderList = input._2();
				if(hotelOrderList.size() <= 1){
					return input;
				}
				List<HotelOrderInfo> orderList = getFilterHotelOrderInfoList(hotelOrderList, false);
				if(orderList.size() == 0){
					return input;
				}
				if(orderList.size() != 1){
					throw new RuntimeException("error[orderList.size() == 1] input:"+input);
				}
				List<HotelOrderInfo> txnList = getFilterHotelOrderInfoList(hotelOrderList, true);
				if(txnList.size() == 0){
					return input;
				}
				for (HotelOrderInfo txn : txnList) {
					txn.joinData(orderList.get(0));
				}
//				for (Iterator iterator = orderList.iterator(); iterator.hasNext();) {
//					HotelOrderInfo order = (HotelOrderInfo) iterator.next();
//					String order_name = order.getHotel_order_name();
//					if(order_name == null){
//						continue;
//					}
//					HotelOrderInfo orderTXN = nameTxnMap.get(order_name);
//					if(orderTXN != null){
//						orderTXN.joinData(order);
//						iterator.remove();
//						continue;
//					}
//					List<String> splits = Arrays.asList(StringUtils.split(order_name));
//					for (Iterator splitsIterator = splits.iterator(); splitsIterator.hasNext();) {
//						String splitName = (String) splitsIterator.next();
//						
//					}
//					
//					if(StringUtils.contains(order_name, txn_name+" ") || StringUtils.contains(order_name, " "+txn_name)){
//						txn.joinData(order);
//						order.
//						continue;
//					}
//				}
				return new Tuple2<String, List<HotelOrderInfo>>(input._1(), txnList);
			}

		}).flatMapValues(new Function<List<HotelOrderInfo>, Iterable<HotelOrderInfo>>() {

			@Override
			public Iterable<HotelOrderInfo> call(List<HotelOrderInfo> arg0) throws Exception {
				return arg0;
			}
			
		}).map(new PairFunction<Tuple2<String, HotelOrderInfo>, String, HotelOrderInfo>() {

			@Override
			public Tuple2<String, HotelOrderInfo> call(Tuple2<String, HotelOrderInfo> input) throws Exception {
				return new Tuple2<String, HotelOrderInfo>(UUID.randomUUID().toString(), input._2());
			}
			
		});
		
		
		saveDataToHbase(resultOrderRDD);
	}
	
	private static List<HotelOrderInfo> getFilterHotelOrderInfoList(List<HotelOrderInfo> hotelOrderList, boolean hasHotel_txn) {
		List<HotelOrderInfo> txnOrderList =  new ArrayList<HotelOrderInfo>();
		for (HotelOrderInfo order : hotelOrderList) {
			if(hasHotel_txn && order.hasHotel_txn()){
				txnOrderList.add(order);
				continue;
			}
			if(!hasHotel_txn && !order.hasHotel_txn()){
				txnOrderList.add(order);
				continue;
			}
		}
		return txnOrderList;
	}

	private static JavaPairRDD<String, HotelOrderInfo> reduceRDD(JavaPairRDD<String, HotelOrderInfo> fieldDataRDD) {
		
		if(partitionSize != null && partitionSize > 0){
			fieldDataRDD = fieldDataRDD.partitionBy(new HashPartitioner(partitionSize));
		}
		JavaPairRDD<String, HotelOrderInfo> reduceRDD = fieldDataRDD.reduceByKey(new Function2<HotelOrderInfo, HotelOrderInfo, HotelOrderInfo>() {
			
			@Override
			public HotelOrderInfo call(HotelOrderInfo arg0, HotelOrderInfo arg1) throws Exception {
				try {
					HotelOrderInfo toJoin = arg1;
					HotelOrderInfo data = arg0;
					if(toJoin == null){
						return data;
					}
					if(data == null){
						return toJoin;
					}
					data.joinData(toJoin);
					return data;
				} catch (Exception e) {
//		            StringWriter sw = new StringWriter();
//		            PrintWriter pw = new PrintWriter(sw);
//		            e.printStackTrace(pw);
//					throw new RuntimeException(e.getMessage()+"::::arg0::"+arg0+"::::arg1::"+arg1+"\n"+pw, e);
					throw new RuntimeException(e);
				}
			}
			
		});
		
		saveValue("reduceRDD", reduceRDD);
		logRDDcount("reduceRDD", reduceRDD);
		return reduceRDD;
	}
	
	private static JavaPairRDD<String, HotelOrderInfo> getOrderRDDFromHdfs(String tableName, String prefix, boolean withPrefix, String rowkeyName) throws IOException {
		
		if (pageSize != null && pageSize > 0) {
			return getOrderRDDFromHbase(tableName, prefix, withPrefix, rowkeyName);
		}

		final Broadcast<String> bc_rowkeyName = rowkeyName == null ? null : sc.broadcast(rowkeyName);

		JavaRDD<String> textFileRDD = null;
		if (partitionSize != null && partitionSize > 0) {
			textFileRDD = sc.textFile("hdfs:///tmp/export/" + tableName, partitionSize);
		} else {
			textFileRDD = sc.textFile("hdfs:///tmp/export/" + tableName);
		}

		JavaPairRDD<String, HotelOrderInfo> orderRDD = textFileRDD.map(new PairFunction<String, String, HotelOrderInfo>() {

			@Override
			public Tuple2<String, HotelOrderInfo> call(String input) throws Exception {
				try {
					int index = StringUtils.indexOf(input, "{\"");
					if (index <= 0) {
						return new Tuple2<String, HotelOrderInfo>(null, null);
					}
					String key = input.substring(0, index).trim();
					String json = input.substring(index, input.length()).trim();
					ObjectMapper objectMapper = new ObjectMapper();
					HotelOrderInfo hotelOrderInfo = objectMapper.readValue(json, HotelOrderInfo.class);
					if (bc_rowkeyName != null) {
						PropertyUtils.setProperty(hotelOrderInfo, bc_rowkeyName.value(), key);
					}
					return new Tuple2<String, HotelOrderInfo>(UUID.randomUUID().toString(), hotelOrderInfo);
				} catch (Exception e) {
					throw new RuntimeException(String.format("Error text:[%s]",input), e);
				}
			}

		}).filter(new Function<Tuple2<String, HotelOrderInfo>, Boolean>() {

			@Override
			public Boolean call(Tuple2<String, HotelOrderInfo> arg0) throws Exception {
				return arg0._1() != null && arg0._2() != null;
			}
		});

		if (partitionSize != null && partitionSize > 0) {
			orderRDD = orderRDD.partitionBy(new HashPartitioner(partitionSize));
		}
		saveValue(tableName, orderRDD);
		logRDDcount(tableName, orderRDD);
		return orderRDD;
	}
	
	private static JavaPairRDD<String, HotelOrderInfo> getOrderRDDFromHbase(String tableName, String prefix, boolean withPrefix, String rowkeyName) throws IOException {
		
		final Broadcast<String> bc_prefix = sc.broadcast(prefix);
		final Broadcast<Boolean> bc_withPrefix = sc.broadcast(withPrefix);
		final Broadcast<String> bc_rowkeyName = rowkeyName == null ? null : sc.broadcast(rowkeyName);
		
		JavaPairRDD<ImmutableBytesWritable, Result> orderHbaseRDD;
		if (pageSize == null || pageSize <= 0) {
			orderHbaseRDD = SparkUtils.getHbaseRDD(sc, inConf, tableName);
		}else{
			orderHbaseRDD = SparkUtils.getHbaseRDD(sc, inConf, tableName, null, new PageFilter(pageSize));
		}
		JavaPairRDD<String, HotelOrderInfo> orderRDD = orderHbaseRDD.map(new PairFunction<Tuple2<ImmutableBytesWritable, Result>, String ,HotelOrderInfo>() {

			@Override
			public Tuple2<String, HotelOrderInfo> call(Tuple2<ImmutableBytesWritable, Result> input) throws Exception {
				HotelOrderInfo hotelOrderInfo = new HotelOrderInfo();
				if(bc_rowkeyName != null){
					PropertyUtils.setProperty(hotelOrderInfo, bc_rowkeyName.value(), Bytes.toString(input._1().get()));
				}
				if(bc_withPrefix.value()){
					hotelOrderInfo.fillFieldDataWithPrefix(bc_prefix.value(), input._2());
				}else{
					hotelOrderInfo.fillFieldDataWithoutPrefix(bc_prefix.value(), input._2());
				}
				return new Tuple2<String, HotelOrderInfo>(UUID.randomUUID().toString(), hotelOrderInfo);
			}
			
		});
		if(partitionSize != null && partitionSize > 0){
			orderRDD = orderRDD.partitionBy(new HashPartitioner(partitionSize));
		}
		saveValue(tableName, orderRDD);
		logRDDcount(tableName, orderRDD);
		return orderRDD;
	}
	
	private static JavaPairRDD<String, HotelOrderInfo> getOrderRDD_txn(String tableName, String prefix, boolean withPrefix, String rowkeyName) throws IOException {

		final Broadcast<String> bc_rowkeyName = rowkeyName == null ? null : sc.broadcast(rowkeyName);

		JavaRDD<String> textFileRDD = null;
		if (partitionSize != null && partitionSize > 0) {
			textFileRDD = sc.textFile("hdfs:///tmp/export/" + tableName, partitionSize);
		} else {
			textFileRDD = sc.textFile("hdfs:///tmp/export/" + tableName);
		}

		JavaPairRDD<String, HotelOrderInfo> orderRDD = textFileRDD.map(new PairFunction<String, String, HotelOrderInfo>() {

			@Override
			public Tuple2<String, HotelOrderInfo> call(String input) throws Exception {
				int index = StringUtils.indexOf(input, "{");
				if (index <= 0) {
					return new Tuple2<String, HotelOrderInfo>(null, null);
				}
				String key = input.substring(0, index).trim();
				String json = input.substring(index, input.length()).trim();
				ObjectMapper objectMapper = new ObjectMapper();
				HotelOrderInfo hotelOrderInfo = objectMapper.readValue(json, HotelOrderInfo.class);
				if (bc_rowkeyName != null) {
					PropertyUtils.setProperty(hotelOrderInfo, bc_rowkeyName.value(), key);
				}
				return new Tuple2<String, HotelOrderInfo>(UUID.randomUUID().toString(), hotelOrderInfo);
			}

		}).filter(new Function<Tuple2<String, HotelOrderInfo>, Boolean>() {

			@Override
			public Boolean call(Tuple2<String, HotelOrderInfo> arg0) throws Exception {
				return arg0._1() != null && arg0._2() != null;
			}
		});

		if (partitionSize != null && partitionSize > 0) {
			orderRDD = orderRDD.partitionBy(new HashPartitioner(partitionSize));
		}
		saveValue(tableName, orderRDD);
		logRDDcount(tableName, orderRDD);
		return orderRDD;

	}

private static JavaPairRDD<String, HotelOrderInfo> getOrderRDD_reception(String tableName, String prefix, boolean withPrefix, String rowkeyName) throws IOException {
	
	final Broadcast<String> bc_rowkeyName = rowkeyName == null ? null : sc.broadcast(rowkeyName);

	JavaRDD<String> textFileRDD = null;
	if (partitionSize != null && partitionSize > 0) {
		textFileRDD = sc.textFile("hdfs:///tmp/export/" + tableName, partitionSize);
	} else {
		textFileRDD = sc.textFile("hdfs:///tmp/export/" + tableName);
	}

	JavaPairRDD<String, HotelOrderInfo> orderRDD = textFileRDD.map(new PairFunction<String, String, HotelOrderInfo>() {

		@Override
		public Tuple2<String, HotelOrderInfo> call(String input) throws Exception {
			int index = StringUtils.indexOf(input, "{");
			if (index <= 0) {
				return new Tuple2<String, HotelOrderInfo>(null, null);
			}
			String key = input.substring(0, index).trim();
			String json = input.substring(index, input.length()).trim();
			ObjectMapper objectMapper = new ObjectMapper();
			HotelOrderInfo hotelOrderInfo = objectMapper.readValue(json, HotelOrderInfo.class);
			if (bc_rowkeyName != null) {
				PropertyUtils.setProperty(hotelOrderInfo, bc_rowkeyName.value(), key);
			}
			return new Tuple2<String, HotelOrderInfo>(UUID.randomUUID().toString(), hotelOrderInfo);
		}

	}).filter(new Function<Tuple2<String, HotelOrderInfo>, Boolean>() {

		@Override
		public Boolean call(Tuple2<String, HotelOrderInfo> arg0) throws Exception {
			return arg0._1() != null && arg0._2() != null;
		}
	});

	if (partitionSize != null && partitionSize > 0) {
		orderRDD = orderRDD.partitionBy(new HashPartitioner(partitionSize));
	}
	saveValue(tableName, orderRDD);
	logRDDcount(tableName, orderRDD);
	return orderRDD;
}

private static JavaPairRDD<String, HotelOrderInfo> getOrderRDD_resv(String tableName, String prefix, boolean withPrefix, String rowkeyName) throws IOException {
	
	final Broadcast<String> bc_rowkeyName = rowkeyName == null ? null : sc.broadcast(rowkeyName);

	JavaRDD<String> textFileRDD = null;
	if (partitionSize != null && partitionSize > 0) {
		textFileRDD = sc.textFile("hdfs:///tmp/export/" + tableName, partitionSize);
	} else {
		textFileRDD = sc.textFile("hdfs:///tmp/export/" + tableName);
	}

	JavaPairRDD<String, HotelOrderInfo> orderRDD = textFileRDD.map(new PairFunction<String, String, HotelOrderInfo>() {

		@Override
		public Tuple2<String, HotelOrderInfo> call(String input) throws Exception {
			int index = StringUtils.indexOf(input, "{");
			if (index <= 0) {
				return new Tuple2<String, HotelOrderInfo>(null, null);
			}
			String key = input.substring(0, index).trim();
			String json = input.substring(index, input.length()).trim();
			ObjectMapper objectMapper = new ObjectMapper();
			HotelOrderInfo hotelOrderInfo = objectMapper.readValue(json, HotelOrderInfo.class);
			if (bc_rowkeyName != null) {
				PropertyUtils.setProperty(hotelOrderInfo, bc_rowkeyName.value(), key);
			}
			return new Tuple2<String, HotelOrderInfo>(UUID.randomUUID().toString(), hotelOrderInfo);
		}

	}).filter(new Function<Tuple2<String, HotelOrderInfo>, Boolean>() {

		@Override
		public Boolean call(Tuple2<String, HotelOrderInfo> arg0) throws Exception {
			return arg0._1() != null && arg0._2() != null;
		}
	});

	if (partitionSize != null && partitionSize > 0) {
		orderRDD = orderRDD.partitionBy(new HashPartitioner(partitionSize));
	}
	saveValue(tableName, orderRDD);
	logRDDcount(tableName, orderRDD);
	return orderRDD;
}

private static JavaPairRDD<String, HotelOrderInfo> getOrderRDD_resvDetail(String tableName, String prefix, boolean withPrefix, String rowkeyName) throws IOException {
	
	final Broadcast<String> bc_rowkeyName = rowkeyName == null ? null : sc.broadcast(rowkeyName);

	JavaRDD<String> textFileRDD = null;
	if (partitionSize != null && partitionSize > 0) {
		textFileRDD = sc.textFile("hdfs:///tmp/export/" + tableName, partitionSize);
	} else {
		textFileRDD = sc.textFile("hdfs:///tmp/export/" + tableName);
	}

	JavaPairRDD<String, HotelOrderInfo> orderRDD = textFileRDD.map(new PairFunction<String, String, HotelOrderInfo>() {

		@Override
		public Tuple2<String, HotelOrderInfo> call(String input) throws Exception {
			int index = StringUtils.indexOf(input, "{");
			if (index <= 0) {
				return new Tuple2<String, HotelOrderInfo>(null, null);
			}
			String key = input.substring(0, index).trim();
			String json = input.substring(index, input.length()).trim();
			ObjectMapper objectMapper = new ObjectMapper();
			HotelOrderInfo hotelOrderInfo = objectMapper.readValue(json, HotelOrderInfo.class);
			if (bc_rowkeyName != null) {
				PropertyUtils.setProperty(hotelOrderInfo, bc_rowkeyName.value(), key);
			}
			return new Tuple2<String, HotelOrderInfo>(UUID.randomUUID().toString(), hotelOrderInfo);
		}

	}).filter(new Function<Tuple2<String, HotelOrderInfo>, Boolean>() {

		@Override
		public Boolean call(Tuple2<String, HotelOrderInfo> arg0) throws Exception {
			return arg0._1() != null && arg0._2() != null;
		}
	});

	if (partitionSize != null && partitionSize > 0) {
		orderRDD = orderRDD.partitionBy(new HashPartitioner(partitionSize));
	}
	saveValue(tableName, orderRDD);
	logRDDcount(tableName, orderRDD);
	return orderRDD;
}

private static JavaPairRDD<String, HotelOrderInfo> getOrderRDD_hbp(String tableName, String prefix, boolean withPrefix, String rowkeyName) throws IOException {
	
	final Broadcast<String> bc_rowkeyName = rowkeyName == null ? null : sc.broadcast(rowkeyName);

	JavaRDD<String> textFileRDD = null;
	if (partitionSize != null && partitionSize > 0) {
		textFileRDD = sc.textFile("hdfs:///tmp/export/" + tableName, partitionSize);
	} else {
		textFileRDD = sc.textFile("hdfs:///tmp/export/" + tableName);
	}

	JavaPairRDD<String, HotelOrderInfo> orderRDD = textFileRDD.map(new PairFunction<String, String, HotelOrderInfo>() {

		@Override
		public Tuple2<String, HotelOrderInfo> call(String input) throws Exception {
			int index = StringUtils.indexOf(input, "{");
			if (index <= 0) {
				return new Tuple2<String, HotelOrderInfo>(null, null);
			}
			String key = input.substring(0, index).trim();
			String json = input.substring(index, input.length()).trim();
			ObjectMapper objectMapper = new ObjectMapper();
			HotelOrderInfo hotelOrderInfo = objectMapper.readValue(json, HotelOrderInfo.class);
			if (bc_rowkeyName != null) {
				PropertyUtils.setProperty(hotelOrderInfo, bc_rowkeyName.value(), key);
			}
			return new Tuple2<String, HotelOrderInfo>(UUID.randomUUID().toString(), hotelOrderInfo);
		}

	}).filter(new Function<Tuple2<String, HotelOrderInfo>, Boolean>() {

		@Override
		public Boolean call(Tuple2<String, HotelOrderInfo> arg0) throws Exception {
			return arg0._1() != null && arg0._2() != null;
		}
	});

	if (partitionSize != null && partitionSize > 0) {
		orderRDD = orderRDD.partitionBy(new HashPartitioner(partitionSize));
	}
	saveValue(tableName, orderRDD);
	logRDDcount(tableName, orderRDD);
	return orderRDD;
}
	
	//PMS_HT_Resv_MAP
	private static JavaPairRDD<String, HotelOrderInfo> getOrderRDD(String tableName, String prefix, boolean withPrefix, String rowkeyName) throws IOException {
		
		final Broadcast<String> bc_prefix = sc.broadcast(prefix);
		final Broadcast<Boolean> bc_withPrefix = sc.broadcast(withPrefix);
		final Broadcast<String> bc_rowkeyName = rowkeyName == null ? null : sc.broadcast(rowkeyName);
		
		JavaPairRDD<ImmutableBytesWritable, Result> orderHbaseRDD;
		if (pageSize == null || pageSize <= 0) {
			orderHbaseRDD = SparkUtils.getHbaseRDD(sc, inConf, tableName);
		}else{
			orderHbaseRDD = SparkUtils.getHbaseRDD(sc, inConf, tableName, null, new PageFilter(pageSize));
		}
		JavaPairRDD<String, HotelOrderInfo> orderRDD = orderHbaseRDD.map(new PairFunction<Tuple2<ImmutableBytesWritable, Result>, String ,HotelOrderInfo>() {

			@Override
			public Tuple2<String, HotelOrderInfo> call(Tuple2<ImmutableBytesWritable, Result> input) throws Exception {
				HotelOrderInfo hotelOrderInfo = new HotelOrderInfo();
				if(bc_rowkeyName != null){
					PropertyUtils.setProperty(hotelOrderInfo, bc_rowkeyName.value(), Bytes.toString(input._1().get()));
				}
				if(bc_withPrefix.value()){
					hotelOrderInfo.fillFieldDataWithPrefix(bc_prefix.value(), input._2());
				}else{
					hotelOrderInfo.fillFieldDataWithoutPrefix(bc_prefix.value(), input._2());
				}
				return new Tuple2<String, HotelOrderInfo>(UUID.randomUUID().toString(), hotelOrderInfo);
			}
			
		});
		if(partitionSize != null && partitionSize > 0){
			orderRDD = orderRDD.partitionBy(new HashPartitioner(partitionSize));
		}
		saveValue(tableName, orderRDD);
		logRDDcount(tableName, orderRDD);
		return orderRDD;
	}

	private static JavaPairRDD<String, HotelOrderInfo> getReceptionOrderRDD() throws IOException {
		JavaPairRDD<ImmutableBytesWritable, Result> receptionHbaseRDD;
		if (pageSize == null || pageSize <= 0) {
			receptionHbaseRDD = SparkUtils.getHbaseRDD(sc, inConf, "PMS_HT_Reception_MAP");
		}else{
			receptionHbaseRDD = SparkUtils.getHbaseRDD(sc, inConf, "PMS_HT_Reception_MAP", null, new PageFilter(pageSize));
		}
		JavaPairRDD<String, HotelOrderInfo> receptionOrderRDD = receptionHbaseRDD.map(new PairFunction<Tuple2<ImmutableBytesWritable, Result>, String ,HotelOrderInfo>() {

			@Override
			public Tuple2<String, HotelOrderInfo> call(Tuple2<ImmutableBytesWritable, Result> input) throws Exception {
				HotelOrderInfo hotelOrderInfo = new HotelOrderInfo();
				hotelOrderInfo.setPMS_REC_ROWKEY(Bytes.toString(input._1().get()));
				hotelOrderInfo.fillFieldDataWithoutPrefix("PMS_REC_", input._2());
				return new Tuple2<String, HotelOrderInfo>(UUID.randomUUID().toString(), hotelOrderInfo);
			}
			
		});
		if(partitionSize != null && partitionSize > 0){
			receptionOrderRDD = receptionOrderRDD.partitionBy(new HashPartitioner(partitionSize));
		}
		saveValue("receptionOrderRDD", receptionOrderRDD);
		return receptionOrderRDD;
	}

	private static JavaPairRDD<String, HotelOrderInfo> getTxnOrderRDD() throws IOException {
		JavaPairRDD<ImmutableBytesWritable, Result> txnHbaseRDD;
		if (pageSize == null || pageSize <= 0) {
			txnHbaseRDD = SparkUtils.getHbaseRDD(sc, inConf, "TB_HOTEL_CRM_TXN_MEMBER_CONTACT_MAP");
		}else{
			txnHbaseRDD = SparkUtils.getHbaseRDD(sc, inConf, "TB_HOTEL_CRM_TXN_MEMBER_CONTACT_MAP", null, new PageFilter(pageSize));
		}
		JavaPairRDD<String, HotelOrderInfo> txnOrderRDD = txnHbaseRDD.map(new PairFunction<Tuple2<ImmutableBytesWritable, Result>, String ,HotelOrderInfo>() {

			@Override
			public Tuple2<String, HotelOrderInfo> call(Tuple2<ImmutableBytesWritable, Result> input) throws Exception {
				HotelOrderInfo hotelOrderInfo = new HotelOrderInfo();
				hotelOrderInfo.fillFieldDataWithPrefix("CRM_", input._2());
				return new Tuple2<String, HotelOrderInfo>(UUID.randomUUID().toString(), hotelOrderInfo);
			}
			
		});
		if(partitionSize != null && partitionSize > 0){
			txnOrderRDD = txnOrderRDD.partitionBy(new HashPartitioner(partitionSize));
		}
		saveValue("txnOrderRDD", txnOrderRDD);
		return txnOrderRDD;
	}
	
	private static JavaPairRDD<String, HotelOrderInfo> joinHotelOrderInfo_bak(JavaPairRDD<String, HotelOrderInfo> txnOrderRDD, String dataFieldName, JavaPairRDD<String, HotelOrderInfo> receptionOrderRDD, String toJoinFieldName) {

		JavaPairRDD<String, HotelOrderInfo> fieldDataRDD = getFieldOrderRDD(txnOrderRDD, dataFieldName);
		if(partitionSize != null && partitionSize > 0){
			fieldDataRDD = fieldDataRDD.partitionBy(new HashPartitioner(partitionSize));
		}
		logRDDcount(String.format("fieldDataRDD{args:dataFieldName[%s],toJoinFieldName[%s]}", dataFieldName, toJoinFieldName), fieldDataRDD);
		
		JavaPairRDD<String, HotelOrderInfo> fieldToJoinRDD = getFieldOrderRDD(receptionOrderRDD, dataFieldName);
		if(partitionSize != null && partitionSize > 0){
			fieldToJoinRDD = fieldToJoinRDD.partitionBy(new HashPartitioner(partitionSize));
		}
		logRDDcount(String.format("fieldToJoinRDD{args:dataFieldName[%s],toJoinFieldName[%s]}", dataFieldName, toJoinFieldName), fieldToJoinRDD);
		
		JavaPairRDD<String, HotelOrderInfo> unionRDD = fieldDataRDD.union(fieldToJoinRDD);
		logRDDcount(String.format("unionRDD{args:dataFieldName[%s],toJoinFieldName[%s]}", dataFieldName, toJoinFieldName), unionRDD);
		
		JavaPairRDD<String, List<HotelOrderInfo>> groupRDD = unionRDD.groupByKey();
		logRDDcount(String.format("groupRDD{args:dataFieldName[%s],toJoinFieldName[%s]}", dataFieldName, toJoinFieldName), groupRDD);
		
		JavaPairRDD<String, HotelOrderInfo> reduceRDD = groupRDD.map(new PairFunction<Tuple2<String, List<HotelOrderInfo>>, String, HotelOrderInfo>()  {
			
			@Override
			public Tuple2<String, HotelOrderInfo> call(Tuple2<String, List<HotelOrderInfo>> input) throws Exception {
				try {
					HotelOrderInfo data = null;
					for (HotelOrderInfo order : input._2()) {
						if(order == null){
							continue;
						}
						if(data == null){
							data = order;
							continue;
						}
						data.joinData(order);
					}
					return new Tuple2<String, HotelOrderInfo>(UUID.randomUUID().toString(), data);
				} catch (Exception e) {
//		            StringWriter sw = new StringWriter();
//		            PrintWriter pw = new PrintWriter(sw);
//		            e.printStackTrace(pw);
//					throw new RuntimeException(e.getMessage()+"::::::"+input+"\n"+pw, e);
					throw new RuntimeException(e);
				}
			}
			
		});
		
		//logRDDcount(String.format("reduceRDD{args:dataFieldName[%s],toJoinFieldName[%s]}", dataFieldName, toJoinFieldName), reduceRDD);
		saveValue("reduceRDD", reduceRDD);

		return reduceRDD;
	}
	
	private static JavaPairRDD<String, HotelOrderInfo> joinHotelOrderInfo(JavaPairRDD<String, HotelOrderInfo> txnOrderRDD, String dataFieldName, JavaPairRDD<String, HotelOrderInfo> receptionOrderRDD, String toJoinFieldName) {

		JavaPairRDD<String, HotelOrderInfo> fieldDataRDD = getFieldOrderRDD(txnOrderRDD, dataFieldName);
		if(partitionSize != null && partitionSize > 0){
			fieldDataRDD = fieldDataRDD.partitionBy(new HashPartitioner(partitionSize));
		}
		logRDDcount(String.format("fieldDataRDD{args:dataFieldName[%s],toJoinFieldName[%s]}", dataFieldName, toJoinFieldName), fieldDataRDD);
		
		JavaPairRDD<String, HotelOrderInfo> fieldToJoinRDD = getFieldOrderRDD(receptionOrderRDD, toJoinFieldName);
		if(partitionSize != null && partitionSize > 0){
			fieldToJoinRDD = fieldToJoinRDD.partitionBy(new HashPartitioner(partitionSize));
		}
		logRDDcount(String.format("fieldToJoinRDD{args:dataFieldName[%s],toJoinFieldName[%s]}", dataFieldName, toJoinFieldName), fieldToJoinRDD);
		
		JavaPairRDD<String, HotelOrderInfo> unionRDD = fieldDataRDD.union(fieldToJoinRDD);
		logRDDcount(String.format("unionRDD{args:dataFieldName[%s],toJoinFieldName[%s]}", dataFieldName, toJoinFieldName), unionRDD);
		
		JavaPairRDD<String, HotelOrderInfo> reduceRDD = unionRDD.reduceByKey(new Function2<HotelOrderInfo, HotelOrderInfo, HotelOrderInfo>() {
			
			@Override
			public HotelOrderInfo call(HotelOrderInfo arg0, HotelOrderInfo arg1) throws Exception {
				try {
					HotelOrderInfo toJoin = arg1;
					HotelOrderInfo data = arg0;
					if(toJoin == null){
						return data;
					}
					if(data == null){
						return toJoin;
					}
					data.joinData(toJoin);
					return data;
				} catch (Exception e) {
//		            StringWriter sw = new StringWriter();
//		            PrintWriter pw = new PrintWriter(sw);
//		            e.printStackTrace(pw);
//					throw new RuntimeException(e.getMessage()+"::::arg0::"+arg0+"::::arg1::"+arg1+"\n"+pw, e);
					throw new RuntimeException(e);
				}
			}
			
		});
		
		//logRDDcount(String.format("reduceRDD{args:dataFieldName[%s],toJoinFieldName[%s]}", dataFieldName, toJoinFieldName), reduceRDD);
		saveValue("reduceRDD", reduceRDD);

		return reduceRDD;
	}
	
	private static JavaPairRDD<String, HotelOrderInfo> getFieldOrderRDD(JavaPairRDD<String, HotelOrderInfo> txnOrderRDD, String orderFieldName) {
		final Broadcast<String> orderFieldName_bd = sc.broadcast(orderFieldName);
		JavaPairRDD<String, HotelOrderInfo> fieldOrderRDD = txnOrderRDD.map(new PairFunction<Tuple2<String, HotelOrderInfo>, String, HotelOrderInfo>() {

			@Override
			public Tuple2<String, HotelOrderInfo> call(Tuple2<String, HotelOrderInfo> input) throws Exception {
				String orderFieldName = orderFieldName_bd.value();
				if (input == null) {
					new Tuple2<String, HotelOrderInfo>(UUID.randomUUID()+"|NULL", null);
				}
				HotelOrderInfo hotelOrderInfo = input._2();
				String fieldValue = (String) PropertyUtils.getProperty(hotelOrderInfo, orderFieldName);
				return new Tuple2<String, HotelOrderInfo>(fieldValue == null ? UUID.randomUUID()+"|NULL" : StringUtils.reverse(fieldValue), hotelOrderInfo);
			}

		});
		if(partitionSize != null && partitionSize > 0){
			fieldOrderRDD = fieldOrderRDD.partitionBy(new HashPartitioner(partitionSize));
		}
		saveValue(String.format("getFieldOrderRDD_%s", orderFieldName), fieldOrderRDD);
		logRDDcount(String.format("getFieldOrderRDD_%s", orderFieldName), fieldOrderRDD);
		return fieldOrderRDD;
	}

	private static void saveDataToHbase(JavaPairRDD<String, HotelOrderInfo> orderRDD) {
		outConf.set(TableOutputFormat.OUTPUT_TABLE, "TB_HOTEL_ORDER_MAP");
		JobConf jobConf = new JobConf(outConf, StreamingRealRecommendTravelLine.class);
		jobConf.setOutputFormat(WritableTableOutputFormat.class);

		if(partitionSize != null && partitionSize > 0){
			orderRDD = orderRDD.partitionBy(new HashPartitioner(partitionSize));
		}
		JavaPairRDD<ImmutableBytesWritable, Put> filter = orderRDD.map(new PairFunction<Tuple2<String, HotelOrderInfo>, ImmutableBytesWritable, Put>() {

			@Override
			public Tuple2<ImmutableBytesWritable, Put> call(Tuple2<String, HotelOrderInfo> input) throws Exception {
				try {
					HotelOrderInfo data = input._2();
					Map<String, String> fieldMap = new HashMap<String, String>();
					addField(data, fieldMap);

					ImmutableBytesWritable row = new ImmutableBytesWritable(new byte[0]);
					String rowkey = data.getROWKEY() != null ? data.getROWKEY() : data.genROWKEY();
					if (fieldMap.isEmpty() || rowkey == null) {
//FIXME						
//						if(true){
//							throw new RuntimeException("input:"+input.toString());
//						}
						return new Tuple2<ImmutableBytesWritable, Put>(row, null);
					}

					Put put = new Put(Bytes.toBytes(rowkey));
					for (Map.Entry<String, String> entry : fieldMap.entrySet()) {
						put.add(Bytes.toBytes("info"), Bytes.toBytes(entry.getKey()), Bytes.toBytes(entry.getValue()));
					}

					return new Tuple2<ImmutableBytesWritable, Put>(row, put);
				} catch (Exception e) {
//		            StringWriter sw = new StringWriter();
//		            PrintWriter pw = new PrintWriter(sw);
//		            e.printStackTrace(pw);
//					throw new RuntimeException(e.getMessage()+"::::::"+input.toString()+"\n"+pw, e);
					throw new RuntimeException(e.getMessage()+"::::::input"+input.toString(), e);
				}
			}

			private void addField(Object obj, Map<String, String> fieldMap) throws IllegalArgumentException, IllegalAccessException {
				if (obj == null) {
					return;
				}
				Field[] fields = obj.getClass().getDeclaredFields();
				
				for (Field field : fields) {
					field.setAccessible(true);
					String dataStr = null;
					String fieldName = field.getName();
					Object dataObj = field.get(obj);
					if("serialVersionUID".equals(fieldName) || dataObj==null){
						continue;
					}
					if (dataObj instanceof String) {
						dataStr = (String)dataObj;
					}else{
						continue;
					}
					if (StringUtils.isBlank(dataStr)) {
						continue;
					}
					fieldMap.put(fieldName, dataStr.trim());
				}
				
			}

		}).filter(new Function<Tuple2<ImmutableBytesWritable, Put>, Boolean>() {

			@Override
			public Boolean call(Tuple2<ImmutableBytesWritable, Put> arg0) throws Exception {
				return arg0._2() != null;
			}

		});
		logRDDcount("saveAsHadoopDataset", filter);
		filter.saveAsHadoopDataset(jobConf);
	}

	private static JavaPairRDD<TravelOrderInfo, BigdataMemberInfo> joinOrderMember(JavaPairRDD<TravelOrderInfo, BigdataMemberInfo> orderMemberRDD, String orderFieldName, JavaRDD<BigdataMemberInfo> memberRDD, String memberFieldName, String memberFieldSplit) {
		// 关联 TB_CRM_TXN_JZL_ORDER_GUEST_MAP, T_BIGDATA_MEMBER
		logRDDcount(String.format("total order count:[];args:orderFieldName[%s],memberFieldName[%s],memberFieldSplit[%s]", orderFieldName, memberFieldName, memberFieldSplit), orderMemberRDD);

		JavaPairRDD<TravelOrderInfo, BigdataMemberInfo> notToJoin = getNotToJoinOrderRDD(orderMemberRDD, orderFieldName).cache();
		logRDDcount(String.format("notToJoin order count:[];args:orderFieldName[%s],memberFieldName[%s],memberFieldSplit[%s]", orderFieldName, memberFieldName, memberFieldSplit), notToJoin);

		JavaPairRDD<TravelOrderInfo, BigdataMemberInfo> toJoin = subtract(orderMemberRDD, notToJoin).cache();
		logRDDcount(String.format("toJoin order count:[];args:orderFieldName[%s],memberFieldName[%s],memberFieldSplit[%s]", orderFieldName, memberFieldName, memberFieldSplit), toJoin);

		JavaPairRDD<String, BigdataMemberInfo> fieldMemberRDD = getFieldMemberRDD(memberRDD, memberFieldName, memberFieldSplit).cache();
		logRDDcount(String.format("field Member count:[];args:orderFieldName[%s],memberFieldName[%s],memberFieldSplit[%s]", orderFieldName, memberFieldName, memberFieldSplit), fieldMemberRDD);

		JavaPairRDD<String, TravelOrderInfo> fieldOrderRDD = null;//getFieldOrderRDD(toJoin, orderFieldName).cache();
		logRDDcount(String.format("field order count:[];args:orderFieldName[%s],memberFieldName[%s],memberFieldSplit[%s]", orderFieldName, memberFieldName, memberFieldSplit), fieldOrderRDD);

		JavaPairRDD<String, Tuple2<TravelOrderInfo, Optional<BigdataMemberInfo>>> leftOuterJoin = fieldOrderRDD.leftOuterJoin(fieldMemberRDD).cache();
		// logRDDcount(String.format("leftOuterJoin count:[];args:orderFieldName[%s],memberFieldName[%s],memberFieldSplit[%s]", orderFieldName, memberFieldName, memberFieldSplit), leftOuterJoin);

		JavaPairRDD<TravelOrderInfo, BigdataMemberInfo> joinResult = leftOuterJoin.map(new PairFunction<Tuple2<String, Tuple2<TravelOrderInfo, Optional<BigdataMemberInfo>>>, TravelOrderInfo, BigdataMemberInfo>() {

			@Override
			public Tuple2<TravelOrderInfo, BigdataMemberInfo> call(Tuple2<String, Tuple2<TravelOrderInfo, Optional<BigdataMemberInfo>>> input) throws Exception {
				return new Tuple2<TravelOrderInfo, BigdataMemberInfo>(input._2()._1(), input._2()._2().orNull());
			}

		});
		logRDDcount(String.format("joinResult order count:[];args:orderFieldName[%s],memberFieldName[%s],memberFieldSplit[%s]", orderFieldName, memberFieldName, memberFieldSplit), joinResult);
		JavaPairRDD<TravelOrderInfo, BigdataMemberInfo> union = notToJoin.union(joinResult);
		// System.out.println("############union.collectAsMap()="+union.collectAsMap());
		return union;
	}

	private static JavaPairRDD<TravelOrderInfo, BigdataMemberInfo> subtract(JavaPairRDD<TravelOrderInfo, BigdataMemberInfo> orderMemberRDD, JavaPairRDD<TravelOrderInfo, BigdataMemberInfo> notToJoin) {
		JavaPairRDD<String, Tuple2<TravelOrderInfo, BigdataMemberInfo>> total = getSubtractTypeRDD(orderMemberRDD);
		logRDDcount("total", total);
		JavaPairRDD<String, Tuple2<TravelOrderInfo, BigdataMemberInfo>> toSub = getSubtractTypeRDD(notToJoin);
		logRDDcount("toSub", toSub);
		JavaPairRDD<String, Tuple2<Tuple2<TravelOrderInfo, BigdataMemberInfo>, Optional<Tuple2<TravelOrderInfo, BigdataMemberInfo>>>> leftOuterJoin = total.leftOuterJoin(toSub);
		// logRDDcount("leftOuterJoin For Sub", leftOuterJoin);
		JavaPairRDD<String, Tuple2<Tuple2<TravelOrderInfo, BigdataMemberInfo>, Optional<Tuple2<TravelOrderInfo, BigdataMemberInfo>>>> filter = leftOuterJoin.filter(new Function<Tuple2<String, Tuple2<Tuple2<TravelOrderInfo, BigdataMemberInfo>, Optional<Tuple2<TravelOrderInfo, BigdataMemberInfo>>>>, Boolean>() {

			@Override
			public Boolean call(Tuple2<String, Tuple2<Tuple2<TravelOrderInfo, BigdataMemberInfo>, Optional<Tuple2<TravelOrderInfo, BigdataMemberInfo>>>> arg0) throws Exception {
				return !arg0._2()._2().isPresent();
			}
		});
		logRDDcount("filter For Sub", filter);
		JavaPairRDD<TravelOrderInfo, BigdataMemberInfo> subResult = filter.map(new PairFunction<Tuple2<String, Tuple2<Tuple2<TravelOrderInfo, BigdataMemberInfo>, Optional<Tuple2<TravelOrderInfo, BigdataMemberInfo>>>>, TravelOrderInfo, BigdataMemberInfo>() {

			@Override
			public Tuple2<TravelOrderInfo, BigdataMemberInfo> call(Tuple2<String, Tuple2<Tuple2<TravelOrderInfo, BigdataMemberInfo>, Optional<Tuple2<TravelOrderInfo, BigdataMemberInfo>>>> arg0) throws Exception {
				return arg0._2()._1();
			}
		});

		return subResult;
	}

	private static JavaPairRDD<String, Tuple2<TravelOrderInfo, BigdataMemberInfo>> getSubtractTypeRDD(JavaPairRDD<TravelOrderInfo, BigdataMemberInfo> orderMemberRDD) {
		JavaPairRDD<String, Tuple2<TravelOrderInfo, BigdataMemberInfo>> subtractTypeRDD = orderMemberRDD.map(new PairFunction<Tuple2<TravelOrderInfo, BigdataMemberInfo>, String, Tuple2<TravelOrderInfo, BigdataMemberInfo>>() {

			@Override
			public Tuple2<String, Tuple2<TravelOrderInfo, BigdataMemberInfo>> call(Tuple2<TravelOrderInfo, BigdataMemberInfo> input) throws Exception {
				return new Tuple2<String, Tuple2<TravelOrderInfo, BigdataMemberInfo>>((input._1() == null ? "NULL" : input._1().getROWKEY()) + "#" + (input._2() == null ? "NULL" : input._2().getROWKEY()), input);
			}

		});
		return subtractTypeRDD;
	}

	private static JavaPairRDD<TravelOrderInfo, BigdataMemberInfo> getNotToJoinOrderRDD(JavaPairRDD<TravelOrderInfo, BigdataMemberInfo> orderMemberRDD,
			String orderFieldName) {
		final Broadcast<String> orderFieldName_bd = sc.broadcast(orderFieldName);
		JavaPairRDD<TravelOrderInfo, BigdataMemberInfo> notToJoin = orderMemberRDD.filter(new Function<Tuple2<TravelOrderInfo, BigdataMemberInfo>, Boolean>() {

			@Override
			public Boolean call(Tuple2<TravelOrderInfo, BigdataMemberInfo> input) throws Exception {
				if (input._2() != null) {
					return true;
				}
				String orderFieldName = orderFieldName_bd.value();
				String fieldValue = input._1().getFieldData(orderFieldName);
				return StringUtils.isBlank(fieldValue);
			}
		});
		return notToJoin;
	}

	private static JavaPairRDD<String, BigdataMemberInfo> getFieldMemberRDD(JavaRDD<BigdataMemberInfo> memberRDD, String memberFieldName, String memberFieldSplit) {
		JavaPairRDD<String, BigdataMemberInfo> fieldMemberRDD = null;
		final Broadcast<String> memberFieldName_bd = sc.broadcast(memberFieldName);
		if (memberFieldSplit == null) {
			fieldMemberRDD = memberRDD.map(new PairFunction<BigdataMemberInfo, String, BigdataMemberInfo>() {

				@Override
				public Tuple2<String, BigdataMemberInfo> call(BigdataMemberInfo input) throws Exception {
					if (input == null) {
						return new Tuple2<String, BigdataMemberInfo>(null, input);
					}
					String memberFieldName = memberFieldName_bd.value();
					String fieldValue = input.getFieldData(memberFieldName);
					return new Tuple2<String, BigdataMemberInfo>(fieldValue, input);
				}

			});
		} else {
			final Broadcast<String> memberFieldSplit_bd = sc.broadcast(memberFieldSplit);
			fieldMemberRDD = memberRDD.flatMap(new PairFlatMapFunction<BigdataMemberInfo, String, BigdataMemberInfo>() {

				@Override
				public Iterable<Tuple2<String, BigdataMemberInfo>> call(BigdataMemberInfo input) throws Exception {
					String memberFieldName = memberFieldName_bd.value();
					String memberFieldSplit = memberFieldSplit_bd.value();
					String values = input.getFieldData(memberFieldName);
					String[] split = StringUtils.split(values, memberFieldSplit);
					List<Tuple2<String, BigdataMemberInfo>> result = new ArrayList<Tuple2<String, BigdataMemberInfo>>();
					if (split == null) {
						return result;
					}
					for (String fieldValue : split) {
						if (StringUtils.isEmpty(fieldValue)) {
							continue;
						}
						result.add(new Tuple2<String, BigdataMemberInfo>(fieldValue, input));
					}
					return result;
				}

			});
		}

		JavaPairRDD<String, BigdataMemberInfo> resultFieldMemberRDD = fieldMemberRDD.filter(new Function<Tuple2<String, BigdataMemberInfo>, Boolean>() {

			@Override
			public Boolean call(Tuple2<String, BigdataMemberInfo> arg0) throws Exception {
				return StringUtils.isNotBlank(arg0._1()) && arg0._2() != null;
			}
		});

		return resultFieldMemberRDD;
	}

	private static void logRDDcount(String name, JavaPairRDD<?, ?> rdd) {
		if (debugFlag) {
			System.out.println(String.format("===========%s COUNT:%s", name, rdd.count()));
		}
	}

	private static void logRDDcount(String name, JavaRDD<?> rdd) {
		if (debugFlag) {
			System.out.println(String.format("===========%s COUNT:%s", name, rdd.count()));
		}
	}

	private static void logValue(String name, Object value) {
		if (debugFlag) {
			System.out.println(String.format("===========%s:%s", name, value));
		}
	}

	private static void saveValue(String name, JavaPairRDD rdd) {
		if (testFlag) {
			System.out.println(String.format("===========save to /tmp/InitHotelOrderJob/" + name));
			rdd.saveAsTextFile("/tmp/InitHotelOrderJob/" + name);
		}
	}

	private static void saveValue(String name, JavaRDD rdd) {
		if (testFlag) {
			System.out.println(String.format("===========save to /tmp/InitHotelOrderJob/" + name));
			rdd.saveAsTextFile("/tmp/InitHotelOrderJob/" + name);
		}
	}

}
