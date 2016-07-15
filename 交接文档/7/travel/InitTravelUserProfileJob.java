package com.jje.bigdata.userProfile.travel;

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

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;

import com.google.common.base.Optional;
import com.jje.bigdata.census.ThreadSafeDateUtils;
import com.jje.bigdata.travel.bigdata.service.MemberService;
import com.jje.bigdata.travel.bigdata.service.TravelService;
import com.jje.bigdata.travel.realRecommend.StreamingRealRecommendTravelLine;
import com.jje.bigdata.userProfile.travel.domain.BigdataMemberInfo;
import com.jje.bigdata.userProfile.travel.domain.TravelOrderInfo;
import com.jje.bigdata.util.WritableTableOutputFormat;

@SuppressWarnings("deprecation")
public class InitTravelUserProfileJob {

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

	public static void main(String[] args) throws Exception {

		System.out.println("============================InitTravelUserProfileJob Main开始" + new Date() + "=============================");
		if (args.length < 3) {
			System.out.println("/usr/lib/ngmr/run_app lexus-1.0-SNAPSHOT.jar com.jje.bigdata.userProfile.travel.InitTravelUserProfileJob ngmr-yarn-client ./lexus-1.0-SNAPSHOT.jar true true");
			System.exit(1);
		}
		sc = new JavaSparkContext(args[0], "InitTravelUserProfileJob", new SparkConf());

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

		System.out.println("============================InitTravelUserProfileJob Main结束" + new Date() + "=============================");
	}

	private static void run() throws IOException {

		JavaRDD<TravelOrderInfo> orderRDD = TravelService.getBigdataOrderRDD(sc, inConf);

		logRDDcount("orderRDD", orderRDD);
		JavaRDD<BigdataMemberInfo> memberRDD = MemberService.getBigdataMemberRDD(sc, inConf);

		JavaPairRDD<TravelOrderInfo, BigdataMemberInfo> orderMemberRDD = orderRDD.map(new PairFunction<TravelOrderInfo, TravelOrderInfo, BigdataMemberInfo>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<TravelOrderInfo, BigdataMemberInfo> call(TravelOrderInfo input) throws Exception {
				return new Tuple2<TravelOrderInfo, BigdataMemberInfo>(input, null);
			}

		}).cache();

		orderMemberRDD = joinOrderMember(orderMemberRDD, "JZL_GUEST_ROWKEY", memberRDD, "GUEST_ID", "|");
		orderMemberRDD = joinOrderMember(orderMemberRDD, "CRM_TXN_X_CONTACT_ID", memberRDD, "PERSON_UID", "|");
		orderMemberRDD = joinOrderMember(orderMemberRDD, "CRM_TXN_MEMBER_ID", memberRDD, "MEMBER_ID", "|");

		saveDataToHbase(orderMemberRDD);
		// save中间数据至hbase
	}

	private static void saveDataToHbase(JavaPairRDD<TravelOrderInfo, BigdataMemberInfo> orderMemberRDD) {
		outConf.set(TableOutputFormat.OUTPUT_TABLE, "TB_TRAVEL_USER_PROFILE_DATA");
		JobConf jobConf = new JobConf(outConf, StreamingRealRecommendTravelLine.class);
		jobConf.setOutputFormat(WritableTableOutputFormat.class);

		orderMemberRDD.map(new PairFunction<Tuple2<TravelOrderInfo, BigdataMemberInfo>, ImmutableBytesWritable, Put>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<ImmutableBytesWritable, Put> call(Tuple2<TravelOrderInfo, BigdataMemberInfo> input) throws Exception {
				try {
					Map<String, String> fieldMap = new HashMap<String, String>();
					addField(input._1(), TravelOrderInfo.class, "O_", fieldMap);
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
					throw new RuntimeException(e.getMessage()+"::::::"+input.toString()+"\n"+pw, e);
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
					if( "serialVersionUID".equals(fieldName) || dataObj==null){
						continue;
					}
					if (dataObj instanceof Date) {
						Date date = (Date) dataObj;
						dataStr = ThreadSafeDateUtils.secFormat(date);
					}else{
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

		}).saveAsHadoopDataset(jobConf);//TODO test
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

		JavaPairRDD<String, TravelOrderInfo> fieldOrderRDD = getFieldOrderRDD(toJoin, orderFieldName).cache();
		logRDDcount(String.format("field order count:[];args:orderFieldName[%s],memberFieldName[%s],memberFieldSplit[%s]", orderFieldName, memberFieldName, memberFieldSplit), fieldOrderRDD);

		JavaPairRDD<String, Tuple2<TravelOrderInfo, Optional<BigdataMemberInfo>>> leftOuterJoin = fieldOrderRDD.leftOuterJoin(fieldMemberRDD).cache();
		// logRDDcount(String.format("leftOuterJoin count:[];args:orderFieldName[%s],memberFieldName[%s],memberFieldSplit[%s]", orderFieldName, memberFieldName, memberFieldSplit), leftOuterJoin);

		JavaPairRDD<TravelOrderInfo, BigdataMemberInfo> joinResult = leftOuterJoin.map(new PairFunction<Tuple2<String, Tuple2<TravelOrderInfo, Optional<BigdataMemberInfo>>>, TravelOrderInfo, BigdataMemberInfo>() {
			private static final long serialVersionUID = 1L;

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
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<String, Tuple2<Tuple2<TravelOrderInfo, BigdataMemberInfo>, Optional<Tuple2<TravelOrderInfo, BigdataMemberInfo>>>> arg0) throws Exception {
				return !arg0._2()._2().isPresent();
			}
		});
		logRDDcount("filter For Sub", filter);
		JavaPairRDD<TravelOrderInfo, BigdataMemberInfo> subResult = filter.map(new PairFunction<Tuple2<String, Tuple2<Tuple2<TravelOrderInfo, BigdataMemberInfo>, Optional<Tuple2<TravelOrderInfo, BigdataMemberInfo>>>>, TravelOrderInfo, BigdataMemberInfo>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<TravelOrderInfo, BigdataMemberInfo> call(Tuple2<String, Tuple2<Tuple2<TravelOrderInfo, BigdataMemberInfo>, Optional<Tuple2<TravelOrderInfo, BigdataMemberInfo>>>> arg0) throws Exception {
				return arg0._2()._1();
			}
		});

		return subResult;
	}

	private static JavaPairRDD<String, Tuple2<TravelOrderInfo, BigdataMemberInfo>> getSubtractTypeRDD(JavaPairRDD<TravelOrderInfo, BigdataMemberInfo> orderMemberRDD) {
		JavaPairRDD<String, Tuple2<TravelOrderInfo, BigdataMemberInfo>> subtractTypeRDD = orderMemberRDD.map(new PairFunction<Tuple2<TravelOrderInfo, BigdataMemberInfo>, String, Tuple2<TravelOrderInfo, BigdataMemberInfo>>() {
			private static final long serialVersionUID = 1L;

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
			private static final long serialVersionUID = 1L;

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

	private static JavaPairRDD<String, TravelOrderInfo> getFieldOrderRDD(JavaPairRDD<TravelOrderInfo, BigdataMemberInfo> toJoin, String orderFieldName) {
		final Broadcast<String> orderFieldName_bd = sc.broadcast(orderFieldName);
		JavaPairRDD<String, TravelOrderInfo> fieldOrderRDD = toJoin.map(new PairFunction<Tuple2<TravelOrderInfo, BigdataMemberInfo>, String, TravelOrderInfo>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, TravelOrderInfo> call(Tuple2<TravelOrderInfo, BigdataMemberInfo> input) throws Exception {
				String orderFieldName = orderFieldName_bd.value();
				if (input._1() == null) {
					new Tuple2<String, TravelOrderInfo>("NULL", input._1());
				}
				String fieldValue = input._1().getFieldData(orderFieldName);
				return new Tuple2<String, TravelOrderInfo>(fieldValue == null ? "NULL" : fieldValue, input._1());
			}

		});
		return fieldOrderRDD;
	}

	private static JavaPairRDD<String, BigdataMemberInfo> getFieldMemberRDD(JavaRDD<BigdataMemberInfo> memberRDD, String memberFieldName, String memberFieldSplit) {
		JavaPairRDD<String, BigdataMemberInfo> fieldMemberRDD = null;
		final Broadcast<String> memberFieldName_bd = sc.broadcast(memberFieldName);
		if (memberFieldSplit == null) {
			fieldMemberRDD = memberRDD.map(new PairFunction<BigdataMemberInfo, String, BigdataMemberInfo>() {
				private static final long serialVersionUID = 1L;

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
				private static final long serialVersionUID = 1L;

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
			private static final long serialVersionUID = 1L;

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

//	private static void logValue(String name, Object value) {
//		if (debugFlag) {
//			System.out.println(String.format("===========%s:%s", name, value));
//		}
//	}
//
//	private static void saveValue(String name, JavaPairRDD rdd) {
//		if (testFlag) {
//			System.out.println(String.format("===========save to /tmp/InitTravelUserProfileJob/" + name));
//			rdd.saveAsTextFile("/tmp/InitTravelUserProfileJob/" + name);
//		}
//	}
//
//	private static void saveValue(String name, JavaRDD rdd) {
//		if (testFlag) {
//			System.out.println(String.format("===========save to /tmp/InitTravelUserProfileJob/" + name));
//			rdd.saveAsTextFile("/tmp/InitTravelUserProfileJob/" + name);
//		}
//	}

}
