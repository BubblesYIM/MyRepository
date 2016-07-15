package com.jje.bigdata.userProfile.travel.tag;

import java.io.IOException;
import java.io.Serializable;
//import java.math.BigDecimal;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;

import com.jje.bigdata.util.SparkUtils;
import com.jje.common.utils.StringUtils;

public class AirLines implements Serializable {
	private static final long serialVersionUID = 1L;
	
	private static JavaSparkContext sc;
	private static SparkConf sparkConf;
	private static Configuration conf;
	private static HConnection conn;
	private static HTableInterface airLinesTable;

	static {
		try {
			conf = HBaseConfiguration.create();
			conf.set(TableInputFormat.SCAN, SparkUtils.convertScanToString(new Scan()));
			conn = HConnectionManager.createConnection(conf);
			airLinesTable = conn.getTable("T_TB_AIRLINES_MAPPING");
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
		System.out.println("/usr/lib/ngmr/run_app_min lexus-1.0-SNAPSHOT.jar com.jje.bigdata.userProfile.travel.tag.AirLines ngmr-yarn-client lexus-1.0-SNAPSHOT.jar");
		try {
			sparkConf = new SparkConf();

			sc = new JavaSparkContext(args[0], "TravelUserConsumptionProfile", sparkConf);
			sc.addJar(args[1]);
			
			Pattern pattern = Pattern.compile("[a-zA-Z0-9]{2}[0-9]{3}");
			final Broadcast<Pattern> bPattern = sc.broadcast(pattern);
			
			conf.set(TableInputFormat.INPUT_TABLE, "JZL_T_TEAM_ROUTE_MAP");
			JavaPairRDD<String, String> routes = sc.newAPIHadoopRDD(conf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class).filter(new Function<Tuple2<ImmutableBytesWritable,Result>, Boolean>() {
				private static final long serialVersionUID = 1L;

				@Override
				public Boolean call(Tuple2<ImmutableBytesWritable, Result> original) throws Exception {
					Result res = original._2();
					String clineNo = SparkUtils.getValue(res, "info", "clineno");
					Pattern pattern = bPattern.value();
					
					if(SparkUtils.isBlank(clineNo)){
						return false;
					}
					Matcher matcher = pattern.matcher(clineNo);
					
					return matcher.find();
				}
			}).map(new PairFunction<Tuple2<ImmutableBytesWritable, Result>, String, String>() {
				private static final long serialVersionUID = 1L;

				@Override
				public Tuple2<String, String> call(Tuple2<ImmutableBytesWritable, Result> original) throws Exception {
					Result res = original._2();
					String clineNo = SparkUtils.getValue(res, "info", "clineno");
					String uteamID = SparkUtils.getValue(res, "info", "uteamid");
					Pattern pattern = bPattern.value();
					Matcher matcher = pattern.matcher(clineNo);
					Set<String> airLines = new HashSet<String>();
					while(matcher.find()){
						String group = matcher.group();
						String alias = group.substring(0, 2).toUpperCase();
						if(SparkUtils.isBlank(alias)){
							continue;
						}
						Result result = airLinesTable.get(new Get(Bytes.toBytes(alias)));
						String company = SparkUtils.getValue(result, "info", "COMPANY");
						if(SparkUtils.isBlank(company)){
							continue;
						}
						airLines.add(company);
					}
					
					String airLinesStr = StringUtils.join(airLines.toArray(),",");
					return new Tuple2<String, String>(uteamID, airLinesStr);
				}
			}).reduceByKey(new Function2<String, String, String>() {
				private static final long serialVersionUID = 1L;

				@Override
				public String call(String arg0, String arg1) throws Exception {
					String[] split1 = arg0.split(",");
					String[] split2 = arg1.split(",");
					Set<String> resultSet = new HashSet<String>();
					for(String str : split1){
						resultSet.add(str);
					}
					for(String str : split2){
						resultSet.add(str);
					}
					return StringUtils.join(resultSet.toArray(),",");
				}
			});
			
//			Map<String, String> maps = routes.collectAsMap();
			SparkUtils.logRDDcount("routesRDD", routes);
//			for (String key : maps.keySet()) {
//				System.out.println("key:"+key + "-------------------val:"+maps.get(key));
//			}
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
