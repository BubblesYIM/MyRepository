package com.jje.bigdata.userProfile.hotel;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.codehaus.jackson.map.ObjectMapper;

import com.jje.bigdata.userProfile.hotel.domain.HotelOrderInfo;

public class TransformHbaseToHdfsMR {

	private static Configuration conf = null;

	private static synchronized Configuration getConfiguration() {
		if (conf == null) {
			conf = HBaseConfiguration.create();
		}
		return conf;
	}

	public static void main(String[] args) {
		
		System.out.println("============================TransformHbaseToHdfsMR Main开始" + new Date() + "=============================");

		if (args.length != 1) {
			System.err.println("####args length error:" + Arrays.asList(args));
			System.err.println("hadoop_wxy jar lexus-1.0-SNAPSHOT.jar com.jje.bigdata.userProfile.hotel.TransformHbaseToHdfsMR tableName prefix withPrefix");
			System.err.println("hadoop_wxy jar lexus-1.0-SNAPSHOT.jar com.jje.bigdata.userProfile.hotel.TransformHbaseToHdfsMR JJ000_WEBSITE_T_HBP_ORDER_MAP HBP_ORDER_ false");
		}

		try {

			System.out.println("####args:" + Arrays.asList(args));

			String tableName = args[0];
			String prefix = args[1];
			Boolean withPrefix = Boolean.parseBoolean(args[2]);

			Configuration conf = getConfiguration();

			String hdfsPath = "/tmp/export/" + tableName;
			conf.setStrings("prefix", prefix);
			conf.setBoolean("withPrefix", withPrefix);

			Job job = new Job(conf, String.format("TransformHbaseToHdfsMR(%s)", tableName));
			job.setJarByClass(TransformHbaseToHdfsMR.class);

			FileOutputFormat.setOutputPath(job, new Path(hdfsPath));
			job.setReducerClass(InnerReduce.class);
			Scan scan = new Scan();
			TableMapReduceUtil.initTableMapperJob(tableName, scan, InnerMapper.class, Text.class, Text.class, job);

			int status = job.waitForCompletion(true) ? 0 : 1;
			System.out.println("job status: " + status);
			//System.exit(status);

		} catch (Exception e) {
			e.printStackTrace();
			//System.exit(1);
		}
		System.out.println("============================TransformHbaseToHdfsMR Main结束" + new Date() + "=============================");
	}

	public static class InnerReduce extends Reducer<Text, Text, Text, Text> {
		private Text result = new Text();

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for (Text val : values) {
				result.set(val);
				context.write(key, result);
			}
		}

	}

	public static class InnerMapper extends TableMapper<Text, Text> {

		@Override
		protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {

			String prefix = context.getConfiguration().get("prefix");
			boolean withPrefix = context.getConfiguration().getBoolean("withPrefix", false);

			HotelOrderInfo hotelOrderInfo = new HotelOrderInfo();
			try {
				if (withPrefix) {
				} else {
					hotelOrderInfo.fillFieldDataWithPrefix(prefix, value);
					hotelOrderInfo.fillFieldDataWithoutPrefix(prefix, value);
				}
			} catch (IllegalAccessException e) {
				throw new RuntimeException(e);
			}
			ObjectMapper objectMapper = new ObjectMapper();
			String json = objectMapper.writeValueAsString(hotelOrderInfo);
			context.write(new Text(key.get()), new Text(json));
		}
	}

}
