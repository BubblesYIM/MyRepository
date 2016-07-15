package com.jje.bigdata.userProfile.hotel;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
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

import com.jje.bigdata.util.MapReduceUtils;

public class TransformHbaseToHdfsMapMR {

	private static Configuration conf = null;

	private static synchronized Configuration getConfiguration() {
		if (conf == null) {
			conf = HBaseConfiguration.create();
		}
		return conf;
	}

	public static void main(String[] args) {

		System.out.println("============================TransformHbaseToHdfsMR Main开始" + new Date() + "=============================");
		Configuration conf = getConfiguration();

		if (args.length != 1) {
			System.err.println("####args length error:" + Arrays.asList(args));
			System.err.println("hadoop_wxy jar lexus-1.0-SNAPSHOT.jar com.jje.bigdata.userProfile.hotel.TransformHbaseToHdfsMapMR tableName family");
			System.err.println("hadoop_wxy jar lexus-1.0-SNAPSHOT.jar com.jje.bigdata.userProfile.hotel.TransformHbaseToHdfsMapMR TB_HOTEL_ORDER_MAP info");
		}

		try {

			List<String> argList = Arrays.asList(args);
			System.out.println("####args:" + argList);

			if (args.length < 2) {
				System.out.println("args error~!");
				System.exit(0);
			}

			if (args.length >= 3) {
				List<String> colums = argList.subList(2, args.length);
				System.out.println("####columns:" + colums);
				conf.set("columns", StringUtils.join(colums, ","));
			}

			String tableName = args[0];
			String family = args[1];

			conf.set("family", family);

			String hdfsPath = "/tmp/jarvis/" + tableName;

			Job job = new Job(conf, String.format("TransformHbaseToHdfsUtils(%s)", tableName));
			job.setJarByClass(TransformHbaseToHdfsMapMR.class);
			job.setJobName(String.format("TransformHbaseToHdfsMapMR(%s)", tableName));

			FileOutputFormat.setOutputPath(job, new Path(hdfsPath));
			job.setReducerClass(InnerReduce.class);
			Scan scan = new Scan();
			TableMapReduceUtil.initTableMapperJob(tableName, scan, InnerMapper.class, Text.class, Text.class, job);

			int status = job.waitForCompletion(true) ? 0 : 1;
			System.out.println("job status: " + status);
			// System.exit(status);

		} catch (Exception e) {
			e.printStackTrace();
			// System.exit(1);
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
			String[] colums = null;
			String family = context.getConfiguration().get("family");
			String columsStr = context.getConfiguration().get("columns");
			if (StringUtils.isNotBlank(columsStr)) {
				colums = columsStr.split(",");
			}
			Map<String, String> mapData = MapReduceUtils.toMap(value, family, colums);
			if (mapData != null && !mapData.isEmpty()) {
				ObjectMapper objectMapper = new ObjectMapper();
				String json = objectMapper.writeValueAsString(mapData);
				context.write(new Text(key.get()), new Text(json));
			}
		}
	}

}
