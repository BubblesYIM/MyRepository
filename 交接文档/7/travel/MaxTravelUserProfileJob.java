package com.jje.bigdata.userProfile.travel;

import java.util.Arrays;
import java.util.Date;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import com.jje.bigdata.userProfile.travel.service.MaxTravelUserProfileReducer;
import com.jje.bigdata.userProfile.travel.service.TravelUserProfileJob;

public class MaxTravelUserProfileJob {

	public static void main(String[] args) throws Exception {

		System.out.println("============================MaxTravelUserProfileJob Main开始" + new Date() + "=============================");
		if (args.length < 3) {
			System.out
					.println("/usr/lib/ngmr/run_app lexus-1.0-SNAPSHOT.jar com.jje.bigdata.userProfile.travel.MaxTravelUserProfileJob ngmr-yarn-client ./lexus-1.0-SNAPSHOT.jar orderConsumptionDate lastConsumptionAmount true true");
			System.exit(1);
		}

		JavaSparkContext sc = null;

		sc = new JavaSparkContext(args[0], "MaxTravelUserProfileJob", new SparkConf());
		sc.addJar(args[1]);
		String maxColumn = args[2];
		String valueColumn = args[3];
		MaxTravelUserProfileReducer reducer = new MaxTravelUserProfileReducer(maxColumn);

		TravelUserProfileJob job = new TravelUserProfileJob(valueColumn, reducer);

		System.out.println("#############args:" + Arrays.asList(args));

		if (args.length >= 5) {
			boolean debugFlag = Boolean.parseBoolean(args[4]);
			job.setDebugFlag(debugFlag);
			System.out.println("###############debugFlag = " + debugFlag);
		}

		if (args.length >= 6) {
			boolean testFlag = Boolean.parseBoolean(args[5]);
			job.setTestFlag(testFlag);
			System.out.println("###############testFlag = " + testFlag);
		}

		job.run(sc);

		System.out.println("============================MaxTravelUserProfileJob Main结束" + new Date() + "=============================");
	}

}
