package com.jje.bigdata.userProfile.travel;

import java.util.Arrays;
import java.util.Date;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import com.jje.bigdata.userProfile.travel.service.SumTravelUserProfileReducer;
import com.jje.bigdata.userProfile.travel.service.TravelUserProfileJob;

public class SumTravelUserProfileJob {

	public static void main(String[] args) throws Exception {

		System.out.println("============================SumTravelUserProfileJob Main开始" + new Date() + "=============================");
		if (args.length < 3) {
			System.out
					.println("/usr/lib/ngmr/run_app lexus-1.0-SNAPSHOT.jar com.jje.bigdata.userProfile.travel.SumTravelUserProfileJob ngmr-yarn-client ./lexus-1.0-SNAPSHOT.jar totalConsumptionAmount true true");
			System.exit(1);
		}

		JavaSparkContext sc = null;

		sc = new JavaSparkContext(args[0], "SumTravelUserProfileJob", new SparkConf());
		sc.addJar(args[1]);

		String column = args[2];
		SumTravelUserProfileReducer reducer = new SumTravelUserProfileReducer(column);

		TravelUserProfileJob job = new TravelUserProfileJob(column, reducer);

		System.out.println("#############args:" + Arrays.asList(args));

		if (args.length >= 4) {
			boolean debugFlag = Boolean.parseBoolean(args[3]);
			job.setDebugFlag(debugFlag);
			System.out.println("###############debugFlag = " + debugFlag);
		}

		if (args.length >= 5) {
			boolean testFlag = Boolean.parseBoolean(args[4]);
			job.setTestFlag(testFlag);
			System.out.println("###############testFlag = " + testFlag);
		}

		job.run(sc);

		System.out.println("============================SumTravelUserProfileJob Main结束" + new Date() + "=============================");
	}

}
