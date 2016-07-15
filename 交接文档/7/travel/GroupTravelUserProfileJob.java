package com.jje.bigdata.userProfile.travel;

import java.util.Arrays;
import java.util.Date;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import com.jje.bigdata.userProfile.travel.service.GroupTravelUserProfileReducer;
import com.jje.bigdata.userProfile.travel.service.TravelUserProfileJob;

public class GroupTravelUserProfileJob {

	public static void main(String[] args) throws Exception {

		System.out.println("============================GroupTravelUserProfileJob Main开始" + new Date() + "=============================");
		if (args.length < 3) {
			System.out
					.println("/usr/lib/ngmr/run_app lexus-1.0-SNAPSHOT.jar com.jje.bigdata.userProfile.travel.GroupTravelUserProfileJob ngmr-yarn-client ./lexus-1.0-SNAPSHOT.jar totalBookingAmountPerYearMap true true");
			System.exit(1);
		}

		JavaSparkContext sc = null;

		sc = new JavaSparkContext(args[0], "GroupTravelUserProfileJob", new SparkConf());
		sc.addJar(args[1]);

		String column = args[2];
		GroupTravelUserProfileReducer reducer = new GroupTravelUserProfileReducer(column);

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

		System.out.println("============================GroupTravelUserProfileJob Main结束" + new Date() + "=============================");
	}

}
