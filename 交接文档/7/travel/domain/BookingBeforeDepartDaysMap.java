package com.jje.bigdata.userProfile.travel.domain;

import java.util.Date;

/**
 * 输出结果：0-3天，4-7天、8-14天、15-21天、22-28天、29-35天、36-42天、43-49天、50-56天、57-63天 、64-70天、
 * 71-77天、78-84天、85-91天、92-98天、99-105天、106-112天、113-119天、120-135天、136-150
 * 天、151-180天、181-210天、211天以上
 */
public class BookingBeforeDepartDaysMap {

	private static double ONE_DAY = 60 * 60 * 24 * 1000;// 86400000

	public static String getInterval(Date bookingDate, Date departDate) {
		double interval = (departDate.getTime() - bookingDate.getTime()) / ONE_DAY;
		if (interval <= 3) {
			return "0-3";
		} else if (interval <= 7) {
			return "4-7";
		} else if (interval <= 14) {
			return "8-14";
		} else if (interval <= 21) {
			return "15-21";
		} else if (interval <= 28) {
			return "22-28";
		} else if (interval <= 35) {
			return "29-35";
		} else if (interval <= 42) {
			return "36-42";
		} else if (interval <= 49) {
			return "43-49";
		} else if (interval <= 56) {
			return "50-56";
		} else if (interval <= 63) {
			return "57-63";
		} else if (interval <= 70) {
			return "64-70";
		} else if (interval <= 77) {
			return "71-77";
		} else if (interval <= 84) {
			return "78-84";
		} else if (interval <= 91) {
			return "85-91";
		} else if (interval <= 98) {
			return "92-98";
		} else if (interval <= 105) {
			return "99-105";
		} else if (interval <= 112) {
			return "106-112";
		} else if (interval <= 119) {
			return "113-119";
		} else if (interval <= 135) {
			return "120-135";
		} else if (interval <= 150) {
			return "136-150";
		} else if (interval <= 180) {
			return "151-180";
		} else if (interval <= 210) {
			return "181-210";
		} else {
			return "211-more";
		}
	}
}
