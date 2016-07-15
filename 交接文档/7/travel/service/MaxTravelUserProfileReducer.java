package com.jje.bigdata.userProfile.travel.service;

import org.apache.commons.beanutils.PropertyUtils;

import com.jje.bigdata.userProfile.travel.domain.TravelUserProfileInfo;

public class MaxTravelUserProfileReducer implements TravelUserProfileReducer {

	private static final long serialVersionUID = 1L;
	private String column;

	public MaxTravelUserProfileReducer(String column) {
		super();
		this.column = column;
	}

	@Override
	public TravelUserProfileInfo process(TravelUserProfileInfo arg1, TravelUserProfileInfo arg2) {
		try {
			Comparable value1 = (Comparable) PropertyUtils.getProperty(arg1, column);
			Comparable value2 = (Comparable) PropertyUtils.getProperty(arg2, column);
			if (value1 == null) {
				return arg2;
			}
			if (value2 == null) {
				return arg1;
			}
			if (value1.compareTo(value2) < 0) {
				return arg2;
			}
			return arg1;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

}
