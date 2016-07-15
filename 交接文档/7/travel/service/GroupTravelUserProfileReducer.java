package com.jje.bigdata.userProfile.travel.service;

import org.apache.commons.beanutils.PropertyUtils;

import com.jje.bigdata.userProfile.travel.domain.NumberMap;
import com.jje.bigdata.userProfile.travel.domain.TravelUserProfileInfo;
import com.jje.bigdata.userProfile.travel.util.GroupAverageMap;

public class GroupTravelUserProfileReducer implements TravelUserProfileReducer {
	private static final long serialVersionUID = 1L;
	private String column;
	private Class<?> type;

	public GroupTravelUserProfileReducer(String column) {
		super();
		this.column = column;
	}

	@Override
	public TravelUserProfileInfo process(TravelUserProfileInfo arg1, TravelUserProfileInfo arg2) {
		try {

			if (type == null) {
				type = PropertyUtils.getPropertyType(arg1, column);
			}

			if (NumberMap.class.equals(type)) {
				NumberMap value1 = (NumberMap) PropertyUtils.getProperty(arg1, column);
				NumberMap value2 = (NumberMap) PropertyUtils.getProperty(arg2, column);
				if (value1 == null) {
					return arg2;
				}
				if (value2 == null) {
					return arg1;
				}
				value1.putAll(value2);
				PropertyUtils.setProperty(arg1, column, value1);
				return arg1;
			}

			if (GroupAverageMap.class.equals(type)) {
				GroupAverageMap value1 = (GroupAverageMap) PropertyUtils.getProperty(arg1, column);
				GroupAverageMap value2 = (GroupAverageMap) PropertyUtils.getProperty(arg2, column);
				GroupAverageMap result = null;
				if (value1 == null) {
					result = value2;
				} else {
					value1.putAll(value2);
					result = value1;
				}
				PropertyUtils.setProperty(arg1, column, result);
				return arg1;
			}

			return null;

		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

}
