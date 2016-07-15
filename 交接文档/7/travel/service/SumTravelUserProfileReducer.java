package com.jje.bigdata.userProfile.travel.service;

import java.math.BigDecimal;

import org.apache.commons.beanutils.PropertyUtils;

import com.jje.bigdata.userProfile.travel.domain.TravelUserProfileInfo;
import com.jje.bigdata.userProfile.travel.util.AverageUnit;

public class SumTravelUserProfileReducer implements TravelUserProfileReducer {
	private static final long serialVersionUID = 1L;
	private String column;
	private Class<?> type;

	public SumTravelUserProfileReducer(String column) {
		super();
		this.column = column;
	}

	@Override
	public TravelUserProfileInfo process(TravelUserProfileInfo arg1, TravelUserProfileInfo arg2) {
		try {
			if (type == null) {
				type = PropertyUtils.getPropertyType(arg1, column);
			}

			if (BigDecimal.class.equals(type)) {
				BigDecimal value1 = (BigDecimal) PropertyUtils.getProperty(arg1, column);
				BigDecimal value2 = (BigDecimal) PropertyUtils.getProperty(arg2, column);
				value1 = value1 == null ? new BigDecimal(0) : value1;
				value2 = value2 == null ? new BigDecimal(0) : value2;
				PropertyUtils.setProperty(arg1, column, value1.add(value2));
				return arg1;
			}

			if (AverageUnit.class.equals(type)) {
				AverageUnit value1 = (AverageUnit) PropertyUtils.getProperty(arg1, column);
				AverageUnit value2 = (AverageUnit) PropertyUtils.getProperty(arg2, column);
				AverageUnit result = null;
				if (value1 == null) {
					result = value2;
				} else {
					value1.add(value2);
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
