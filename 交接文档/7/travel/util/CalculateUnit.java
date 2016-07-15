package com.jje.bigdata.userProfile.travel.util;

import java.io.Serializable;
import java.math.BigDecimal;

public interface CalculateUnit extends Serializable {
	
	void add(CalculateUnit calculateUnit);

	BigDecimal getValue();

}
