package com.jje.bigdata.userProfile.travel.util;

import java.io.IOException;
import java.math.BigDecimal;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.jje.bigdata.util.SparkUtils;

public class AverageUnit implements CalculateUnit {
	private static final long serialVersionUID = 1L;
	private BigDecimal amount;
	private BigDecimal count;

	public AverageUnit() {
		super();
		// TODO Auto-generated constructor stub
	}

	public AverageUnit(BigDecimal amount, BigDecimal count) {
		this();
		this.amount = amount;
		this.count = count;
	}
	
	public AverageUnit(BigDecimal amount) {
		this(amount, new BigDecimal(1));
	}

	public void add(BigDecimal amount) {
		if (amount == null) {
			return;
		}
		this.amount = this.amount == null ? amount : this.amount.add(amount);
		this.count = this.count == null ? new BigDecimal(1) : this.count.add(new BigDecimal(1));
	}

	public void add(CalculateUnit calculateUnit) {
		AverageUnit data = null;
		if (calculateUnit instanceof AverageUnit) {
			data = (AverageUnit) calculateUnit;
		}
		if (data == null || data.getAmount() == null || data.getCount() == null) {
			return;
		}
		this.amount = this.amount == null ? data.getAmount() : this.amount.add(data.getAmount());
		this.count = this.count == null ? data.getCount() : this.count.add(data.getCount());
	}

	@Override
	public BigDecimal getValue() {
		if (this.amount == null || this.count == null) {
			return null;
		}
		return this.amount.divide(count);
	}

	public BigDecimal getAmount() {
		return amount;
	}

	public void setAmount(BigDecimal amount) {
		this.amount = amount;
	}

	public BigDecimal getCount() {
		return count;
	}

	public void setCount(BigDecimal count) {
		this.count = count;
	}

	public static AverageUnit newInstance(String data) throws JsonParseException, JsonMappingException, IOException {
		if (SparkUtils.isNull(data)) {
			return null;
		}
		ObjectMapper objectMapper = new ObjectMapper();
		objectMapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		AverageUnit readValue = objectMapper.readValue(data, AverageUnit.class);
		return readValue;
	}
	
	@JsonIgnore
	public String getJsonString() throws JsonGenerationException, JsonMappingException, IOException {
		ObjectMapper objectMapper = new ObjectMapper();
		return objectMapper.writeValueAsString(this);
	}

	@Override
	public String toString() {
		try {
			return getJsonString();
		} catch (JsonGenerationException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return "AverageUnit [amount=" + amount + ", count=" + count + "]";
	}

}
