package com.jje.bigdata.userProfile.hotel.domain;

import java.io.Serializable;

import com.jje.bigdata.inf.SparkHasRowkeyAble;
import com.jje.bigdata.userProfile.travel.domain.BigdataMemberInfo;

public class TempHotelMemberInfo implements Serializable, SparkHasRowkeyAble {
	private static final long serialVersionUID = 1L;

	private String ROWKEY;

	private HotelOrderInfo order;
	private BigdataMemberInfo member;

	public TempHotelMemberInfo() {
		super();
	}

	public String getROWKEY() {
		return ROWKEY;
	}

	public void setROWKEY(String rOWKEY) {
		ROWKEY = rOWKEY;
	}

	public HotelOrderInfo getOrder() {
		return order;
	}

	public void setOrder(HotelOrderInfo order) {
		this.order = order;
	}

	public BigdataMemberInfo getMember() {
		return member;
	}

	public void setMember(BigdataMemberInfo member) {
		this.member = member;
	}

	@Override
	public String getIdentify() {
		return getROWKEY();
	}

	@Override
	public void setIdentify(String rowkey) {
		setROWKEY(rowkey);
	}

}
