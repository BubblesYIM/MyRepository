package com.jje.bigdata.userProfile.hotel.domain;

import java.io.Serializable;
import java.util.Date;

import com.jje.bigdata.userProfile.travel.domain.BigdataMemberInfo;
import com.jje.bigdata.userProfile.travel.domain.NumberMap;

public class HotelUserProfileInfo implements Serializable {

	private static final long serialVersionUID = 1L;

	private BigdataMemberInfo member = null;
	private HotelOrderInfo order = null;
	
	//标签结果相关数据
	private NumberMap hotelBrand = null;
	private NumberMap hotelStar = null;
	private NumberMap hotelName = null;
	private NumberMap hotelCity = null;
	private NumberMap checkInMonth = null;
	private NumberMap monthAndHotelCity = null;
	private NumberMap checkInMonthAndHotel = null;
	private NumberMap holidays = null;
	private NumberMap userRequire = null;
	private NumberMap bookingChannel = null;
	private NumberMap rateType = null;
	private NumberMap payResource = null;
	private int bookingCount = 0;
	private int checkInCount = 0;
	private Double checkInDays = 0.0d;
	private Double sumConsumption = 0.0d;
	private Double lastConsumption =0.0d;
	
	private Date lastCheckInTime = null;
	private Date bookingTime = null;
	
	public NumberMap getPayResource() {
		return payResource;
	}

	public void setPayResource(NumberMap payResource) {
		this.payResource = payResource;
	}

	public Double getCheckInDays() {
		return checkInDays;
	}

	public void setCheckInDays(Double checkInDays) {
		this.checkInDays = checkInDays;
	}

	public NumberMap getRateType() {
		return rateType;
	}

	public void setRateType(NumberMap rateType) {
		this.rateType = rateType;
	}

	public Date getBookingTime() {
		return bookingTime;
	}

	public void setBookingTime(Date bookingTime) {
		this.bookingTime = bookingTime;
	}

	public int getBookingCount() {
		return bookingCount;
	}

	public void setBookingCount(int bookingCount) {
		this.bookingCount = bookingCount;
	}

	public int getCheckInCount() {
		return checkInCount;
	}

	public void setCheckInCount(int checkInCount) {
		this.checkInCount = checkInCount;
	}

	public NumberMap getBookingChannel() {
		return bookingChannel;
	}

	public void setBookingChannel(NumberMap bookingChannel) {
		this.bookingChannel = bookingChannel;
	}

	public Double getLastConsumption() {
		return lastConsumption;
	}

	public void setLastConsumption(Double lastConsumption) {
		this.lastConsumption = lastConsumption;
	}

	public Double getSumConsumption() {
		return sumConsumption;
	}

	public void setSumConsumption(Double sumConsumption) {
		this.sumConsumption = sumConsumption;
	}

	public NumberMap getUserRequire() {
		return userRequire;
	}

	public void setUserRequire(NumberMap userRequire) {
		this.userRequire = userRequire;
	}

	public NumberMap getCheckInMonthAndHotel() {
		return checkInMonthAndHotel;
	}

	public void setCheckInMonthAndHotel(NumberMap checkInMonthAndHotel) {
		this.checkInMonthAndHotel = checkInMonthAndHotel;
	}

	public NumberMap getHolidays() {
		return holidays;
	}

	public void setHolidays(NumberMap holidays) {
		this.holidays = holidays;
	}

	public HotelUserProfileInfo(HotelOrderInfo order, BigdataMemberInfo member) {
		super();
		this.member = member;
		this.order = order;
	}

	public NumberMap getMonthAndHotelCity() {
		return monthAndHotelCity;
	}

	public void setMonthAndHotelCity(NumberMap monthAndHotelCity) {
		this.monthAndHotelCity = monthAndHotelCity;
	}

	public NumberMap getCheckInMonth() {
		return checkInMonth;
	}

	public void setCheckInMonth(NumberMap checkInMonth) {
		this.checkInMonth = checkInMonth;
	}

	public NumberMap getHotelCity() {
		return hotelCity;
	}

	public void setHotelCity(NumberMap hotelCity) {
		this.hotelCity = hotelCity;
	}

	public NumberMap getHotelName() {
		return hotelName;
	}

	public void setHotelName(NumberMap hotelName) {
		this.hotelName = hotelName;
	}

	public Date getLastCheckInTime() {
		return lastCheckInTime;
	}

	public void setLastCheckInTime(Date lastCheckInTime) {
		this.lastCheckInTime = lastCheckInTime;
	}

	public NumberMap getHotelBrand() {
		return hotelBrand;
	}

	public void setHotelBrand(NumberMap hotelBrand) {
		this.hotelBrand = hotelBrand;
	}

	public NumberMap getHotelStar() {
		return hotelStar;
	}

	public void setHotelStar(NumberMap hotelStar) {
		this.hotelStar = hotelStar;
	}

	public HotelUserProfileInfo() {
		super();
	}
	
	public BigdataMemberInfo getMember() {
		return member;
	}

	public void setMember(BigdataMemberInfo member) {
		this.member = member;
	}

	public HotelOrderInfo getOrder() {
		return order;
	}

	public void setOrder(HotelOrderInfo order) {
		this.order = order;
	}

}
