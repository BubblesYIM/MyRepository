package com.jje.bigdata.userProfile.app.domain;

import com.jje.bigdata.userProfile.travel.domain.NumberMap;

import java.io.Serializable;
import java.util.Date;


public class MobileUserProfileInfo implements Serializable {
	private static final long serialVersionUID = 1L;

	private MobileUserBehaviorDto behavior = null;
	private MobileSessionDto session = null;

	private String rowkey;
	private NumberMap SystemOS;// 系统版本
	private NumberMap advertID;// 用户设备号
	private NumberMap LBSInfo;//LBS信息
	private NumberMap queryWords;//搜索关键字
	private Date lastOpenTime;
	private NumberMap bookingDistance; //预定位置与酒店位置,距离
	private NumberMap preferenceWithViewHour;//访问APP时间偏好
	private NumberMap appVisitTimes;//每次访问时间记录
	private NumberMap brand;//用户手机品牌
	private boolean hasBeenBooking;//是否预定过酒店
	private boolean hasFocusActivity;
	private boolean mapBooking;
	private NumberMap activitySessionNumber;
	private NumberMap zoneScreen;
	private NumberMap brandScreen;
	private NumberMap sortType;
	private NumberMap preferenceCity;

	public NumberMap getPreferenceCity() {
		return preferenceCity;
	}

	public void setPreferenceCity(NumberMap preferenceCity) {
		this.preferenceCity = preferenceCity;
	}

	public NumberMap getSortType() {
		return sortType;
	}

	public void setSortType(NumberMap sortType) {
		this.sortType = sortType;
	}

	public NumberMap getBrandScreen() {
		return brandScreen;
	}

	public void setBrandScreen(NumberMap brandScreen) {
		this.brandScreen = brandScreen;
	}

	public NumberMap getZoneScreen() {
		return zoneScreen;
	}

	public void setZoneScreen(NumberMap zoneScreen) {
		this.zoneScreen = zoneScreen;
	}

	public boolean isMapBooking() {
		return mapBooking;
	}

	public void setMapBooking(boolean mapBooking) {
		this.mapBooking = mapBooking;
	}

	public NumberMap getActivitySessionNumber() {
		return activitySessionNumber;
	}

	public void setActivitySessionNumber(NumberMap activitySessionNumber) {
		this.activitySessionNumber = activitySessionNumber;
	}

	public boolean isHasFocusActivity() {
		return hasFocusActivity;
	}

	public void setHasFocusActivity(boolean hasFocusActivity) {
		this.hasFocusActivity = hasFocusActivity;
	}

	public boolean isHasBeenBooking() {
		return hasBeenBooking;
	}

	public void setHasBeenBooking(boolean hasBeenBooking) {
		this.hasBeenBooking = hasBeenBooking;
	}

	public NumberMap getBrand() {
		return brand;
	}

	public void setBrand(NumberMap brand) {
		this.brand = brand;
	}

	public NumberMap getAppVisitTimes() {
		return appVisitTimes;
	}

	public void setAppVisitTimes(NumberMap appVisitTimes) {
		this.appVisitTimes = appVisitTimes;
	}

	public NumberMap getPreferenceWithViewHour() {
		return preferenceWithViewHour;
	}

	public void setPreferenceWithViewHour(NumberMap preferenceWithViewHour) {
		this.preferenceWithViewHour = preferenceWithViewHour;
	}

	public NumberMap getBookingDistance() {
		return bookingDistance;
	}

	public void setBookingDistance(NumberMap bookingDistance) {
		this.bookingDistance = bookingDistance;
	}

	public NumberMap getQueryWords() {
		return queryWords;
	}

	public void setQueryWords(NumberMap queryWords) {
		this.queryWords = queryWords;
	}

	public NumberMap getLBSInfo() {
		return LBSInfo;
	}

	public void setLBSInfo(NumberMap lBSInfo) {
		LBSInfo = lBSInfo;
	}

	public Date getLastOpenTime() {
		return lastOpenTime;
	}

	public void setLastOpenTime(Date lastOpenTime) {
		this.lastOpenTime = lastOpenTime;
	}

	public NumberMap getAdvertID() {
		return advertID;
	}

	public void setAdvertID(NumberMap advertID) {
		this.advertID = advertID;
	}

	public MobileUserBehaviorDto getBehavior() {
		return behavior;
	}

	public void setBehavior(MobileUserBehaviorDto behavior) {
		this.behavior = behavior;
	}

	public MobileSessionDto getSession() {
		return session;
	}

	public void setSession(MobileSessionDto session) {
		this.session = session;
	}

	public String getRowkey() {
		return rowkey;
	}

	public void setRowkey(String rowkey) {
		this.rowkey = rowkey;
	}

	public NumberMap getSystemOS() {
		return SystemOS;
	}

	public void setSystemOS(NumberMap systemOS) {
		SystemOS = systemOS;
	}

}
