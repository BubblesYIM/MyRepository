package com.jje.bigdata.userProfile.app.domain;

import java.io.Serializable;
import java.util.Date;

public class MobileUserBehaviorDto implements Serializable {
	private static final long serialVersionUID = 1L;
	private String rowkey;
	private String sessionID;
	private String userID;
	private String systemType;
	private String softVersion;
	private String location;
	private String realUserID;
	private BehaviorType type;
	private Date openTime;
	private Date beHaviorTime;
	private String descript;
	
	public String getRealUserID() {
		return realUserID;
	}

	public void setRealUserID(String realUserID) {
		this.realUserID = realUserID;
	}

	public String getUserID() {
		return userID;
	}

	public void setUserID(String userID) {
		this.userID = userID;
	}

	public String getRowkey() {
		return rowkey;
	}

	public void setRowkey(String rowkey) {
		this.rowkey = rowkey;
	}

	public String getSessionID() {
		return sessionID;
	}

	public void setSessionID(String sessionID) {
		this.sessionID = sessionID;
	}

	public String getSystemType() {
		return systemType;
	}

	public void setSystemType(String systemType) {
		this.systemType = systemType;
	}

	public String getSoftVersion() {
		return softVersion;
	}

	public void setSoftVersion(String softVersion) {
		this.softVersion = softVersion;
	}

	public String getLocation() {
		return location;
	}

	public void setLocation(String location) {
		this.location = location;
	}

	public BehaviorType getType() {
		return type;
	}

	public void setType(BehaviorType type) {
		this.type = type;
	}

	public Date getOpenTime() {
		return openTime;
	}

	public void setOpenTime(Date openTime) {
		this.openTime = openTime;
	}

	public Date getBeHaviorTime() {
		return beHaviorTime;
	}

	public void setBeHaviorTime(Date beHaviorTime) {
		this.beHaviorTime = beHaviorTime;
	}

	public String getDescript() {
		return descript;
	}

	public void setDescript(String descript) {
		this.descript = descript;
	}

	public enum BehaviorType {
		VIEW(),EVENT(),BUSINESS;
	}
	
	@Override
	public String toString() {
		return "MobileUserBehaviorDto [rowkey=" + rowkey + ", sessionID=" + sessionID + ", userID=" + userID + ", systemType=" + systemType + ", softVersion=" + softVersion + ", location=" + location + ", type=" + type + ", openTime=" + openTime + ", beHaviorTime=" + beHaviorTime + ", descript=" + descript + "]";
	}

}
