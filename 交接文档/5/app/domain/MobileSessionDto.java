package com.jje.bigdata.userProfile.app.domain;

import java.io.Serializable;
import java.util.Date;

public class MobileSessionDto implements Serializable {
	private static final long serialVersionUID = 2744255484856834346L;
	private String sessionID;
	private Date startViewTime;
	private Date endViewTime;
	private String rowkey;
	private String location;
	private String advertID;
	private String osVersion;
	private String dviceRowkey;
	private String userID;
	private String deviceID;
	
	public String getDeviceID() {
		return deviceID;
	}

	public void setDeviceID(String deviceID) {
		this.deviceID = deviceID;
	}

	public String getOsVersion() {
		return osVersion;
	}

	public void setOsVersion(String osVersion) {
		this.osVersion = osVersion;
	}

	public String getUserID() {
		return userID;
	}

	public void setUserID(String userID) {
		this.userID = userID;
	}

	public String getDviceRowkey() {
		return dviceRowkey;
	}

	public void setDviceRowkey(String dviceRowkey) {
		this.dviceRowkey = dviceRowkey;
	}

	public String getAdvertID() {
		return advertID;
	}

	public void setAdvertID(String advertID) {
		this.advertID = advertID;
	}

	public String getLocation() {
		return location;
	}

	public void setLocation(String location) {
		this.location = location;
	}

	public String getSessionID() {
		return sessionID;
	}

	public void setSessionID(String sessionID) {
		this.sessionID = sessionID;
	}

	public Date getStartViewTime() {
		return startViewTime;
	}

	public void setStartViewTime(Date startViewTime) {
		this.startViewTime = startViewTime;
	}

	public Date getEndViewTime() {
		return endViewTime;
	}

	public void setEndViewTime(Date endViewTime) {
		this.endViewTime = endViewTime;
	}

	public String getRowkey() {
		return rowkey;
	}

	public void setRowkey(String rowkey) {
		this.rowkey = rowkey;
	}

	@Override
	public String toString() {
		return "MobileSessionDto [sessionID=" + sessionID + ", startViewTime=" + startViewTime + ", endViewTime=" + endViewTime + ", rowkey=" + rowkey + ", location=" + location + ", advertID=" + advertID + ", osVersion=" + osVersion + ", dviceRowkey=" + dviceRowkey + ", userID=" + userID + ", deviceID=" + deviceID + "]";
	}

}
