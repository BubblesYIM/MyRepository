package com.jje.bigdata.userProfile.app.domain;

import java.io.Serializable;

public class AddressDto implements Serializable {
	private static final long serialVersionUID = 1L;
	
	private String country;
	private String province;
	private String city;

	public String getCountry() {
		return country;
	}

	public void setCountry(String country) {
		this.country = country;
	}

	public String getProvince() {
		return province;
	}

	public void setProvince(String province) {
		this.province = province;
	}

	public String getCity() {
		return city;
	}

	public void setCity(String city) {
		this.city = city;
	}

	@Override
	public String toString() {
		return "AddressDto [country=" + country + ", province=" + province + ", city=" + city + "]";
	}
	
}
