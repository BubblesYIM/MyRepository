package com.jje.bigdata.userProfile.travel.domain;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.jje.bigdata.util.SparkUtils;

public class TravelRouteInfo implements Serializable {
	private static final long serialVersionUID = 1L;
	private String airLines;
	private Set<String> cities = new HashSet<String>();
	private Set<String> nations = new HashSet<String>();
	private Set<String> continents = new HashSet<String>();

	public String getAirLines() {
		return airLines;
	}

	public void setAirLines(String airLines) {
		this.airLines = airLines;
	}

	public Set<String> getCities() {
		return cities;
	}

	public void setCities(Set<String> cities) {
		this.cities = cities;
	}

	public void addCity(String city, Map<String, Map<String, String>> cityMap) {
		if (SparkUtils.isNull(city)) {
			return;
		}
		this.cities.add(city);
		Map<String, String> map = cityMap.get(city);
		if (map == null) {
			return;
		}
		addNation(map.get("cnation"));
		addContinent(map.get("ccontinent"));
	}

	public Set<String> getNations() {
		return nations;
	}

	public void setNations(Set<String> nations) {
		this.nations = nations;
	}

	public void addNation(String nation) {
		if (SparkUtils.isNull(nation)) {
			return;
		}
		this.nations.add(nation);
	}

	public Set<String> getContinents() {
		return continents;
	}

	public void setContinents(Set<String> continents) {
		this.continents = continents;
	}

	public void addContinent(String continent) {
		if (SparkUtils.isNull(continent)) {
			return;
		}
		this.continents.add(continent);
	}

}
