package com.jje.bigdata.travel.recommend;

import com.jje.bigdata.travel.bigdata.domain.Hotel;
import com.jje.bigdata.util.SparkUtils;
import org.apache.spark.api.java.function.Function;

import java.sql.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public abstract class MyFunction<T, R> extends Function<T, R> {
	private static ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);
	private static final long serialVersionUID = 1L;
	Connection conn = null;
	Statement sta = null;
	Map<String, Hotel> hotelMap = null;

	static AtomicInteger aint = new AtomicInteger();

	public static String getDBName() {
		int i = aint.incrementAndGet();
		return "DB" + i;
	}

	public MyFunction(Map<String, Hotel> hotelMap) throws Exception {
		this.hotelMap = hotelMap;
	}

	public MyFunction() {
		super();
	}

	void inidata() throws Exception {
		Class.forName("org.hsqldb.jdbcDriver");
		String daname = getDBName();
		conn = DriverManager.getConnection("jdbc:hsqldb:mem:" + daname, "sa", "");
		sta = conn.createStatement();
		sta.execute("create table tb_hotel_info(hotel_id int,brand char(10),star char(10),longitude double,latitude double)");
		// sta.execute("CREATE  INDEX PUBLIC.IDX_DEPARTMENT ON hotel (brand, star)");
		sta.execute("CREATE  INDEX PUBLIC.IDX_longitude ON tb_hotel_info(longitude)");
		sta.execute("CREATE  INDEX PUBLIC.IDX_latitude ON tb_hotel_info(latitude)");
		sta.execute("CREATE  INDEX PUBLIC.IDX_brand ON tb_hotel_info(brand)");
		sta.execute("CREATE  INDEX PUBLIC.IDX_star ON tb_hotel_info(star)");

		for(Map.Entry<String, Hotel> entry:hotelMap.entrySet()){
			Hotel hotel = entry.getValue();

			String brand = hotel.getBrand();
			String star = hotel.getBusinessStarRating();

			star = SparkUtils.isNull(star) ? "INN" : star;
			String lon = hotel.getLOCATION_LONGITUDE();
			String lat = hotel.getLOCATION_LATITUDE();

			if(SparkUtils.isNull(lon) || SparkUtils.isNull(lat)){
				continue;
			}
			String hotelId = entry.getKey();
			String sql = "insert into tb_hotel_info(hotel_id,brand,star,longitude,latitude) values("+ hotelId +",'" + brand + "','" + star + "'," + lon + "," + lat + ")";
			sta.execute(sql);
		}


//		ResultSet resultSet = sta.executeQuery("select count(1) from tb_hotel_info");
//
//		throw new RuntimeException(resultSet.getString(1));
	}

	public List<String> findHotels(double minlongitude, double maxlongitude, double minlatitude, double maxlatitude,
                                   Set<String> brands, Set<String> stars) throws SQLException {

		try {
			if(conn==null){
				inidata();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		String brandStr = "(";
		for(String brand:brands) {
			brandStr += "'" + brand + "',";
		}
		brandStr = brandStr.substring(0, brandStr.length()-1) + ")";

		String starStr = "(";
		for(String star:stars) {
			starStr += "'" + star + "',";
		}
		starStr = starStr.substring(0, starStr.length()-1) + ")";

		List<String> hotels = new ArrayList<String>();

		ResultSet rs = sta.executeQuery("select hotel_id from tb_hotel_info where longitude between " + minlongitude + " and " + maxlongitude
				+ " and latitude between " + minlatitude + " and " + maxlatitude
				+ " and brand in " + brandStr
				+ " and star in " + starStr);
		while (rs.next()) {
			hotels.add(rs.getString(1));
		}
//		throw new RuntimeException((endDate - initDate) + "====================================" + (stopDate - startDate) + "==================================" + (System.currentTimeMillis() - stopDate));
		return hotels;
	}

	public String[] findHotelInfo(String hotel_id) throws SQLException {
		if(conn==null){
			try {
				inidata();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		ResultSet rs = sta.executeQuery("select brand,star from tb_hotel_info where hotel_id=" + hotel_id);
    	if(rs.next()) {
            return new String[]{rs.getString(1).trim(),rs.getString(2).trim()};
        }
		return null;
	}


}
