package com.jje.bigdata.userProfile.app.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.URL;
import java.net.URLConnection;

import net.sf.json.JSONObject;

import org.json.JSONException;

import com.jje.bigdata.userProfile.app.domain.AddressDto;

public class AddressUtils implements Serializable {
	private static final long serialVersionUID = 1L;
	private static final double EARTH_RADIUS = 6378.137;   
	
	
	private static double rad(double d){   
	     return d * Math.PI / 180.0;   
	}
	
	public static double getDistance(double lat1, double lng1, double lat2, double lng2){
	    double radLat1 = rad(lat1);   
	    double radLat2 = rad(lat2);   
	    double a = radLat1 - radLat2;   
	    double b = rad(lng1) - rad(lng2);   
	    double s = 2 * Math.asin(Math.sqrt(Math.pow(Math.sin(a/2),2) + Math.cos(radLat1)*Math.cos(radLat2)*Math.pow(Math.sin(b/2),2)));   
	    s = s * EARTH_RADIUS;
	    s = ((double)Math.round(s * 100)) / 100;
	    return s;   
	}
	
	public static double getDistance(String lat1, String lng1, String lat2, String lng2){
		return getDistance(Double.parseDouble(lat1), Double.parseDouble(lng1), Double.parseDouble(lat2), Double.parseDouble(lng2));
	}
	
	public static AddressDto getCityByLocation(double lon, double lat) throws IOException, JSONException {
		String location = String.format("%s,%s", lon,lat);
		return getCityByLocationString(location);
	}
	
	
	public static AddressDto getCityByLocationString(String location) throws IOException, JSONException {
		String url = String.format("http://172.24.88.137:6066/porsche/maps/getCityByLocationString/%s", location);
		String addressInfo = getResponse(url);
		JSONObject json = JSONObject.fromObject(addressInfo);
		AddressDto dto = (AddressDto)JSONObject.toBean(json, AddressDto.class);
		return dto;
	}
	
	private static String getResponse(String url) throws IOException {
		String result = "";
        BufferedReader in = null;
        try {
            URL realUrl = new URL(url);
            // 打开和URL之间的连接
            URLConnection connection = realUrl.openConnection();
            // 设置通用的请求属性
            connection.setRequestProperty("accept", "*/*");
            connection.setRequestProperty("connection", "Keep-Alive");
            connection.setRequestProperty("user-agent",
                    "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1;SV1)");
            // 建立实际的连接
            connection.connect();
            // 定义 BufferedReader输入流来读取URL的响应
            in = new BufferedReader(new InputStreamReader(
                    connection.getInputStream(),"UTF-8"));
            String line;
            while ((line = in.readLine()) != null) {
                result += line;
            }
        } catch (IOException e) {
           throw e;
        }finally {
            try {
                if (in != null) {
                    in.close();
                }
            } catch (Exception e2) {
                e2.printStackTrace();
            }
        }
        return result;
	}


	public static void main(String[] args) throws IOException, JSONException {
//		AddressDto dto = getCityByLocationString("39.76623,116.43213");
//		System.out.println(dto.getCity());
//		System.out.println(dto.getCountry());
//		String testrString = "{\"abc\" : \"123\"}";
//		JSONObject json = JSONObject.fromObject(testrString);
//		
//		System.out.println(json);
//		System.out.println(json.get("abc"));
		//31.232550, 121.458487   31.218170, 121.489197
		double distance = getDistance("43.802536079618996", "87.59878367213852", "41.7510109990396", "86.1532322941948");
		System.out.println(distance);
	}
}
