package com.jje.bigdata.userProfile.hotel.domain;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.annotate.JsonIgnore;

import com.jje.bigdata.census.ThreadSafeDateUtils;
import com.jje.bigdata.inf.SparkHasRowkeyAble;
import com.jje.bigdata.util.SparkUtils;

public class HotelOrderInfo implements Serializable, SparkHasRowkeyAble {

	private static final long serialVersionUID = -2875639051898752290L;

	private String ROWKEY;

	// TB_HOTEL_CRM_TXN_MEMBER_CONTACT_MAP
	private String CRM_TXN_ROWKEY;
	private String CRM_TXN_MEMBER_ID;//同住人
	private String CRM_TXN_X_ORDER_NUM;
	private String CRM_TXN_X_HOTEL_ID;
	private String CRM_TXN_X_HOTEL_NAME;
	private String CRM_TXN_X_CONTACT_ID;
	private String CRM_TXN_CREATED_BY;
	private String CRM_TXN_START_DT;
	private String CRM_TXN_END_DT;
	private String CRM_TXN_AMT_VAL;
	private String CRM_TXN_COMMENTS;
	private String CRM_MEM_NAME;
	private String CRM_CON_ROW_ID;//同住人
	private String CRM_CON_PERSON_UID;
	private String CRM_CON_FST_NAME;
	private String CRM_CON_LAST_NAME;
	
	// JJ000_SIEBEL_T_TXN_LIST_MAP
	private String PMS_TXN_ROWKEY;
	private String PMS_TXN_ROW_ID;
	private String PMS_TXN_SOURCE_TYPE; //JJ001 星级酒店 JJ002锦江之星 JJ003锦江旅游
	private String PMS_TXN_MEMBER_NAME;
	private String PMS_TXN_MEMBER_CDTYPE;
	private String PMS_TXN_MEMBER_CARDID;
	private String PMS_TXN_START_DATE;
	private String PMS_TXN_END_DATE;
	private String PMS_TXN_TXN_COST;
	private String PMS_TXN_ATTRIB_01;
	private String PMS_TXN_RECORD_ID;

	// PMS_HT_Reception_MAP
	// 锦江之星入住接待单表
	private String PMS_REC_ROWKEY;
	private String PMS_REC_ResvId;
	private String PMS_REC_Name;
	private String PMS_REC_UnitId;
	private String PMS_REC_ArrDT;
	private String PMS_REC_DepDT;

	// PMS_HT_Resv_MAP
	// 锦江之星预订订单相关表
	private String PMS_RESV_ROWKEY;
	private String PMS_RESV_Name;
	private String PMS_RESV_OldArrDT;
	private String PMS_RESV_OldDepDT;
	private String PMS_RESV_ResvUnitNm;
	private String PMS_RESV_ResvUnitid;

	// PMS_HT_ResvDetail_MAP
	private String PMS_DETAIL_ROWKEY;
	private String PMS_DETAIL_ResvId;
	private String PMS_DETAIL_ArrDT;//到店時間
	private String PMS_DETAIL_DepDT;

	// JJ000_WEBSITE_T_HBP_ORDER_MAP
	private String HBP_ORDER_ROWKEY;
	private String HBP_ORDER_FIRST_SUB_ORDER_NO;
	private String HBP_ORDER_ORDER_NO;
	private String HBP_ORDER_JREZ_ORDER_NO;
	private String HBP_ORDER_HOTEL_ID;
	private String HBP_ORDER_HOTEL_JJ_CODE;
	private String HBP_ORDER_HOTEL_JREZ_CODE;
	private String HBP_ORDER_GUESTS;
	private String HBP_ORDER_ARRIVAL_DATE;
	private String HBP_ORDER_CHECK_OUT_DATE;

	// 与结果表相关的业务字段
	private String orderNo;// 预定单号:来源+"_"+订单号
	private String days;// 预定天数
	private String roomNights;// 预定天数
	private Date bookingDate;// 预定时间
	private Date storeTime;// 预定入住时间
	private Date departTime;// 预定离店时间
	private String roomNum;// 预定房间数
	private String rateType;//预定类型
	private String rateGroupType;//预定大类
	private String bookingPrice;//预定价格
	private String actualPrice;//实际价格
	private String descript;//用户备注
	private String paymentType;//支付类型  现付 预付
	private Date actualStoreTime;//实际入住时间
	private Date actualDepartTime;//实际离店时间
	private String status;//预定状态
	private Date updateTime;//更新时间
	private String channel;//预定来源
	private String parttners;//同住人
	private String holidays;
	
	//酒店相关
	private String hotelName;
	private String hotelID;
	private String hotelBrand;
	private String hotelStar;
	private String hotelCity;
	
	//支付相关
	private String payResource;//支付渠道
	private String payChannel;//支付来源
	private String webFee;//网上预付金额
	
	public String getWebFee() {
		return webFee;
	}

	public void setWebFee(String webFee) {
		this.webFee = webFee;
	}

	public String getHolidays() {
		return holidays;
	}

	public void setHolidays(String holidays) {
		this.holidays = holidays;
	}

	public String getRoomNights() {
		return roomNights;
	}

	public void setRoomNights(String roomNights) {
		this.roomNights = roomNights;
	}

	public String getPayChannel() {
		return payChannel;
	}

	public void setPayChannel(String payChannel) {
		this.payChannel = payChannel;
	}

	public String getPayResource() {
		return payResource;
	}

	public void setPayResource(String payResource) {
		this.payResource = payResource;
	}

	public String getParttners() {
		return parttners;
	}

	public void setParttners(String parttners) {
		this.parttners = parttners;
	}

	public String getHotelName() {
		return hotelName;
	}

	public void setHotelName(String hotelName) {
		this.hotelName = hotelName;
	}

	public String getHotelID() {
		return hotelID;
	}

	public void setHotelID(String hotelID) {
		this.hotelID = hotelID;
	}

	public String getHotelBrand() {
		return hotelBrand;
	}

	public void setHotelBrand(String hotelBrand) {
		this.hotelBrand = hotelBrand;
	}

	public String getHotelStar() {
		return hotelStar;
	}

	public void setHotelStar(String hotelStar) {
		this.hotelStar = hotelStar;
	}

	public String getHotelCity() {
		return hotelCity;
	}

	public void setHotelCity(String hotelCity) {
		this.hotelCity = hotelCity;
	}

	public String getChannel() {
		return channel;
	}

	public void setChannel(String channel) {
		this.channel = channel;
	}

	public String getBookingPrice() {
		return bookingPrice;
	}

	public void setBookingPrice(String bookingPrice) {
		this.bookingPrice = bookingPrice;
	}

	public Date getUpdateTime() {
		return updateTime;
	}

	public void setUpdateTime(Date updateTime) {
		this.updateTime = updateTime;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public Date getActualDepartTime() {
		return actualDepartTime;
	}

	public void setActualDepartTime(Date actualDepartTime) {
		this.actualDepartTime = actualDepartTime;
	}

	public Date getActualStoreTime() {
		return actualStoreTime;
	}

	public void setActualStoreTime(Date actualStoreTime) {
		this.actualStoreTime = actualStoreTime;
	}

	public String getPaymentType() {
		return paymentType;
	}

	public void setPaymentType(String paymentType) {
		this.paymentType = paymentType;
	}

	public String getDescript() {
		return descript;
	}

	public void setDescript(String descript) {
		this.descript = descript;
	}

	public String getActualPrice() {
		return actualPrice;
	}

	public void setActualPrice(String actualPirce) {
		this.actualPrice = actualPirce;
	}

	public String getRateGroupType() {
		return rateGroupType;
	}

	public void setRateGroupType(String rateGroupType) {
		this.rateGroupType = rateGroupType;
	}

	public String getRateType() {
		return rateType;
	}

	public void setRateType(String rateType) {
		this.rateType = rateType;
	}

	public String getRoomNum() {
		return roomNum;
	}

	public void setRoomNum(String roomNum) {
		this.roomNum = roomNum;
	}

	public String getDays() {
		return days;
	}

	public void setDays(String days) {
		this.days = days;
	}

	public Date getStoreTime() {
		return storeTime;
	}

	public void setStoreTime(Date storeTime) {
		this.storeTime = storeTime;
	}

	public Date getDepartTime() {
		return departTime;
	}

	public void setDepartTime(Date departTime) {
		this.departTime = departTime;
	}

	public String getOrderNo() {
		return orderNo;
	}

	public void setOrderNo(String orderNo) {
		this.orderNo = orderNo;
	}

	public Date getBookingDate() {
		return bookingDate;
	}

	public void setBookingDate(Date bookingDate) {
		this.bookingDate = bookingDate;
	}

	public String getROWKEY() {
		return ROWKEY;
	}

	public void setROWKEY(String rOWKEY) {
		ROWKEY = rOWKEY;
	}

	public String getCRM_TXN_ROWKEY() {
		return CRM_TXN_ROWKEY;
	}

	public void setCRM_TXN_ROWKEY(String cRM_TXN_ROW_ID) {
		CRM_TXN_ROWKEY = cRM_TXN_ROW_ID;
	}

	public String getCRM_TXN_MEMBER_ID() {
		return CRM_TXN_MEMBER_ID;
	}

	public void setCRM_TXN_MEMBER_ID(String cRM_TXN_MEMBER_ID) {
		CRM_TXN_MEMBER_ID = cRM_TXN_MEMBER_ID;
	}

	public String getCRM_TXN_X_ORDER_NUM() {
		return CRM_TXN_X_ORDER_NUM;
	}

	public void setCRM_TXN_X_ORDER_NUM(String cRM_TXN_X_ORDER_NUM) {
		CRM_TXN_X_ORDER_NUM = cRM_TXN_X_ORDER_NUM;
	}

	public String getCRM_TXN_X_HOTEL_ID() {
		return CRM_TXN_X_HOTEL_ID;
	}

	public void setCRM_TXN_X_HOTEL_ID(String cRM_TXN_X_HOTEL_ID) {
		CRM_TXN_X_HOTEL_ID = cRM_TXN_X_HOTEL_ID;
	}

	public String getCRM_TXN_X_HOTEL_NAME() {
		return CRM_TXN_X_HOTEL_NAME;
	}

	public void setCRM_TXN_X_HOTEL_NAME(String cRM_TXN_X_HOTEL_Name) {
		CRM_TXN_X_HOTEL_NAME = cRM_TXN_X_HOTEL_Name;
	}

	public String getCRM_TXN_X_CONTACT_ID() {
		return CRM_TXN_X_CONTACT_ID;
	}

	public void setCRM_TXN_X_CONTACT_ID(String cRM_TXN_X_CONTACT_ID) {
		CRM_TXN_X_CONTACT_ID = cRM_TXN_X_CONTACT_ID;
	}

	public String getCRM_TXN_CREATED_BY() {
		return CRM_TXN_CREATED_BY;
	}

	public void setCRM_TXN_CREATED_BY(String cRM_TXN_CREATED_BY) {
		CRM_TXN_CREATED_BY = cRM_TXN_CREATED_BY;
	}

	public String getCRM_TXN_START_DT() {
		return CRM_TXN_START_DT;
	}

	public void setCRM_TXN_START_DT(String cRM_TXN_START_DT) {
		CRM_TXN_START_DT = cRM_TXN_START_DT;
	}

	public String getCRM_TXN_END_DT() {
		return CRM_TXN_END_DT;
	}

	public void setCRM_TXN_END_DT(String cRM_TXN_END_DT) {
		CRM_TXN_END_DT = cRM_TXN_END_DT;
	}

	public String getCRM_TXN_AMT_VAL() {
		return CRM_TXN_AMT_VAL;
	}

	public void setCRM_TXN_AMT_VAL(String cRM_TXN_AMT_VAL) {
		CRM_TXN_AMT_VAL = cRM_TXN_AMT_VAL;
	}

	public String getCRM_TXN_COMMENTS() {
		return CRM_TXN_COMMENTS;
	}

	public void setCRM_TXN_COMMENTS(String cRM_TXN_COMMENTS) {
		CRM_TXN_COMMENTS = cRM_TXN_COMMENTS;
	}

	public String getCRM_MEM_NAME() {
		return CRM_MEM_NAME;
	}

	public void setCRM_MEM_NAME(String cRM_MEM_NAME) {
		CRM_MEM_NAME = cRM_MEM_NAME;
	}

	public String getCRM_CON_ROW_ID() {
		return CRM_CON_ROW_ID;
	}

	public void setCRM_CON_ROW_ID(String cRM_CON_ROW_ID) {
		CRM_CON_ROW_ID = cRM_CON_ROW_ID;
	}

	public String getCRM_CON_PERSON_UID() {
		return CRM_CON_PERSON_UID;
	}

	public void setCRM_CON_PERSON_UID(String cRM_CON_PERSON_UID) {
		CRM_CON_PERSON_UID = cRM_CON_PERSON_UID;
	}

	public String getCRM_CON_FST_NAME() {
		return CRM_CON_FST_NAME;
	}

	public void setCRM_CON_FST_NAME(String cRM_CON_FST_NAME) {
		CRM_CON_FST_NAME = cRM_CON_FST_NAME;
	}

	public String getCRM_CON_LAST_NAME() {
		return CRM_CON_LAST_NAME;
	}

	public void setCRM_CON_LAST_NAME(String cRM_CON_LAST_NAME) {
		CRM_CON_LAST_NAME = cRM_CON_LAST_NAME;
	}

	public String getPMS_REC_ROWKEY() {
		return PMS_REC_ROWKEY;
	}

	public void setPMS_REC_ROWKEY(String pMS_REC_ROWKEY) {
		PMS_REC_ROWKEY = pMS_REC_ROWKEY;
	}

	public String getPMS_REC_ResvId() {
		return PMS_REC_ResvId;
	}

	public void setPMS_REC_ResvId(String pMS_REC_ResvId) {
		PMS_REC_ResvId = pMS_REC_ResvId;
	}

	public String getPMS_REC_Name() {
		return PMS_REC_Name;
	}

	public void setPMS_REC_Name(String pMS_REC_Name) {
		PMS_REC_Name = pMS_REC_Name;
	}

	public String getPMS_REC_ArrDT() {
		return PMS_REC_ArrDT;
	}

	public void setPMS_REC_ArrDT(String pMS_REC_ArrDT) {
		PMS_REC_ArrDT = pMS_REC_ArrDT;
	}

	public String getPMS_REC_DepDT() {
		return PMS_REC_DepDT;
	}

	public void setPMS_REC_DepDT(String pMS_REC_DepDT) {
		PMS_REC_DepDT = pMS_REC_DepDT;
	}

	public String getPMS_REC_UnitId() {
		return PMS_REC_UnitId;
	}

	public void setPMS_REC_UnitId(String pMS_REC_UnitId) {
		PMS_REC_UnitId = pMS_REC_UnitId;
	}

	public String getPMS_RESV_ROWKEY() {
		return PMS_RESV_ROWKEY;
	}

	public void setPMS_RESV_ROWKEY(String pMS_RESV_ROWKEY) {
		PMS_RESV_ROWKEY = pMS_RESV_ROWKEY;
	}

	public String getPMS_RESV_Name() {
		return PMS_RESV_Name;
	}

	public void setPMS_RESV_Name(String pMS_RESV_Name) {
		PMS_RESV_Name = pMS_RESV_Name;
	}

	public String getPMS_RESV_OldArrDT() {
		return PMS_RESV_OldArrDT;
	}

	public void setPMS_RESV_OldArrDT(String pMS_RESV_OldArrDT) {
		PMS_RESV_OldArrDT = pMS_RESV_OldArrDT;
	}

	public String getPMS_RESV_OldDepDT() {
		return PMS_RESV_OldDepDT;
	}

	public void setPMS_RESV_OldDepDT(String pMS_RESV_OldDepDT) {
		PMS_RESV_OldDepDT = pMS_RESV_OldDepDT;
	}

	public String getPMS_RESV_ResvUnitNm() {
		return PMS_RESV_ResvUnitNm;
	}

	public void setPMS_RESV_ResvUnitNm(String pMS_RESV_ResvUnitNm) {
		PMS_RESV_ResvUnitNm = pMS_RESV_ResvUnitNm;
	}

	public String getPMS_RESV_ResvUnitid() {
		return PMS_RESV_ResvUnitid;
	}

	public void setPMS_RESV_ResvUnitid(String pMS_RESV_ResvUnitid) {
		PMS_RESV_ResvUnitid = pMS_RESV_ResvUnitid;
	}

	public String getPMS_DETAIL_ROWKEY() {
		return PMS_DETAIL_ROWKEY;
	}

	public void setPMS_DETAIL_ROWKEY(String pMS_DETAIL_ROWKEY) {
		PMS_DETAIL_ROWKEY = pMS_DETAIL_ROWKEY;
	}

	public String getPMS_DETAIL_ResvId() {
		return PMS_DETAIL_ResvId;
	}

	public void setPMS_DETAIL_ResvId(String pMS_DETAIL_ResvId) {
		PMS_DETAIL_ResvId = pMS_DETAIL_ResvId;
	}

	public String getPMS_DETAIL_ArrDT() {
		return PMS_DETAIL_ArrDT;
	}

	public void setPMS_DETAIL_ArrDT(String pMS_DETAIL_ArrDT) {
		PMS_DETAIL_ArrDT = pMS_DETAIL_ArrDT;
	}

	public String getPMS_DETAIL_DepDT() {
		return PMS_DETAIL_DepDT;
	}

	public void setPMS_DETAIL_DepDT(String pMS_DETAIL_DepDT) {
		PMS_DETAIL_DepDT = pMS_DETAIL_DepDT;
	}

	public String getHBP_ORDER_ROWKEY() {
		return HBP_ORDER_ROWKEY;
	}

	public void setHBP_ORDER_ROWKEY(String hBP_ORDER_ROWKEY) {
		HBP_ORDER_ROWKEY = hBP_ORDER_ROWKEY;
	}

	public String getHBP_ORDER_FIRST_SUB_ORDER_NO() {
		return HBP_ORDER_FIRST_SUB_ORDER_NO;
	}

	public void setHBP_ORDER_FIRST_SUB_ORDER_NO(String hBP_ORDER_FIRST_SUB_ORDER_NO) {
		HBP_ORDER_FIRST_SUB_ORDER_NO = hBP_ORDER_FIRST_SUB_ORDER_NO;
	}

	public String getHBP_ORDER_ORDER_NO() {
		return HBP_ORDER_ORDER_NO;
	}

	public void setHBP_ORDER_ORDER_NO(String hBP_ORDER_ORDER_NO) {
		HBP_ORDER_ORDER_NO = hBP_ORDER_ORDER_NO;
	}

	public String getHBP_ORDER_JREZ_ORDER_NO() {
		return HBP_ORDER_JREZ_ORDER_NO;
	}

	public void setHBP_ORDER_JREZ_ORDER_NO(String hBP_ORDER_JREZ_ORDER_NO) {
		HBP_ORDER_JREZ_ORDER_NO = hBP_ORDER_JREZ_ORDER_NO;
	}

	public String getHBP_ORDER_HOTEL_ID() {
		return HBP_ORDER_HOTEL_ID;
	}

	public void setHBP_ORDER_HOTEL_ID(String hBP_ORDER_HOTEL_ID) {
		HBP_ORDER_HOTEL_ID = hBP_ORDER_HOTEL_ID;
	}

	public String getHBP_ORDER_HOTEL_JJ_CODE() {
		return HBP_ORDER_HOTEL_JJ_CODE;
	}

	public void setHBP_ORDER_HOTEL_JJ_CODE(String hBP_ORDER_HOTEL_JJ_CODE) {
		HBP_ORDER_HOTEL_JJ_CODE = hBP_ORDER_HOTEL_JJ_CODE;
	}

	public String getHBP_ORDER_HOTEL_JREZ_CODE() {
		return HBP_ORDER_HOTEL_JREZ_CODE;
	}

	public void setHBP_ORDER_HOTEL_JREZ_CODE(String hBP_ORDER_HOTEL_JREZ_CODE) {
		HBP_ORDER_HOTEL_JREZ_CODE = hBP_ORDER_HOTEL_JREZ_CODE;
	}

	public String getHBP_ORDER_GUESTS() {
		return HBP_ORDER_GUESTS;
	}

	public void setHBP_ORDER_GUESTS(String hBP_ORDER_GUESTS) {
		HBP_ORDER_GUESTS = hBP_ORDER_GUESTS;
	}

	public String getHBP_ORDER_ARRIVAL_DATE() {
		return HBP_ORDER_ARRIVAL_DATE;
	}

	public void setHBP_ORDER_ARRIVAL_DATE(String hBP_ORDER_ARRIVAL_DATE) {
		HBP_ORDER_ARRIVAL_DATE = hBP_ORDER_ARRIVAL_DATE;
	}

	public String getHBP_ORDER_CHECK_OUT_DATE() {
		return HBP_ORDER_CHECK_OUT_DATE;
	}

	public void setHBP_ORDER_CHECK_OUT_DATE(String hBP_ORDER_CHECK_OUT_DATE) {
		HBP_ORDER_CHECK_OUT_DATE = hBP_ORDER_CHECK_OUT_DATE;
	}

	public String getPMS_TXN_ROWKEY() {
		return PMS_TXN_ROWKEY;
	}

	public void setPMS_TXN_ROWKEY(String pMS_TXN_ROWKEY) {
		PMS_TXN_ROWKEY = pMS_TXN_ROWKEY;
	}

	public String getPMS_TXN_ROW_ID() {
		return PMS_TXN_ROW_ID;
	}

	public void setPMS_TXN_ROW_ID(String pMS_TXN_ROW_ID) {
		PMS_TXN_ROW_ID = pMS_TXN_ROW_ID;
	}

	public String getPMS_TXN_SOURCE_TYPE() {
		return PMS_TXN_SOURCE_TYPE;
	}

	public void setPMS_TXN_SOURCE_TYPE(String pMS_TXN_SOURCE_TYPE) {
		PMS_TXN_SOURCE_TYPE = pMS_TXN_SOURCE_TYPE;
	}

	public String getPMS_TXN_MEMBER_NAME() {
		return PMS_TXN_MEMBER_NAME;
	}

	public void setPMS_TXN_MEMBER_NAME(String pMS_TXN_MEMBER_NAME) {
		PMS_TXN_MEMBER_NAME = pMS_TXN_MEMBER_NAME;
	}

	public String getPMS_TXN_MEMBER_CDTYPE() {
		return PMS_TXN_MEMBER_CDTYPE;
	}

	public void setPMS_TXN_MEMBER_CDTYPE(String pMS_TXN_MEMBER_CDTYPE) {
		PMS_TXN_MEMBER_CDTYPE = pMS_TXN_MEMBER_CDTYPE;
	}

	public String getPMS_TXN_MEMBER_CARDID() {
		return PMS_TXN_MEMBER_CARDID;
	}

	public void setPMS_TXN_MEMBER_CARDID(String pMS_TXN_MEMBER_CARDID) {
		PMS_TXN_MEMBER_CARDID = pMS_TXN_MEMBER_CARDID;
	}

	public String getPMS_TXN_START_DATE() {
		return PMS_TXN_START_DATE;
	}

	public void setPMS_TXN_START_DATE(String pMS_TXN_START_DATE) {
		PMS_TXN_START_DATE = pMS_TXN_START_DATE;
	}

	public String getPMS_TXN_END_DATE() {
		return PMS_TXN_END_DATE;
	}

	public void setPMS_TXN_END_DATE(String pMS_TXN_END_DATE) {
		PMS_TXN_END_DATE = pMS_TXN_END_DATE;
	}

	public String getPMS_TXN_TXN_COST() {
		return PMS_TXN_TXN_COST;
	}

	public void setPMS_TXN_TXN_COST(String pMS_TXN_TXN_COST) {
		PMS_TXN_TXN_COST = pMS_TXN_TXN_COST;
	}

	public String getPMS_TXN_ATTRIB_01() {
		return PMS_TXN_ATTRIB_01;
	}

	public void setPMS_TXN_ATTRIB_01(String pMS_TXN_ATTRIB_01) {
		PMS_TXN_ATTRIB_01 = pMS_TXN_ATTRIB_01;
	}

	public String getPMS_TXN_RECORD_ID() {
		return PMS_TXN_RECORD_ID;
	}

	public void setPMS_TXN_RECORD_ID(String pMS_TXN_RECORD_ID) {
		PMS_TXN_RECORD_ID = pMS_TXN_RECORD_ID;
	}

	public String getFieldData(String Name) {
		try {
			Field field = HotelOrderInfo.class.getDeclaredField(Name);
			field.setAccessible(true);
			return (String) field.get(this);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public void fillFieldDataWithPrefix(String prefix, Result data) throws IllegalAccessException {
		Field[] fields = HotelOrderInfo.class.getDeclaredFields();
		for (Field field : fields) {
			if (field.getName().indexOf(prefix) != 0) {
				continue;
			}
			field.setAccessible(true);
			String dateStr = Bytes.toString(data.getValue("info".getBytes(), field.getName().getBytes()));
			if (StringUtils.isBlank(dateStr)) {
				continue;
			}
			field.set(this, dateStr);
		}
	}

	public void fillFieldDataWithoutPrefix(String prefix, Result data) throws IllegalAccessException {
		Field[] fields = HotelOrderInfo.class.getDeclaredFields();
		for (Field field : fields) {
			if (field.getName().indexOf(prefix) != 0) {
				continue;
			}
			field.setAccessible(true);
			String dateStr = Bytes.toString(data.getValue("info".getBytes(), StringUtils.removeStart(field.getName(), prefix).getBytes()));
			if (StringUtils.isBlank(dateStr)) {
				continue;
			}
			field.set(this, dateStr);
		}
	}

	public void fillFieldData(String Name, String data) {
		try {
			Field field = HotelOrderInfo.class.getDeclaredField(Name);
			field.setAccessible(true);
			if (field.getType().equals(String.class)) {
				field.set(this, data);
				return;
			}
			try {
				String firstLetter = Name.substring(0, 1).toUpperCase();
				String setMethodName = "parse" + firstLetter + Name.substring(1);
				Method parseMethod = HotelOrderInfo.class.getMethod(setMethodName, String.class);
				parseMethod.invoke(this, data);
				return;
			} catch (Exception e) {
				e.printStackTrace();
			}
			Constructor<?> constructor = field.getType().getConstructor(String.class);
			field.set(this, constructor.newInstance(data));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public void fillFieldData(Map<String, String> dataMap) {
		Field[] fields = HotelOrderInfo.class.getDeclaredFields();
		Map<String, Field> fieldMap = new HashMap<String, Field>();
		for (Field field : fields) {
			fieldMap.put(field.getName(), field);
		}
		try {
			for (Map.Entry<String, String> entry : dataMap.entrySet()) {
				if (!fieldMap.containsKey(entry.getKey())) {
					continue;
				}
				Field field = fieldMap.get(entry.getKey());
				field.setAccessible(true);
				String dateStr = null;
				if (entry.getValue() instanceof String) {
					dateStr = ((String) entry.getValue()).trim();
				}
				if (StringUtils.isNotEmpty(dateStr)) {
					field.set(this, dateStr);
				}
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public void fillFieldDataFormTravelOrderInfo(Map<String, String> dataMap) throws Exception {
		Field[] fields = HotelOrderInfo.class.getDeclaredFields();
		Map<String, Field> fieldMap = new HashMap<String, Field>();
		for (Field field : fields) {
			fieldMap.put("O_" + field.getName(), field);
		}
		try {
			for (Map.Entry<String, String> entry : dataMap.entrySet()) {
				if (!fieldMap.containsKey(entry.getKey())) {
					continue;
				}
				Field field = fieldMap.get(entry.getKey());
				field.setAccessible(true);
				String dataStr = null;
				if (entry.getValue() instanceof String) {
					dataStr = ((String) entry.getValue()).trim();
				}
				if (StringUtils.isNotEmpty(dataStr)) {
					Class<?> type = field.getType();
					if (type == Date.class) {
						Date data = ThreadSafeDateUtils.secParse(dataStr);
						field.set(this, data);
					} else if (type == Integer.class) {
						int data = Integer.parseInt(dataStr);
						field.set(this, data);
					} else if (type == Double.class) {
						double data = Double.parseDouble(dataStr);
						field.set(this, data);
					} else {
						field.set(this, dataStr);
					}
				}
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public void joinData(HotelOrderInfo toJoin) throws IllegalAccessException {
		Field[] fields = HotelOrderInfo.class.getDeclaredFields();
		for (Field field : fields) {
			if ("serialVersionUID".equals(field.getName())) {
				continue;
			}
			field.setAccessible(true);
			Object toJoinValue = field.get(toJoin);
			String dateStr = null;
			if (toJoinValue instanceof String) {
				dateStr = (String) toJoinValue;

			}
			if (StringUtils.isBlank(dateStr)) {
				continue;
			}
			field.set(this, dateStr);
		}
	}

	@Override
	public String toString() {
		return "HotelOrderInfo [ROWKEY=" + ROWKEY + ", CRM_TXN_ROWKEY=" + CRM_TXN_ROWKEY + ", CRM_TXN_MEMBER_ID=" + CRM_TXN_MEMBER_ID + ", CRM_TXN_X_ORDER_NUM=" + CRM_TXN_X_ORDER_NUM + ", CRM_TXN_X_HOTEL_ID=" + CRM_TXN_X_HOTEL_ID + ", CRM_TXN_X_HOTEL_NAME=" + CRM_TXN_X_HOTEL_NAME + ", CRM_TXN_X_CONTACT_ID=" + CRM_TXN_X_CONTACT_ID + ", CRM_TXN_CREATED_BY=" + CRM_TXN_CREATED_BY + ", CRM_TXN_START_DT=" + CRM_TXN_START_DT + ", CRM_TXN_END_DT=" + CRM_TXN_END_DT + ", CRM_TXN_AMT_VAL="
				+ CRM_TXN_AMT_VAL + ", CRM_TXN_COMMENTS=" + CRM_TXN_COMMENTS + ", CRM_MEM_NAME=" + CRM_MEM_NAME + ", CRM_CON_ROW_ID=" + CRM_CON_ROW_ID + ", CRM_CON_PERSON_UID=" + CRM_CON_PERSON_UID + ", CRM_CON_FST_NAME=" + CRM_CON_FST_NAME + ", CRM_CON_LAST_NAME=" + CRM_CON_LAST_NAME + ", PMS_TXN_ROWKEY=" + PMS_TXN_ROWKEY + ", PMS_TXN_ROW_ID=" + PMS_TXN_ROW_ID + ", PMS_TXN_SOURCE_TYPE=" + PMS_TXN_SOURCE_TYPE + ", PMS_TXN_MEMBER_NAME=" + PMS_TXN_MEMBER_NAME + ", PMS_TXN_MEMBER_CDTYPE="
				+ PMS_TXN_MEMBER_CDTYPE + ", PMS_TXN_MEMBER_CARDID=" + PMS_TXN_MEMBER_CARDID + ", PMS_TXN_START_DATE=" + PMS_TXN_START_DATE + ", PMS_TXN_END_DATE=" + PMS_TXN_END_DATE + ", PMS_TXN_TXN_COST=" + PMS_TXN_TXN_COST + ", PMS_TXN_ATTRIB_01=" + PMS_TXN_ATTRIB_01 + ", PMS_TXN_RECORD_ID=" + PMS_TXN_RECORD_ID + ", PMS_REC_ROWKEY=" + PMS_REC_ROWKEY + ", PMS_REC_ResvId=" + PMS_REC_ResvId + ", PMS_REC_Name=" + PMS_REC_Name + ", PMS_REC_UnitId=" + PMS_REC_UnitId + ", PMS_REC_ArrDT="
				+ PMS_REC_ArrDT + ", PMS_REC_DepDT=" + PMS_REC_DepDT + ", PMS_RESV_ROWKEY=" + PMS_RESV_ROWKEY + ", PMS_RESV_Name=" + PMS_RESV_Name + ", PMS_RESV_OldArrDT=" + PMS_RESV_OldArrDT + ", PMS_RESV_OldDepDT=" + PMS_RESV_OldDepDT + ", PMS_RESV_ResvUnitNm=" + PMS_RESV_ResvUnitNm + ", PMS_RESV_ResvUnitid=" + PMS_RESV_ResvUnitid + ", PMS_DETAIL_ROWKEY=" + PMS_DETAIL_ROWKEY + ", PMS_DETAIL_ResvId=" + PMS_DETAIL_ResvId + ", PMS_DETAIL_ArrDT=" + PMS_DETAIL_ArrDT + ", PMS_DETAIL_DepDT="
				+ PMS_DETAIL_DepDT + ", HBP_ORDER_ROWKEY=" + HBP_ORDER_ROWKEY + ", HBP_ORDER_FIRST_SUB_ORDER_NO=" + HBP_ORDER_FIRST_SUB_ORDER_NO + ", HBP_ORDER_ORDER_NO=" + HBP_ORDER_ORDER_NO + ", HBP_ORDER_JREZ_ORDER_NO=" + HBP_ORDER_JREZ_ORDER_NO + ", HBP_ORDER_HOTEL_ID=" + HBP_ORDER_HOTEL_ID + ", HBP_ORDER_HOTEL_JJ_CODE=" + HBP_ORDER_HOTEL_JJ_CODE + ", HBP_ORDER_HOTEL_JREZ_CODE=" + HBP_ORDER_HOTEL_JREZ_CODE + ", HBP_ORDER_GUESTS=" + HBP_ORDER_GUESTS + ", HBP_ORDER_ARRIVAL_DATE="
				+ HBP_ORDER_ARRIVAL_DATE + ", HBP_ORDER_CHECK_OUT_DATE=" + HBP_ORDER_CHECK_OUT_DATE + ", orderNo=" + orderNo + ", days=" + days + ", bookingDate=" + bookingDate + ", storeTime=" + storeTime + ", departTime=" + departTime + ", roomNum=" + roomNum + ", rateType=" + rateType + ", rateGroupType=" + rateGroupType + ", bookingPrice=" + bookingPrice + ", actualPrice=" + actualPrice + ", descript=" + descript + ", paymentType=" + paymentType + ", actualStoreTime=" + actualStoreTime
				+ ", actualDepartTime=" + actualDepartTime + ", status=" + status + ", updateTime=" + updateTime + ", channel=" + channel + ", parttners=" + parttners + ", hotelName=" + hotelName + ", hotelID=" + hotelID + ", hotelBrand=" + hotelBrand + ", hotelStar=" + hotelStar + ", hotelCity=" + hotelCity + ", payResource=" + payResource + ", payChannel=" + payChannel + "]";
	}

	public String genROWKEY() {
		StringBuilder sb = new StringBuilder();
		sb.append(UUID.randomUUID());
		sb.append("#");
		sb.append(StringUtils.join(new String[] { this.CRM_TXN_ROWKEY, this.PMS_REC_ROWKEY, this.PMS_RESV_ROWKEY, this.PMS_DETAIL_ROWKEY, this.HBP_ORDER_ROWKEY }, "|"));
		this.ROWKEY = sb.toString();
		return this.ROWKEY;
	}

	@JsonIgnore
	public String getHBP_ORDER_ResvId() {
		if (HBP_ORDER_FIRST_SUB_ORDER_NO == null) {
			return null;
		}
		if (StringUtils.contains(HBP_ORDER_FIRST_SUB_ORDER_NO, "$")) {
			return StringUtils.substringAfterLast(HBP_ORDER_FIRST_SUB_ORDER_NO, "$");
		}
		return HBP_ORDER_FIRST_SUB_ORDER_NO;
	}

	@JsonIgnore
	public String getCRM_TXN_ResvId() {
		if (SparkUtils.isNotBlank(CRM_TXN_X_ORDER_NUM)) {
			return CRM_TXN_X_ORDER_NUM;
		}
		if ("0-1".equals(CRM_TXN_CREATED_BY)) {
			return null;
		}
		return CRM_TXN_COMMENTS;
	}
	
	@JsonIgnore
	private String getCRM_TXN_Name() {
		String name = CRM_MEM_NAME;
		if (SparkUtils.isBlank(name)) {
			name = CRM_CON_LAST_NAME;
		}
		return name;
	}
	
	@JsonIgnore
	public String getCRM_TXN_UNIQUE_KEY() {
		return StringUtils.join(new String[] { 
				getCRM_TXN_Name(), 
				StringUtils.substring(CRM_TXN_START_DT, 0, 10), 
				StringUtils.substring(CRM_TXN_END_DT, 0, 10),
				StringUtils.substringBefore(CRM_TXN_AMT_VAL, "."), 
				CRM_TXN_X_HOTEL_ID }, 
			"|");
	}
	
	@JsonIgnore
	public String getPMS_TXN_UNIQUE_KEY() {// member_name || start_date || end_date ||txn_cost||attrib_01
		return StringUtils.join(new String[] { 
					PMS_TXN_MEMBER_NAME, 
					StringUtils.substring(PMS_TXN_START_DATE, 0, 10), 
					StringUtils.substring(PMS_TXN_END_DATE, 0, 10),
					StringUtils.substringBefore(PMS_TXN_TXN_COST, "."), 
					PMS_TXN_ATTRIB_01 }, 
				"|");
	}
	
	@JsonIgnore
	public String getHotel_txn_name() {
		String name = getCRM_TXN_Name();
		if(SparkUtils.isBlank(name)){
			name = PMS_TXN_MEMBER_NAME;
		}
		return StringUtils.trim(name);
	}
	
	@JsonIgnore
	public String getHotel_order_name() {
		String name = PMS_REC_Name;
		if(SparkUtils.isBlank(name)){
			name = PMS_RESV_Name;
		}
		if(SparkUtils.isBlank(name)){
			name = HBP_ORDER_GUESTS;
		}
		return StringUtils.trim(name);
	}

	@Override
	public String getIdentify() {
		return getROWKEY();
	}

	@Override
	public void setIdentify(String rowkey) {
		setROWKEY(rowkey);
	}
	
	public boolean hasHotel_txn() {
		return SparkUtils.isNotBlank(CRM_TXN_ROWKEY) || SparkUtils.isNotBlank(PMS_TXN_ROWKEY);
	}

	@Override
	public int hashCode() {
		return this.getIdentify().hashCode();
	}
	
}
