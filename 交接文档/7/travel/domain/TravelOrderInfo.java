package com.jje.bigdata.userProfile.travel.domain;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.jje.bigdata.census.ThreadSafeDateUtils;

public class TravelOrderInfo implements Serializable {
	private static final long serialVersionUID = 1L;
	private String ROWKEY;
	private String CRM_TXN_ROWKEY;
	private String CRM_TXN_X_ORDER_NUM;
	private String CRM_TXN_X_GROUP_CODE;
	private String CRM_TXN_MEMBER_ID;
	private String CRM_TXN_X_CONTACT_ID;
	private String CRM_MEM_NAME;
	private String CRM_CON_ROW_ID;
	private String CRM_CON_PERSON_UID;
	private String CRM_CON_FST_NAME;
	private String CRM_CON_LAST_NAME;
	private String JZL_GUEST_ROWKEY;
	private String JZL_GUEST_UTEAMID;
	private String JZL_GUEST_CNAME;
	private String JZL_GUEST_CMOBILETEL;
	private String JZL_GUEST_CEMAIL;
	private String JZL_GUEST_CIDCARD;
	private String JZL_GUEST_UORDERID;
	private String JZL_GUEST_UMEMBERID;
	private String JZL_ORDER_CORDERNO;
	private String JZL_ORDER_UMEMBERID;
	private String JZL_ORDER_CCONTACT;
	private String JZL_ORDER_UCUSTOMERID;
	private String JZL_ORDER_CORDERSOURCE;
	private String JZL_ORDER_DSIGNUP;
	private String JZL_ORDER_CCONFIRMED;
	private String JZL_ORDER_UCARDID;
	private String JZL_TEAM_CTEAMCODE;
	private String TBP_ORDER_ID;
	private String TBP_ORDER_CODE;
	private String TBP_ORDER_MC_MEMBER_CODE;
	private String TBP_ORDER_GROUP_ID;
	private String TBP_GUEST_ID;
	private String TBP_GUEST_NAME;
	private String TBP_GROUP_CODE;
	private Date departDate;// 出发日期
	private Date bookingDate; // 预定日期
	private String departHoliday;// 出发日期节假日
	private String cStatus; //
	private String resource; // 订单来源
	private Integer days;// 出行天数
	private String payType;// 支付方式
	private Double price;
    private String businCategory;//国内-国外 来自TBP
    private String coptype;//国内-国外  来自JZL
    private String airLines;//航空公司
	private String ccontracttype;//合同类型
	private String ctype;
	private String clinename;
	private String journeyCities;//旅行城市
	private String journeyNations;//旅行国家
	private String journeyContinents;//旅行大洲

	public String getClinename() {
		return clinename;
	}

	public void setClinename(String clinename) {
		this.clinename = clinename;
	}

	public String getJourneyCities() {
		return journeyCities;
	}

	public void setJourneyCities(String journeyCities) {
		this.journeyCities = journeyCities;
	}

	public String getJourneyNations() {
		return journeyNations;
	}

	public void setJourneyNations(String journeyNations) {
		this.journeyNations = journeyNations;
	}

	public String getJourneyContinents() {
		return journeyContinents;
	}

	public void setJourneyContinents(String journeyContinents) {
		this.journeyContinents = journeyContinents;
	}

	public String getCtype() {
		return ctype;
	}

	public void setCtype(String ctype) {
		this.ctype = ctype;
	}



	public String getCcontracttype() {
		return ccontracttype;
	}

	public void setCcontracttype(String ccontracttype) {
		this.ccontracttype = ccontracttype;
	}

	public String getAirLines() {
		return airLines;
	}

	public void setAirLines(String airLines) {
		this.airLines = airLines;
	}

	public String getCoptype() {
        return coptype;
    }

    public void setCoptype(String coptype) {
        this.coptype = coptype;
    }

    public String getBusinCategory() {
        return businCategory;
    }

    public void setBusinCategory(String businCategory) {
        this.businCategory = businCategory;
    }

    public Double getPrice() {
		return price;
	}

	public void setPrice(Double price) {
		this.price = price;
	}

	public String getPayType() {
		return payType;
	}

	public void setPayType(String payType) {
		this.payType = payType;
	}

	public Integer getDays() {
		return days;
	}

	public void setDays(Integer days) {
		this.days = days;
	}

	public String getResource() {
		return resource;
	}

	public void setResource(String resource) {
		this.resource = resource;
	}

	public String getcStatus() {
		return cStatus;
	}

	public void setcStatus(String cStatus) {
		this.cStatus = cStatus;
	}

	public Date getBookingDate() {
		return bookingDate;
	}

	public void setBookingDate(Date bookingDate) {
		this.bookingDate = bookingDate;
	}
	
	public String getDepartHoliday() {
		return departHoliday;
	}

	public void setDepartHoliday(String departHoliday) {
		this.departHoliday = departHoliday;
	}

	public Date getDepartDate() {
		return departDate;
	}

	public void setDepartDate(Date departDate) {
		this.departDate = departDate;
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

	public void setCRM_TXN_ROWKEY(String cRM_TXN_ROWKEY) {
		CRM_TXN_ROWKEY = cRM_TXN_ROWKEY;
	}

	public String getCRM_TXN_X_ORDER_NUM() {
		return CRM_TXN_X_ORDER_NUM;
	}

	public void setCRM_TXN_X_ORDER_NUM(String cRM_TXN_X_ORDER_NUM) {
		CRM_TXN_X_ORDER_NUM = cRM_TXN_X_ORDER_NUM;
	}

	public String getCRM_TXN_X_GROUP_CODE() {
		return CRM_TXN_X_GROUP_CODE;
	}

	public void setCRM_TXN_X_GROUP_CODE(String cRM_TXN_X_GROUP_CODE) {
		CRM_TXN_X_GROUP_CODE = cRM_TXN_X_GROUP_CODE;
	}

	public String getCRM_TXN_MEMBER_ID() {
		return CRM_TXN_MEMBER_ID;
	}

	public void setCRM_TXN_MEMBER_ID(String cRM_TXN_MEMBER_ID) {
		CRM_TXN_MEMBER_ID = cRM_TXN_MEMBER_ID;
	}

	public String getCRM_TXN_X_CONTACT_ID() {
		return CRM_TXN_X_CONTACT_ID;
	}

	public void setCRM_TXN_X_CONTACT_ID(String cRM_TXN_X_CONTACT_ID) {
		CRM_TXN_X_CONTACT_ID = cRM_TXN_X_CONTACT_ID;
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

	public String getJZL_GUEST_ROWKEY() {
		return JZL_GUEST_ROWKEY;
	}

	public void setJZL_GUEST_ROWKEY(String jZL_GUEST_ROWKEY) {
		JZL_GUEST_ROWKEY = jZL_GUEST_ROWKEY;
	}

	public String getJZL_GUEST_UTEAMID() {
		return JZL_GUEST_UTEAMID;
	}

	public void setJZL_GUEST_UTEAMID(String jZL_GUEST_UTEAMID) {
		JZL_GUEST_UTEAMID = jZL_GUEST_UTEAMID;
	}

	public String getJZL_GUEST_CNAME() {
		return JZL_GUEST_CNAME;
	}

	public void setJZL_GUEST_CNAME(String jZL_GUEST_CNAME) {
		JZL_GUEST_CNAME = jZL_GUEST_CNAME;
	}

	public String getJZL_GUEST_CMOBILETEL() {
		return JZL_GUEST_CMOBILETEL;
	}

	public void setJZL_GUEST_CMOBILETEL(String jZL_GUEST_CMOBILETEL) {
		JZL_GUEST_CMOBILETEL = jZL_GUEST_CMOBILETEL;
	}

	public String getJZL_GUEST_CEMAIL() {
		return JZL_GUEST_CEMAIL;
	}

	public void setJZL_GUEST_CEMAIL(String jZL_GUEST_CEMAIL) {
		JZL_GUEST_CEMAIL = jZL_GUEST_CEMAIL;
	}

	public String getJZL_GUEST_CIDCARD() {
		return JZL_GUEST_CIDCARD;
	}

	public void setJZL_GUEST_CIDCARD(String jZL_GUEST_CIDCARD) {
		JZL_GUEST_CIDCARD = jZL_GUEST_CIDCARD;
	}

	public String getJZL_GUEST_UORDERID() {
		return JZL_GUEST_UORDERID;
	}

	public void setJZL_GUEST_UORDERID(String jZL_GUEST_UORDERID) {
		JZL_GUEST_UORDERID = jZL_GUEST_UORDERID;
	}

	public String getJZL_GUEST_UMEMBERID() {
		return JZL_GUEST_UMEMBERID;
	}

	public void setJZL_GUEST_UMEMBERID(String jZL_GUEST_UMEMBERID) {
		JZL_GUEST_UMEMBERID = jZL_GUEST_UMEMBERID;
	}

	public String getJZL_ORDER_CORDERNO() {
		return JZL_ORDER_CORDERNO;
	}

	public void setJZL_ORDER_CORDERNO(String jZL_ORDER_CORDERNO) {
		JZL_ORDER_CORDERNO = jZL_ORDER_CORDERNO;
	}

	public String getJZL_ORDER_UMEMBERID() {
		return JZL_ORDER_UMEMBERID;
	}

	public void setJZL_ORDER_UMEMBERID(String jZL_ORDER_UMEMBERID) {
		JZL_ORDER_UMEMBERID = jZL_ORDER_UMEMBERID;
	}

	public String getJZL_ORDER_CCONTACT() {
		return JZL_ORDER_CCONTACT;
	}

	public void setJZL_ORDER_CCONTACT(String jZL_ORDER_CCONTACT) {
		JZL_ORDER_CCONTACT = jZL_ORDER_CCONTACT;
	}

	public String getJZL_ORDER_UCUSTOMERID() {
		return JZL_ORDER_UCUSTOMERID;
	}

	public void setJZL_ORDER_UCUSTOMERID(String jZL_ORDER_UCUSTOMERID) {
		JZL_ORDER_UCUSTOMERID = jZL_ORDER_UCUSTOMERID;
	}

	public String getJZL_ORDER_CORDERSOURCE() {
		return JZL_ORDER_CORDERSOURCE;
	}

	public void setJZL_ORDER_CORDERSOURCE(String jZL_ORDER_CORDERSOURCE) {
		JZL_ORDER_CORDERSOURCE = jZL_ORDER_CORDERSOURCE;
	}

	public String getJZL_ORDER_DSIGNUP() {
		return JZL_ORDER_DSIGNUP;
	}

	public void setJZL_ORDER_DSIGNUP(String jZL_ORDER_DSIGNUP) {
		JZL_ORDER_DSIGNUP = jZL_ORDER_DSIGNUP;
	}

	public String getJZL_ORDER_CCONFIRMED() {
		return JZL_ORDER_CCONFIRMED;
	}

	public void setJZL_ORDER_CCONFIRMED(String jZL_ORDER_CCONFIRMED) {
		JZL_ORDER_CCONFIRMED = jZL_ORDER_CCONFIRMED;
	}

	public String getJZL_ORDER_UCARDID() {
		return JZL_ORDER_UCARDID;
	}

	public void setJZL_ORDER_UCARDID(String jZL_ORDER_UCARDID) {
		JZL_ORDER_UCARDID = jZL_ORDER_UCARDID;
	}

	public String getJZL_TEAM_CTEAMCODE() {
		return JZL_TEAM_CTEAMCODE;
	}

	public void setJZL_TEAM_CTEAMCODE(String jZL_TEAM_CTEAMCODE) {
		JZL_TEAM_CTEAMCODE = jZL_TEAM_CTEAMCODE;
	}

	public String getTBP_ORDER_ID() {
		return TBP_ORDER_ID;
	}

	public void setTBP_ORDER_ID(String tBP_ORDER_ID) {
		TBP_ORDER_ID = tBP_ORDER_ID;
	}

	public String getTBP_ORDER_CODE() {
		return TBP_ORDER_CODE;
	}

	public void setTBP_ORDER_CODE(String tBP_ORDER_CODE) {
		TBP_ORDER_CODE = tBP_ORDER_CODE;
	}

	public String getTBP_ORDER_MC_MEMBER_CODE() {
		return TBP_ORDER_MC_MEMBER_CODE;
	}

	public void setTBP_ORDER_MC_MEMBER_CODE(String tBP_ORDER_MC_MEMBER_CODE) {
		TBP_ORDER_MC_MEMBER_CODE = tBP_ORDER_MC_MEMBER_CODE;
	}

	public String getTBP_ORDER_GROUP_ID() {
		return TBP_ORDER_GROUP_ID;
	}

	public void setTBP_ORDER_GROUP_ID(String tBP_ORDER_GROUP_ID) {
		TBP_ORDER_GROUP_ID = tBP_ORDER_GROUP_ID;
	}

	public String getTBP_GUEST_ID() {
		return TBP_GUEST_ID;
	}

	public void setTBP_GUEST_ID(String tBP_GUEST_ID) {
		TBP_GUEST_ID = tBP_GUEST_ID;
	}

	public String getTBP_GUEST_NAME() {
		return TBP_GUEST_NAME;
	}

	public void setTBP_GUEST_NAME(String tBP_GUEST_NAME) {
		TBP_GUEST_NAME = tBP_GUEST_NAME;
	}

	public String getTBP_GROUP_CODE() {
		return TBP_GROUP_CODE;
	}

	public void setTBP_GROUP_CODE(String tBP_GROUP_CODE) {
		TBP_GROUP_CODE = tBP_GROUP_CODE;
	}
	
	public String getFieldData(String name) {
		try {
			Field field = TravelOrderInfo.class.getDeclaredField(name);
			field.setAccessible(true);
			return (String) field.get(this);
		} catch (Exception e) {
						e.printStackTrace();
		}
		return null;
	}
	
	public void fillFieldData(String name, String data) {
		try {
			Field field = TravelOrderInfo.class.getDeclaredField(name);
			field.setAccessible(true);
			if (field.getType().equals(String.class)) {
				field.set(this, data);
				return;
			}
			try {
				String firstLetter = name.substring(0, 1).toUpperCase();
				String setMethodName = "parse" + firstLetter + name.substring(1);
				Method parseMethod = TravelOrderInfo.class.getMethod(setMethodName, String.class);
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
		Field[] fields = TravelOrderInfo.class.getDeclaredFields();
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
		Field[] fields = TravelOrderInfo.class.getDeclaredFields();
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

	@Override
	public String toString() {
		return "TravelOrderInfo [ROWKEY=" + ROWKEY + ", CRM_TXN_ROWKEY=" + CRM_TXN_ROWKEY + ", CRM_TXN_X_ORDER_NUM=" + CRM_TXN_X_ORDER_NUM + ", CRM_TXN_X_GROUP_CODE=" + CRM_TXN_X_GROUP_CODE + ", CRM_TXN_MEMBER_ID=" + CRM_TXN_MEMBER_ID + ", CRM_TXN_X_CONTACT_ID=" + CRM_TXN_X_CONTACT_ID + ", CRM_MEM_NAME=" + CRM_MEM_NAME + ", CRM_CON_ROW_ID=" + CRM_CON_ROW_ID + ", CRM_CON_PERSON_UID=" + CRM_CON_PERSON_UID + ", CRM_CON_FST_NAME=" + CRM_CON_FST_NAME + ", CRM_CON_LAST_NAME=" + CRM_CON_LAST_NAME
				+ ", JZL_GUEST_ROWKEY=" + JZL_GUEST_ROWKEY + ", JZL_GUEST_UTEAMID=" + JZL_GUEST_UTEAMID + ", JZL_GUEST_CNAME=" + JZL_GUEST_CNAME + ", JZL_GUEST_CMOBILETEL=" + JZL_GUEST_CMOBILETEL + ", JZL_GUEST_CEMAIL=" + JZL_GUEST_CEMAIL + ", JZL_GUEST_CIDCARD=" + JZL_GUEST_CIDCARD + ", JZL_GUEST_UORDERID=" + JZL_GUEST_UORDERID + ", JZL_GUEST_UMEMBERID=" + JZL_GUEST_UMEMBERID + ", JZL_ORDER_CORDERNO=" + JZL_ORDER_CORDERNO + ", JZL_ORDER_UMEMBERID=" + JZL_ORDER_UMEMBERID
				+ ", JZL_ORDER_CCONTACT=" + JZL_ORDER_CCONTACT + ", JZL_ORDER_UCUSTOMERID=" + JZL_ORDER_UCUSTOMERID + ", JZL_ORDER_CORDERSOURCE=" + JZL_ORDER_CORDERSOURCE + ", JZL_ORDER_DSIGNUP=" + JZL_ORDER_DSIGNUP + ", JZL_ORDER_CCONFIRMED=" + JZL_ORDER_CCONFIRMED + ", JZL_ORDER_UCARDID=" + JZL_ORDER_UCARDID + ", JZL_TEAM_CTEAMCODE=" + JZL_TEAM_CTEAMCODE + ", TBP_ORDER_ID=" + TBP_ORDER_ID + ", TBP_ORDER_CODE=" + TBP_ORDER_CODE + ", TBP_ORDER_MC_MEMBER_CODE=" + TBP_ORDER_MC_MEMBER_CODE
				+ ", TBP_ORDER_GROUP_ID=" + TBP_ORDER_GROUP_ID + ", TBP_GUEST_ID=" + TBP_GUEST_ID + ", TBP_GUEST_NAME=" + TBP_GUEST_NAME + ", TBP_GROUP_CODE=" + TBP_GROUP_CODE + ", departDate=" + departDate + ", bookingDate=" + bookingDate + ", departHoliday=" + departHoliday + ", cStatus=" + cStatus + ", resource=" + resource + ", days=" + days + ", payType=" + payType + ", price=" + price + ", businCategory=" + businCategory + ", coptype=" + coptype + ", airLines=" + airLines + "]";
	}

}
