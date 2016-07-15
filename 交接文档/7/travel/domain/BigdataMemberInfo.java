package com.jje.bigdata.userProfile.travel.domain;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.jje.bigdata.census.ThreadSafeDateUtils;
import com.jje.bigdata.inf.SparkHasRowkeyAble;

public class BigdataMemberInfo implements Serializable,SparkHasRowkeyAble {
	private static final long serialVersionUID = 1L;
	private String ROWKEY;
	private String CONTACT_ID;
	private String PERSON_UID;
	private String GUEST_ID;
	private String NAME;
	private String EMAIL;
	private String PHONE;
	private String ID;
	private String ID_TYPE;
	private String MC_CODE;
	private String MEMBER_ID;
	private String CARD_NUM;

	public String getROWKEY() {
		return ROWKEY;
	}

	public void setROWKEY(String rOWKEY) {
		ROWKEY = rOWKEY;
	}

	public String getCONTACT_ID() {
		return CONTACT_ID;
	}

	public void setCONTACT_ID(String cONTACT_ID) {
		CONTACT_ID = cONTACT_ID;
	}

	public String getPERSON_UID() {
		return PERSON_UID;
	}

	public void setPERSON_UID(String pERSON_UID) {
		PERSON_UID = pERSON_UID;
	}

	public String getGUEST_ID() {
		return GUEST_ID;
	}

	public void setGUEST_ID(String gUEST_ID) {
		GUEST_ID = gUEST_ID;
	}

	public String getNAME() {
		return NAME;
	}

	public void setNAME(String nAME) {
		NAME = nAME;
	}

	public String getEMAIL() {
		return EMAIL;
	}

	public void setEMAIL(String eMAIL) {
		EMAIL = eMAIL;
	}

	public String getPHONE() {
		return PHONE;
	}

	public void setPHONE(String pHONE) {
		PHONE = pHONE;
	}

	public String getID() {
		return ID;
	}

	public void setID(String iD) {
		ID = iD;
	}

	public String getID_TYPE() {
		return ID_TYPE;
	}

	public void setID_TYPE(String iD_TYPE) {
		ID_TYPE = iD_TYPE;
	}

	public String getMC_CODE() {
		return MC_CODE;
	}

	public void setMC_CODE(String mC_CODE) {
		MC_CODE = mC_CODE;
	}

	public String getMEMBER_ID() {
		return MEMBER_ID;
	}

	public void setMEMBER_ID(String mEMBER_ID) {
		MEMBER_ID = mEMBER_ID;
	}

	public String getCARD_NUM() {
		return CARD_NUM;
	}

	public void setCARD_NUM(String cARD_NUM) {
		CARD_NUM = cARD_NUM;
	}
	
	public String getFieldData(String name) {
		try {
			Field field = BigdataMemberInfo.class.getDeclaredField(name);
			field.setAccessible(true);
			return (String) field.get(this);
		} catch (Exception e) {
						e.printStackTrace();
		}
		return null;
	}

	public void fillFieldData(Map<String, String> dataMap) {
		Field[] fields = BigdataMemberInfo.class.getDeclaredFields();
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
	
	
	public void fillFieldDataFormTravelUserProfile(Map<String, String> dataMap) throws Exception {
		Field[] fields = BigdataMemberInfo.class.getDeclaredFields();
		Map<String, Field> fieldMap = new HashMap<String, Field>();
		for (Field field : fields) {
			fieldMap.put("M_" + field.getName(), field);
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
					if(type == Date.class){
						Date data = ThreadSafeDateUtils.secParse(dataStr);
						field.set(this, data);
					}else if(type == Integer.class){
						int data = Integer.parseInt(dataStr);
						field.set(this, data);
					}else if(type == Double.class){
						double data = Double.parseDouble(dataStr);
						field.set(this, data);
					}else{
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
		return "BigdataMemberInfo [ROWKEY=" + ROWKEY + ", CONTACT_ID=" + CONTACT_ID + ", PERSON_UID=" + PERSON_UID + ", GUEST_ID=" + GUEST_ID + ", NAME=" + NAME + ", EMAIL="
				+ EMAIL + ", PHONE=" + PHONE + ", ID=" + ID + ", ID_TYPE=" + ID_TYPE + ", MC_CODE=" + MC_CODE + ", MEMBER_ID=" + MEMBER_ID + ", CARD_NUM=" + CARD_NUM + "]";
	}

	@Override
	public int hashCode() {
		return ((ROWKEY == null) ? 0 : ROWKEY.hashCode());
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		BigdataMemberInfo other = (BigdataMemberInfo) obj;
		if (ROWKEY == null) {
			if (other.ROWKEY != null)
				return false;
		} else if (!ROWKEY.equals(other.ROWKEY))
			return false;
		return true;
	}

	@Override
	public String getIdentify() {
		return this.ROWKEY;
	}

	@Override
	public void setIdentify(String rowkey) {
		setROWKEY(rowkey);
	}
	
}
