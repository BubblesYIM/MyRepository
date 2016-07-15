package com.jje.bigdata.userProfile.travel.domain;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;

import com.jje.bigdata.userProfile.travel.util.AverageUnit;
import com.jje.bigdata.userProfile.travel.util.GroupAverageMap;
import com.jje.bigdata.util.SparkUtils;

public class TravelUserProfileInfo implements Serializable {

	private static final long serialVersionUID = 1L;
	private static String[] excludeOutboundJourneyCities = new String[] { "浦东", "上海", "北京", "深圳", "南京", "舟山" };

	private BigdataMemberInfo member = null;
	private TravelOrderInfo order = null;

	private String rowkey;
	private NumberMap everyYearConsumptionCount;// 每年消费数量
	private NumberMap everyYearBookingCount;// 每年预定数量
	private NumberMap everyYearConsumptionAndBookingProportion;// 每年消费预定比
	private NumberMap consumptionAndBookingCount;// 消费预定数量
	private BigDecimal totalConsumptionAndBookingProportion;// 消费预定比
	private String status;// 预定还是消费(不输出)
	private Integer totalConsumptionCount;// 消费总数
	private Integer totalBookingCount;// 预定总数
	private Date lastTravelDate;// 最后一次旅行时间
	private String orderResource;// 订单来源
	private NumberMap BookingResourceCount;// 预定来源
	private List<Date> dates;// 旅游时间(不输出)
	private NumberMap travelfrequency;// 旅游频次
	private BigDecimal totalConsumptionAmount;// 旅游消费总金额
	private NumberMap totalConsumptionAmountPerYearMap;// 每年旅游消费总金额
	private BigDecimal totalBookingAmount;// 旅游预定总金额
	private NumberMap totalBookingAmountPerYearMap;// 每年旅游预定总金额
	private Date orderConsumptionDate;// 旅游消费时间
	private BigDecimal lastConsumptionAmount;// 最近一次旅游消费金额
	private NumberMap bookingJourneyDaysMap;// 旅游出行天数偏好
	private NumberMap bookingTimeByMonthMap;// 旅游预订时间偏好 (按月)
	private NumberMap bookingTimeByWeekDayMap;// 旅游预订时间习惯（按周）
	private NumberMap bookingTimeByHourMap;// 旅游预订时间偏好（按小时）
	private NumberMap departTimeByMonthMap;// 旅游出行时间偏好 (按月)
	private NumberMap departTimeByWeekDayMap;// 旅游出行时间偏好（按周）
	private NumberMap departTimeByHolidayMap;// 旅游出行时间偏好（按节假日）
	private AverageUnit bookingDomesticAmountAvg;// 境内游平均预订金额
	private GroupAverageMap bookingDomesticAmountPerYearAvg;// 每年境内游平均预订金额
	private AverageUnit bookingOutboundAmountAvg;// 出境游平均预订金额
	private GroupAverageMap bookingOutboundAmountPerYearAvg;// 每年出境游平均预订金额
	private NumberMap airLinesMap;//线路航空公司
	private NumberMap bookingBeforeDepartDaysMap;// 旅游提前预订天数偏好
	private NumberMap domesticJourneyCitiesMap;// 旅游城市(境内)
	private NumberMap outboundJourneyCitiesMap;// 旅游城市(出境)
	private NumberMap journeyNationsMap;// 旅行国家
	private NumberMap journeyContinentsMap;// 旅行大洲
	
	public NumberMap getAirLinesMap() {
		return airLinesMap;
	}

	public void setAirLinesMap(NumberMap airLinesMap) {
		this.airLinesMap = airLinesMap;
	}

	public NumberMap getEveryYearConsumptionCount() {
		return everyYearConsumptionCount;
	}

	public void setEveryYearConsumptionCount(NumberMap everyYearConsumptionCount) {
		this.everyYearConsumptionCount = everyYearConsumptionCount;
	}

	public NumberMap getEveryYearBookingCount() {
		return everyYearBookingCount;
	}

	public void setEveryYearBookingCount(NumberMap everyYearBookingCount) {
		this.everyYearBookingCount = everyYearBookingCount;
	}

	public NumberMap getEveryYearConsumptionAndBookingProportion() {
		return everyYearConsumptionAndBookingProportion;
	}

	public void setEveryYearConsumptionAndBookingProportion(NumberMap everyYearConsumptionAndBookingProportion) {
		this.everyYearConsumptionAndBookingProportion = everyYearConsumptionAndBookingProportion;
	}

	public NumberMap getConsumptionAndBookingCount() {
		return consumptionAndBookingCount;
	}

	public void setConsumptionAndBookingCount(NumberMap consumptionAndBookingCount) {
		this.consumptionAndBookingCount = consumptionAndBookingCount;
	}

	public BigDecimal getTotalConsumptionAndBookingProportion() {
		return totalConsumptionAndBookingProportion;
	}

	public void setTotalConsumptionAndBookingProportion(BigDecimal totalConsumptionAndBookingProportion) {
		this.totalConsumptionAndBookingProportion = totalConsumptionAndBookingProportion;
	}

	public Integer getTotalConsumptionCount() {
		return totalConsumptionCount;
	}

	public void setTotalConsumptionCount(Integer totalConsumptionCount) {
		this.totalConsumptionCount = totalConsumptionCount;
	}

	public Integer getTotalBookingCount() {
		return totalBookingCount;
	}

	public void setTotalBookingCount(Integer totalBookingCount) {
		this.totalBookingCount = totalBookingCount;
	}

	public NumberMap getBookingResourceCount() {
		return BookingResourceCount;
	}

	public void setBookingResourceCount(NumberMap bookingResourceCount) {
		BookingResourceCount = bookingResourceCount;
	}

	public NumberMap getTravelfrequency() {
		return travelfrequency;
	}

	public void setTravelfrequency(NumberMap travelfrequency) {
		this.travelfrequency = travelfrequency;
	}

	public List<Date> getDates() {
		return dates;
	}

	public void setDates(List<Date> dates) {
		this.dates = dates;
	}

	public TravelUserProfileInfo() {
		super();
	}

	public TravelUserProfileInfo(BigdataMemberInfo member, TravelOrderInfo order) {
		this();
		this.member = member;
		this.order = order;
	}

	public BigdataMemberInfo getMember() {
		return member;
	}

	public void setMember(BigdataMemberInfo member) {
		this.member = member;
	}

	public TravelOrderInfo getOrder() {
		return order;
	}

	public void setOrder(TravelOrderInfo order) {
		this.order = order;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public Date getLastTravelDate() {
		return lastTravelDate;
	}

	public void setLastTravelDate(Date lastTravelDate) {
		this.lastTravelDate = lastTravelDate;
	}

	public String getOrderResource() {
		return orderResource;
	}

	public void setOrderResource(String orderResource) {
		this.orderResource = orderResource;
	}

	public String getRowkey() {
		return rowkey;
	}

	public void setRowkey(String rowkey) {
		this.rowkey = rowkey;
	}

	public BigDecimal getTotalConsumptionAmount() {
		if (totalConsumptionAmount == null && order != null && order.getPrice() != null && "已出游".equals(order.getcStatus())) {
			totalConsumptionAmount = new BigDecimal(order.getPrice());
		}
		return totalConsumptionAmount;
	}

	public void setTotalConsumptionAmount(BigDecimal totalConsumptionAmount) {
		this.totalConsumptionAmount = totalConsumptionAmount;
	}

	public NumberMap getTotalConsumptionAmountPerYearMap() {
		if (totalConsumptionAmountPerYearMap == null && order != null && order.getBookingDate() != null && order.getPrice() != null && "已出游".equals(order.getcStatus())) {
			totalConsumptionAmountPerYearMap = new NumberMap(String.valueOf(order.getBookingDate().getYear() + 1900), new BigDecimal(order.getPrice()));
		}
		return totalConsumptionAmountPerYearMap;
	}

	public void setTotalConsumptionAmountPerYearMap(NumberMap totalConsumptionAmountPerYear) {
		this.totalConsumptionAmountPerYearMap = totalConsumptionAmountPerYear;
	}

	public BigDecimal getTotalBookingAmount() {
		if (totalBookingAmount == null && order != null && order.getPrice() != null) {
			totalBookingAmount = new BigDecimal(order.getPrice());
		}
		return totalBookingAmount;
	}

	public void setTotalBookingAmount(BigDecimal totalOrderAmount) {
		this.totalBookingAmount = totalOrderAmount;
	}

	public NumberMap getTotalBookingAmountPerYearMap() {
		if (totalBookingAmountPerYearMap == null && order != null && order.getBookingDate() != null && order.getPrice() != null) {
			totalBookingAmountPerYearMap = new NumberMap(String.valueOf(order.getBookingDate().getYear() + 1900), new BigDecimal(order.getPrice()));
		}
		return totalBookingAmountPerYearMap;
	}

	public void setTotalBookingAmountPerYearMap(NumberMap totalOrderAmountPerYear) {
		this.totalBookingAmountPerYearMap = totalOrderAmountPerYear;
	}

	public Date getOrderConsumptionDate() {
		if (orderConsumptionDate == null && order != null && order.getDepartDate() != null && "已出游".equals(order.getcStatus())) {
			orderConsumptionDate = order.getDepartDate();
		}
		return orderConsumptionDate;
	}

	public void setOrderConsumptionDate(Date orderConsumptionDate) {
		this.orderConsumptionDate = orderConsumptionDate;
	}

	public BigDecimal getLastConsumptionAmount() {
		if (lastConsumptionAmount == null && order != null && order.getPrice() != null && "已出游".equals(order.getcStatus())) {
			lastConsumptionAmount = new BigDecimal(order.getPrice());
		}
		return lastConsumptionAmount;
	}

	public void setLastConsumptionAmount(BigDecimal lastConsumptionAmount) {
		this.lastConsumptionAmount = lastConsumptionAmount;
	}

	public NumberMap getBookingJourneyDaysMap() {
		if (bookingJourneyDaysMap == null && order != null && order.getDays() != null) {
			int days = order.getDays();
			String level = null;
			if (days <= 3) {
				level = "0-3Days";
			} else if (days <= 6) {
				level = "4-6Days";
			} else if (days <= 8) {
				level = "7-8Days";
			} else if (days <= 10) {
				level = "9-10Days";
			} else {
				level = "11AndMoreDays";
			}

			if (level != null) {
				bookingJourneyDaysMap = new NumberMap(level, new BigDecimal(1));
			}
		}
		return bookingJourneyDaysMap;
	}

	public void setBookingJourneyDaysMap(NumberMap orderJourneyDaysMap) {
		this.bookingJourneyDaysMap = orderJourneyDaysMap;
	}

	public NumberMap getBookingTimeByMonthMap() {
		if (bookingTimeByMonthMap == null && order != null && order.getBookingDate() != null) {
			bookingTimeByMonthMap = new NumberMap(String.valueOf(order.getBookingDate().getMonth() + 1), new BigDecimal(1));
		}
		return bookingTimeByMonthMap;
	}

	public void setBookingTimeByMonthMap(NumberMap orderTimeByMonthMap) {
		this.bookingTimeByMonthMap = orderTimeByMonthMap;
	}

	public NumberMap getBookingTimeByWeekDayMap() {
		if (bookingTimeByWeekDayMap == null && order != null && order.getBookingDate() != null) {
			bookingTimeByWeekDayMap = new NumberMap(String.valueOf(order.getBookingDate().getDay()), new BigDecimal(1));
		}
		return bookingTimeByWeekDayMap;
	}

	public void setBookingTimeByWeekDayMap(NumberMap orderTimeByWeekDayMap) {
		this.bookingTimeByWeekDayMap = orderTimeByWeekDayMap;
	}

	public NumberMap getBookingTimeByHourMap() {
		if (bookingTimeByHourMap == null && order != null && order.getBookingDate() != null) {
			bookingTimeByHourMap = new NumberMap(String.valueOf(order.getBookingDate().getHours()), new BigDecimal(1));
		}
		return bookingTimeByHourMap;
	}

	public void setBookingTimeByHourMap(NumberMap orderTimeByHourMap) {
		this.bookingTimeByHourMap = orderTimeByHourMap;
	}

	public NumberMap getDepartTimeByMonthMap() {
		if (departTimeByMonthMap == null && order != null && order.getDepartDate() != null) {
			departTimeByMonthMap = new NumberMap(String.valueOf(order.getDepartDate().getMonth() + 1), new BigDecimal(1));
		}
		return departTimeByMonthMap;
	}

	public void setDepartTimeByMonthMap(NumberMap departTimeByMonthMap) {
		this.departTimeByMonthMap = departTimeByMonthMap;
	}

	public NumberMap getDepartTimeByWeekDayMap() {
		if (departTimeByWeekDayMap == null && order != null && order.getDepartDate() != null) {
			departTimeByWeekDayMap = new NumberMap(String.valueOf(order.getDepartDate().getDay()), new BigDecimal(1));
		}
		return departTimeByWeekDayMap;
	}

	public void setDepartTimeByWeekDayMap(NumberMap departTimeByWeekDayMap) {
		this.departTimeByWeekDayMap = departTimeByWeekDayMap;
	}

	public NumberMap getDepartTimeByHolidayMap() {
		if (departTimeByHolidayMap != null || order == null || order.getDepartHoliday() == null) {
			return departTimeByHolidayMap;
		}
		String[] holidays = order.getDepartHoliday().split("\\||_");
		departTimeByHolidayMap = new NumberMap();
		for (String holiday : holidays) {
			if (StringUtils.isEmpty(holiday)) {
				continue;
			}
			departTimeByHolidayMap.put(holiday, new BigDecimal(1));
		}
		if(departTimeByHolidayMap.size() > 1){
			departTimeByHolidayMap.remove("common");
		}
		if (departTimeByHolidayMap.isEmpty()) {
			departTimeByHolidayMap = null;
		}

		return departTimeByHolidayMap;
	}

	public String parseOrderCoptype(TravelOrderInfo order) {
		if (SparkUtils.isNull(order.getCoptype())) {
			return order.getBusinCategory();
		}
		if ("国内".equals(order.getCoptype())) {
			return "DOMESTIC";
		}
		if ("出境".equals(order.getCoptype())) {
			return "OUTBOUND";
		}
		return null;
	}

	public void setDepartTimeByHolidayMap(NumberMap departTimeByHolidayMap) {
		this.departTimeByHolidayMap = departTimeByHolidayMap;
	}

	public AverageUnit getBookingDomesticAmountAvg() {
		if (bookingDomesticAmountAvg != null || order == null || order.getPrice() == null) {
			return bookingDomesticAmountAvg;
		}
		String coptype = parseOrderCoptype(order);
		if (!"DOMESTIC".equals(coptype)) {
			return bookingDomesticAmountAvg;
		}
		bookingDomesticAmountAvg = new AverageUnit(new BigDecimal(order.getPrice()));
		return bookingDomesticAmountAvg;
	}

	public void setBookingDomesticAmountAvg(AverageUnit bookingDomesticAmountAvg) {
		this.bookingDomesticAmountAvg = bookingDomesticAmountAvg;
	}

	public GroupAverageMap getBookingDomesticAmountPerYearAvg() {
		if (bookingDomesticAmountPerYearAvg != null) {
			return bookingDomesticAmountPerYearAvg;
		}
		AverageUnit avg = this.getBookingDomesticAmountAvg();
		if (avg == null || order.getBookingDate() == null) {
			return bookingDomesticAmountPerYearAvg;
		}
		bookingDomesticAmountPerYearAvg = new GroupAverageMap(String.valueOf(order.getBookingDate().getYear() + 1900), avg);
		return bookingDomesticAmountPerYearAvg;
	}

	public void setBookingDomesticAmountPerYearAvg(GroupAverageMap bookingDomesticAmountPerYearAvg) {
		this.bookingDomesticAmountPerYearAvg = bookingDomesticAmountPerYearAvg;
	}

	public AverageUnit getBookingOutboundAmountAvg() {
		if (bookingOutboundAmountAvg != null || order == null || order.getPrice() == null) {
			return bookingOutboundAmountAvg;
		}
		String coptype = parseOrderCoptype(order);
		if (!"OUTBOUND".equals(coptype)) {
			return bookingOutboundAmountAvg;
		}
		bookingOutboundAmountAvg = new AverageUnit(new BigDecimal(order.getPrice()));
		return bookingOutboundAmountAvg;
	}

	public void setBookingOutboundAmountAvg(AverageUnit bookingOutboundAmountAvg) {
		this.bookingOutboundAmountAvg = bookingOutboundAmountAvg;
	}

	public GroupAverageMap getBookingOutboundAmountPerYearAvg() {
		if (bookingOutboundAmountPerYearAvg != null) {
			return bookingOutboundAmountPerYearAvg;
		}
		AverageUnit avg = this.getBookingOutboundAmountAvg();
		if (avg == null || order.getBookingDate() == null) {
			return bookingOutboundAmountPerYearAvg;
		}
		bookingOutboundAmountPerYearAvg = new GroupAverageMap(String.valueOf(order.getBookingDate().getYear() + 1900), avg);
		return bookingOutboundAmountPerYearAvg;
	}

	public void setBookingOutboundAmountPerYearAvg(GroupAverageMap bookingOutboundAmountPerYearAvg) {
		this.bookingOutboundAmountPerYearAvg = bookingOutboundAmountPerYearAvg;
	}
	
	public NumberMap getBookingBeforeDepartDaysMap() {
		if (bookingBeforeDepartDaysMap != null || order == null || order.getBookingDate() == null || order.getDepartDate() == null) {
			return bookingBeforeDepartDaysMap;
		}
		bookingBeforeDepartDaysMap = new NumberMap(BookingBeforeDepartDaysMap.getInterval(order.getBookingDate(), order.getDepartDate()), new BigDecimal(1));
		return bookingBeforeDepartDaysMap;
	}

	public void setBookingBeforeDepartDaysMap(NumberMap bookingBeforeDepartDaysMap) {
		this.bookingBeforeDepartDaysMap = bookingBeforeDepartDaysMap;
	}
	
	public NumberMap getDomesticJourneyCitiesMap() {
		if (domesticJourneyCitiesMap != null || order == null || StringUtils.isEmpty(order.getJourneyCities())) {
			return domesticJourneyCitiesMap;
		}
		String coptype = parseOrderCoptype(order);
		if (!"DOMESTIC".equals(coptype)) {
			return domesticJourneyCitiesMap;
		}
		domesticJourneyCitiesMap = new NumberMap();
		String[] cities = StringUtils.split(order.getJourneyCities(), "|");
		for (String city : cities) {
			if("上海".equals(city) && cities.length > 1){
				continue;
			}
			domesticJourneyCitiesMap.put(city, new BigDecimal(1));
		}
		return domesticJourneyCitiesMap;
	}

	public void setDomesticJourneyCitiesMap(NumberMap domesticJourneyCitiesMap) {
		this.domesticJourneyCitiesMap = domesticJourneyCitiesMap;
	}

	public NumberMap getOutboundJourneyCitiesMap() {

		if (outboundJourneyCitiesMap != null || order == null || StringUtils.isEmpty(order.getJourneyCities())) {
			return outboundJourneyCitiesMap;
		}
		String coptype = parseOrderCoptype(order);
		if (!"OUTBOUND".equals(coptype)) {
			return outboundJourneyCitiesMap;
		}
		outboundJourneyCitiesMap = new NumberMap();
		String[] cities = StringUtils.split(order.getJourneyCities(), "|");
		for (String city : cities) {
			if (ArrayUtils.contains(excludeOutboundJourneyCities, city)) {
				continue;
			}
			outboundJourneyCitiesMap.put(city, new BigDecimal(1));
		}
		return outboundJourneyCitiesMap;
	}

	public void setOutboundJourneyCitiesMap(NumberMap outboundJourneyCitiesMap) {
		this.outboundJourneyCitiesMap = outboundJourneyCitiesMap;
	}

	public NumberMap getJourneyNationsMap() {
		if (journeyNationsMap != null || order == null || StringUtils.isEmpty(order.getJourneyNations())) {
			return journeyNationsMap;
		}
		journeyNationsMap = new NumberMap();
		String[] nations = StringUtils.split(order.getJourneyNations(), "|");
		String coptype = parseOrderCoptype(order);
		for (String nation : nations) {
			if ("OUTBOUND".equals(coptype) && "中国".equals(nation) && nations.length > 1) {
				continue;
			}
			journeyNationsMap.put(nation, new BigDecimal(1));
		}
		return journeyNationsMap;
	}

	public void setJourneyNationsMap(NumberMap journeyNationsMap) {
		this.journeyNationsMap = journeyNationsMap;
	}

	public NumberMap getJourneyContinentsMap() {
		if (journeyContinentsMap != null || order == null || StringUtils.isEmpty(order.getJourneyContinents())) {
			return journeyContinentsMap;
		}
		journeyContinentsMap = new NumberMap();
		String[] continents = StringUtils.split(order.getJourneyContinents(), "|");
		String coptype = parseOrderCoptype(order);
		for (String continent : continents) {
			if ("OUTBOUND".equals(coptype) && "亚洲".equals(continent) && continents.length > 1) {
				continue;
			}
			journeyContinentsMap.put(continent, new BigDecimal(1));
		}
		return journeyContinentsMap;
	}

	public void setJourneyContinentsMap(NumberMap journeyContinentsMap) {
		this.journeyContinentsMap = journeyContinentsMap;
	}

	@Override
	public String toString() {
		return "TravelUserProfileInfo [member=" + member + ", order=" + order + ", rowkey=" + rowkey + ", everyYearConsumptionCount=" + everyYearConsumptionCount
				+ ", everyYearBookingCount=" + everyYearBookingCount + ", everyYearConsumptionAndBookingProportion=" + everyYearConsumptionAndBookingProportion
				+ ", consumptionAndBookingCount=" + consumptionAndBookingCount + ", totalConsumptionAndBookingProportion=" + totalConsumptionAndBookingProportion + ", status="
				+ status + ", totalConsumptionCount=" + totalConsumptionCount + ", totalBookingCount=" + totalBookingCount + ", lastTravelDate=" + lastTravelDate
				+ ", orderResource=" + orderResource + ", BookingResourceCount=" + BookingResourceCount + ", dates=" + dates + ", travelfrequency=" + travelfrequency
				+ ", totalConsumptionAmount=" + totalConsumptionAmount + ", totalConsumptionAmountPerYearMap=" + totalConsumptionAmountPerYearMap + ", totalBookingAmount="
				+ totalBookingAmount + ", totalBookingAmountPerYearMap=" + totalBookingAmountPerYearMap + ", orderConsumptionDate=" + orderConsumptionDate
				+ ", lastConsumptionAmount=" + lastConsumptionAmount + ", bookingJourneyDaysMap=" + bookingJourneyDaysMap + ", bookingTimeByMonthMap=" + bookingTimeByMonthMap
				+ ", bookingTimeByWeekDayMap=" + bookingTimeByWeekDayMap + ", bookingTimeByHourMap=" + bookingTimeByHourMap + ", departTimeByMonthMap=" + departTimeByMonthMap
				+ ", departTimeByWeekDayMap=" + departTimeByWeekDayMap + ", departTimeByHolidayMap=" + departTimeByHolidayMap + ", bookingDomesticAmountAvg="
				+ bookingDomesticAmountAvg + ", bookingDomesticAmountPerYearAvg=" + bookingDomesticAmountPerYearAvg + ", bookingOutboundAmountAvg=" + bookingOutboundAmountAvg
				+ ", bookingOutboundAmountPerYearAvg=" + bookingOutboundAmountPerYearAvg + ", airLinesMap=" + airLinesMap + ", bookingBeforeDepartDaysMap="
				+ bookingBeforeDepartDaysMap + ", domesticJourneyCitiesMap=" + domesticJourneyCitiesMap + ", outboundJourneyCitiesMap=" + outboundJourneyCitiesMap
				+ ", journeyNationsMap=" + journeyNationsMap + ", journeyContinentsMap=" + journeyContinentsMap + "]";
	}

}
