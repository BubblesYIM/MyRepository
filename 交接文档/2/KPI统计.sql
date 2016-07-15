#进入预定页面的链接
#	bookingnew
#	NewOrder
#	Payment?orderNo
#	order/save

/**酒店
hotel/order/bookingResult
  样例数据:http://www.jinjiang.com/hotel/order/bookingResult/406688?isScorePay=false&staticServerUrl=http%3A%2F%2Fstatic.jinjiang.com%2Fopt%2Fstatic%2F
ordersucceed?orderno
  样例数据:http://sitecore.jinjiang.com/OrderSucceed?orderNo=H1D529D22767     datajson:{"orderNo":"H1D529D8436D"}
Payment?orderNo
  样例数据:http://www.jinjiang.com/Payment?orderNo=H1D529D22756
 */
/**旅游
payment?orderno
  样例数据:
travel/order/save
  样例数据:http://pre.jinjiang.com/travel/order/save
payment/payOnline
  样例数据:
 */

#1.大数据推荐系统销售额
#执行以下sql产生结果
#/usr/lib/ngmr/run_app lexus-1.0-SNAPSHOT.jar com.jje.bigdata.census.CensusRecommendationRevenue ngmr-yarn-client lexus-1.0-SNAPSHOT.jar hotel 2016-01-01 2017-01-01 true
#/usr/lib/ngmr/run_app lexus-1.0-SNAPSHOT.jar com.jje.bigdata.census.CensusRecommendationRevenue ngmr-yarn-client lexus-1.0-SNAPSHOT.jar travel 2016-01-01 2017-01-01 true


#2.酒店浏览过跟踪代码的用户数
select count(distinct c.utrace) from T_DBP_PASS_USER_VIEW c
  where c.staytime > '2016-01-01'
        and c.staytime < '2016-06-01'
        and c.viewtype = 'HOTEL'
        and c.ip!='116.236.229.42'
        and c.ip!='124.74.27.86'
        and (c.viewUrl like '%#RE%' or viewurl rlike '\\S+#\\d{1,2}J\\S*' or c.viewUrl like '%DBP_LAV=%' or c.viewUrl like '%bd_time=%');


#3.酒店点击预订按钮用户数(浏览过追踪代码并进入过下单页面的)
select count(distinct a.utrace) from T_DBP_PASS_USER_VIEW a
where (upper(a.viewurl) like upper('%neworder%'))
      and a.staytime > '2016-01-01'
      and a.staytime < '2016-06-01'
      and a.viewtype = 'HOTEL'
      and a.ip!='116.236.229.42'
      and a.ip!='124.74.27.86'
      and a.utrace in (
  select distinct c.utrace from T_DBP_PASS_USER_VIEW c
  where c.staytime > '2016-01-01'
        and c.staytime < '2016-06-01'
        and c.ip!='116.236.229.42'
        and c.ip!='124.74.27.86'
        and (c.viewUrl like '%#RE%' or viewurl rlike '\\S+#\\d{1,2}J\\S*' or c.viewUrl like '%DBP_LAV=%' or c.viewUrl like '%bd_time=%')
);


/**
select count(distinct a.utrace) from T_DBP_PASS_USER_VIEW a
where (upper(a.viewurl) like upper('%neworder%'))
      and a.staytime > '2016-01-01'
      and a.staytime < '2016-04-20'
      and a.viewtype = 'HOTEL'
      and a.ip!='116.236.229.42'
      and a.ip!='124.74.27.86'
      and a.utrace in (
  select distinct c.utrace from T_DBP_PASS_USER_VIEW c
  where c.staytime > '2016-01-01'
        and c.staytime < '2016-04-20'
        and c.ip!='116.236.229.42'
        and c.viewtype = 'HOTEL'
        and c.ip!='124.74.27.86'
        and (c.viewUrl like '%#RE%' or viewurl rlike '\\S+#\\d{1,2}J\\S*' or c.viewUrl like '%DBP_LAV=%' or c.viewUrl like '%bd_time=%')
);
*/

#4.旅游浏览过跟踪代码的用户数
select count(distinct c.utrace) from T_DBP_PASS_USER_VIEW c
  where c.staytime > '2016-01-01'
        and c.staytime < '2016-06-01'
        and c.viewtype = 'TRAVEL'
        and c.ip!='116.236.229.42'
        and c.ip!='124.74.27.86'
        and (c.viewUrl like '%#RE%' or viewurl rlike '\\S+#\\d{1,2}J\\S*' or c.viewUrl like '%DBP_LAV=%' or c.viewUrl like '%bd_time=%');


#5.旅游点击预订按钮用户数(浏览过追踪代码并进入过下单页面的)
select count(distinct a.utrace) from T_DBP_PASS_USER_VIEW a
where (upper(a.viewurl) like upper('%order/save%') or upper(a.viewurl) like upper('%bookingnew%') or upper(a.viewurl) like upper('%neworder%'))
      and a.staytime > '2016-01-01'
      and a.staytime < '2016-06-01'
      and a.viewtype = 'TRAVEL'
      and a.ip!='116.236.229.42'
      and a.ip!='124.74.27.86'
      and a.utrace in (
  select distinct c.utrace from T_DBP_PASS_USER_VIEW c
  where c.staytime > '2016-01-01'
        and c.staytime < '2016-06-01'
        and c.ip!='116.236.229.42'
        and c.ip!='124.74.27.86'
        and (c.viewUrl like '%#RE%' or viewurl rlike '\\S+#\\d{1,2}J\\S*' or c.viewUrl like '%DBP_LAV=%' or c.viewUrl like '%bd_time=%')
);

#6.旅游网站看过产品的用户数
select count(distinct c.utrace) from T_DBP_PASS_USER_VIEW c
where c.staytime > '2016-01-01'
      and c.staytime < '2016-06-01'
      and c.viewtype = 'TRAVEL'
      and c.ip!='116.236.229.42'
      and c.ip!='124.74.27.86'
      and (c.viewUrl rlike '\\S+#\\d{1,2}J\\S*' or (c.viewUrl like '%bd_time=%' and (c.viewUrl like '%bd_type=hotle_web%'  or c.viewUrl like '%bd_type=travel_web_home%' or c.viewUrl like '%bd_type=travel_web_detail%' or c.viewUrl like '%bd_type=travel_web_search%')));

#6.1旅游网站进入过订单预定页面的用户数
select count(distinct a.utrace) from T_DBP_PASS_USER_VIEW a
where (upper(a.viewurl) like upper('%neworder%') or upper(a.viewurl) like upper('%travel/order%') or upper(a.viewurl) like upper('%bookingnew%'))
      and a.staytime > '2016-01-01'
      and a.staytime < '2016-06-01'
      and a.viewtype = 'TRAVEL'
      and a.ip!='116.236.229.42'
      and a.ip!='124.74.27.86'
      and a.utrace in (
      	select distinct c.utrace from T_DBP_PASS_USER_VIEW c
		where c.staytime > '2016-01-01'
		      and c.staytime < '2016-06-01'
		      and c.viewtype = 'TRAVEL'
		      and c.ip!='116.236.229.42'
		      and c.ip!='124.74.27.86'
		      and (c.viewUrl rlike '\\S+#\\d{1,2}J\\S*' or (c.viewUrl like '%bd_time=%' and (c.viewUrl like '%bd_type=hotle_web%'  or c.viewUrl like '%bd_type=travel_web_home%' or c.viewUrl like '%bd_type=travel_web_detail%' or c.viewUrl like '%bd_type=travel_web_search%')))
      	);

#6.2.旅游网站成功下单的用户数
select count(distinct a.utrace) from T_DBP_PASS_USER_VIEW a
where (upper(a.viewurl) like upper('%ordersucceed%') or upper(a.viewurl) like upper('%payment?orderno%') or upper(a.viewurl) like upper('%travel/order/save%'))
      and a.staytime > '2016-01-01'
      and a.staytime < '2016-06-01'
      and a.viewtype = 'TRAVEL'
      and a.ip!='116.236.229.42'
      and a.ip!='124.74.27.86'
      and a.utrace in (
      	select distinct c.utrace from T_DBP_PASS_USER_VIEW c
		where c.staytime > '2016-01-01'
		      and c.staytime < '2016-06-01'
		      and c.viewtype = 'TRAVEL'
		      and c.ip!='116.236.229.42'
		      and c.ip!='124.74.27.86'
		      and (c.viewUrl rlike '\\S+#\\d{1,2}J\\S*' or (c.viewUrl like '%bd_time=%' and (c.viewUrl like '%bd_type=hotle_web%'  or c.viewUrl like '%bd_type=travel_web_home%' or c.viewUrl like '%bd_type=travel_web_detail%' or c.viewUrl like '%bd_type=travel_web_search%')))
      	);

#7.旅游看过大数据自动EDM的人
select count(distinct c.utrace) from T_DBP_PASS_USER_VIEW c
where c.staytime > '2016-01-01'
      and c.staytime < '2016-06-01'
      and c.viewtype = 'TRAVEL'
      and c.ip!='116.236.229.42'
      and c.ip!='124.74.27.86'
      and (c.viewUrl like '%DBP_LAV=%' or (c.viewUrl like '%bd_time=%' and (c.viewUrl like '%bd_type=EDMAUTOHOTLE20150109%'  or c.viewUrl like '%bd_type=EDMAUTOTRAVEL20150422%')));

#7.1.旅游看过大数据自动EDM并且点击预定按钮的人
select count(distinct a.utrace) from T_DBP_PASS_USER_VIEW a
where (upper(a.viewurl) like upper('%neworder%') or upper(a.viewurl) like upper('%travel/order%') or upper(a.viewurl) like upper('%bookingnew%'))
      and a.staytime > '2016-01-01'
      and a.staytime < '2016-06-01'
      and a.viewtype = 'TRAVEL'
      and a.ip!='116.236.229.42'
      and a.ip!='124.74.27.86'
      and a.utrace in (
        select distinct c.utrace from T_DBP_PASS_USER_VIEW c
        where c.staytime > '2016-01-01'
              and c.staytime < '2016-06-01'
              and c.viewtype = 'TRAVEL'
              and c.ip!='116.236.229.42'
              and c.ip!='124.74.27.86'
              and (c.viewUrl like '%DBP_LAV=%' or (c.viewUrl like '%bd_time=%' and (c.viewUrl like '%bd_type=EDMAUTOHOTLE20150109%'  or c.viewUrl like '%bd_type=EDMAUTOTRAVEL20150422%')))
);

#7.2.旅游看过大数据自动EDM并且成功预定的用户数
select count(distinct a.utrace) from T_DBP_PASS_USER_VIEW a
where (upper(a.viewurl) like upper('%ordersucceed%') or upper(a.viewurl) like upper('%payment?orderno%') or upper(a.viewurl) like upper('%travel/order/save%'))
      and a.staytime > '2016-01-01'
      and a.staytime < '2016-06-01'
      and a.viewtype = 'TRAVEL'
      and a.ip!='116.236.229.42'
      and a.ip!='124.74.27.86'
      and a.utrace in (
        select distinct c.utrace from T_DBP_PASS_USER_VIEW c
        where c.staytime > '2016-01-01'
              and c.staytime < '2016-06-01'
              and c.viewtype = 'TRAVEL'
              and c.ip!='116.236.229.42'
              and c.ip!='124.74.27.86'
              and (c.viewUrl like '%DBP_LAV=%' or (c.viewUrl like '%bd_time=%' and (c.viewUrl like '%bd_type=EDMAUTOHOTLE20150109%'  or c.viewUrl like '%bd_type=EDMAUTOTRAVEL20150422%')))
);

#8.旅游看过会员EDM的人
select count(distinct c.utrace) from T_DBP_PASS_USER_VIEW c
where c.staytime > '2016-01-01'
      and c.staytime < '2016-06-01'
      and c.viewtype = 'TRAVEL'
      and c.ip!='116.236.229.42'
      and c.ip!='124.74.27.86'
      and (c.viewUrl like '%#RE%' or (c.viewUrl like '%bd_time=%' and c.viewUrl not like '%bd_type=hotle_web%'  and c.viewUrl not like '%bd_type=travel_web_home%' and c.viewUrl not like '%bd_type=travel_web_detail%' and c.viewUrl not like '%bd_type=travel_web_search%' and c.viewUrl not like '%bd_type=EDMAUTOHOTLE20150109%' and c.viewUrl not like '%bd_type=EDMAUTOTRAVEL20150422%'));

#8.1.旅游看过会员EDM并且点击预定按钮的人
select count(distinct a.utrace) from T_DBP_PASS_USER_VIEW a
where (upper(a.viewurl) like upper('%neworder%') or upper(a.viewurl) like upper('%travel/order%') or upper(a.viewurl) like upper('%bookingnew%'))
      and a.staytime > '2016-01-01'
      and a.staytime < '2016-06-01'
      and a.viewtype = 'TRAVEL'
      and a.ip!='116.236.229.42'
      and a.ip!='124.74.27.86'
      and a.utrace in (
        select distinct c.utrace from T_DBP_PASS_USER_VIEW c
		where c.staytime > '2016-01-01'
		      and c.staytime < '2016-06-01'
		      and c.viewtype = 'TRAVEL'
		      and c.ip!='116.236.229.42'
		      and c.ip!='124.74.27.86'
		      and (c.viewUrl like '%#RE%' or (c.viewUrl like '%bd_time=%' and c.viewUrl not like '%bd_type=hotle_web%'  and c.viewUrl not like '%bd_type=travel_web_home%' and c.viewUrl not like '%bd_type=travel_web_detail%' and c.viewUrl not like '%bd_type=travel_web_search%' and c.viewUrl not like '%bd_type=EDMAUTOHOTLE20150109%' and c.viewUrl not like '%bd_type=EDMAUTOTRAVEL20150422%'))
);

#8.2.旅游看过会员EDM并且成功预定的用户数
select count(distinct a.utrace) from T_DBP_PASS_USER_VIEW a
where (upper(a.viewurl) like upper('%ordersucceed%') or upper(a.viewurl) like upper('%payment?orderno%') or upper(a.viewurl) like upper('%travel/order/save%'))
      and a.staytime > '2016-01-01'
      and a.staytime < '2016-06-01'
      and a.viewtype = 'TRAVEL'
      and a.ip!='116.236.229.42'
      and a.ip!='124.74.27.86'
      and a.utrace in (
        select distinct c.utrace from T_DBP_PASS_USER_VIEW c
		where c.staytime > '2016-01-01'
		      and c.staytime < '2016-06-01'
		      and c.viewtype = 'TRAVEL'
		      and c.ip!='116.236.229.42'
		      and c.ip!='124.74.27.86'
		      and (c.viewUrl like '%#RE%' or (c.viewUrl like '%bd_time=%' and c.viewUrl not like '%bd_type=hotle_web%'  and c.viewUrl not like '%bd_type=travel_web_home%' and c.viewUrl not like '%bd_type=travel_web_detail%' and c.viewUrl not like '%bd_type=travel_web_search%' and c.viewUrl not like '%bd_type=EDMAUTOHOTLE20150109%' and c.viewUrl not like '%bd_type=EDMAUTOTRAVEL20150422%'))
);


#9.酒店网站看过产品的用户数
select count(distinct c.utrace) from T_DBP_PASS_USER_VIEW c
where c.staytime > '2016-01-01'
      and c.staytime < '2016-06-01'
      and c.viewtype = 'HOTEL'
      and c.ip!='116.236.229.42'
      and c.ip!='124.74.27.86'
      and (c.viewUrl rlike '\\S+#\\d{1,2}J\\S*' or (c.viewUrl like '%bd_time=%' and (c.viewUrl like '%bd_type=hotle_web%'  or c.viewUrl like '%bd_type=travel_web_home%' or c.viewUrl like '%bd_type=travel_web_detail%' or c.viewUrl like '%bd_type=travel_web_search%')));


#9.1.酒店看过网站产品并且点击预定按钮的人
select count(distinct a.utrace) from T_DBP_PASS_USER_VIEW a
where (upper(a.viewurl) like upper('%neworder%') or upper(a.viewurl) like upper('%travel/order%') or upper(a.viewurl) like upper('%bookingnew%'))
      and a.staytime > '2016-01-01'
      and a.staytime < '2016-06-01'
      and a.viewtype = 'HOTEL'
      and a.ip!='116.236.229.42'
      and a.ip!='124.74.27.86'
      and a.utrace in (
        select distinct c.utrace from T_DBP_PASS_USER_VIEW c
		where c.staytime > '2016-01-01'
		      and c.staytime < '2016-06-01'
		      and c.viewtype = 'HOTEL'
		      and c.ip!='116.236.229.42'
		      and c.ip!='124.74.27.86'
		      and (c.viewUrl rlike '\\S+#\\d{1,2}J\\S*' or (c.viewUrl like '%bd_time=%' and (c.viewUrl like '%bd_type=hotle_web%'  or c.viewUrl like '%bd_type=travel_web_home%' or c.viewUrl like '%bd_type=travel_web_detail%' or c.viewUrl like '%bd_type=travel_web_search%')))
);

#9.2.酒店看过网站产品并且成功预定的人
select count(distinct a.utrace) from T_DBP_PASS_USER_VIEW a
where (upper(a.viewurl) like upper('%ordersucceed%') or upper(a.viewurl) like upper('%payment?orderno%') or upper(a.viewurl) like upper('%travel/order/save%'))
      and a.staytime > '2016-01-01'
      and a.staytime < '2016-06-01'
      and a.viewtype = 'HOTEL'
      and a.ip!='116.236.229.42'
      and a.ip!='124.74.27.86'
      and a.utrace in (
        select distinct c.utrace from T_DBP_PASS_USER_VIEW c
		where c.staytime > '2016-01-01'
		      and c.staytime < '2016-06-01'
		      and c.viewtype = 'HOTEL'
		      and c.ip!='116.236.229.42'
		      and c.ip!='124.74.27.86'
		      and (c.viewUrl rlike '\\S+#\\d{1,2}J\\S*' or (c.viewUrl like '%bd_time=%' and (c.viewUrl like '%bd_type=hotle_web%'  or c.viewUrl like '%bd_type=travel_web_home%' or c.viewUrl like '%bd_type=travel_web_detail%' or c.viewUrl like '%bd_type=travel_web_search%')))
);

#10.酒店看过大数据自动EDM的人
select count(distinct c.utrace) from T_DBP_PASS_USER_VIEW c
where c.staytime > '2016-01-01'
      and c.staytime < '2016-06-01'
      and c.viewtype = 'HOTEL'
      and c.ip!='116.236.229.42'
      and c.ip!='124.74.27.86'
      and (c.viewUrl like '%DBP_LAV=%' or (c.viewUrl like '%bd_time=%' and (c.viewUrl like '%bd_type=EDMAUTOHOTLE20150109%'  or c.viewUrl like '%bd_type=EDMAUTOTRAVEL20150422%')));

#10.1.酒店看过大数据自动EDM并且点击预定按钮的人
select count(distinct a.utrace) from T_DBP_PASS_USER_VIEW a
where (upper(a.viewurl) like upper('%neworder%') or upper(a.viewurl) like upper('%travel/order%') or upper(a.viewurl) like upper('%bookingnew%'))
      and a.staytime > '2016-01-01'
      and a.staytime < '2016-06-01'
      and a.viewtype = 'HOTEL'
      and a.ip!='116.236.229.42'
      and a.ip!='124.74.27.86'
      and a.utrace in (
        select distinct c.utrace from T_DBP_PASS_USER_VIEW c
		where c.staytime > '2016-01-01'
		      and c.staytime < '2016-06-01'
		      and c.viewtype = 'HOTEL'
		      and c.ip!='116.236.229.42'
		      and c.ip!='124.74.27.86'
		      and (c.viewUrl like '%DBP_LAV=%' or (c.viewUrl like '%bd_time=%' and (c.viewUrl like '%bd_type=EDMAUTOHOTLE20150109%'  or c.viewUrl like '%bd_type=EDMAUTOTRAVEL20150422%')))
);

#10.2.酒店看过大数据自动EDM并且成功预定的人
select count(distinct a.utrace) from T_DBP_PASS_USER_VIEW a
where (upper(a.viewurl) like upper('%ordersucceed%') or upper(a.viewurl) like upper('%payment?orderno%') or upper(a.viewurl) like upper('%travel/order/save%'))
      and a.staytime > '2016-01-01'
      and a.staytime < '2016-06-01'
      and a.viewtype = 'HOTEL'
      and a.ip!='116.236.229.42'
      and a.ip!='124.74.27.86'
      and a.utrace in (
        select distinct c.utrace from T_DBP_PASS_USER_VIEW c
		where c.staytime > '2016-01-01'
		      and c.staytime < '2016-06-01'
		      and c.viewtype = 'HOTEL'
		      and c.ip!='116.236.229.42'
		      and c.ip!='124.74.27.86'
		      and (c.viewUrl like '%DBP_LAV=%' or (c.viewUrl like '%bd_time=%' and (c.viewUrl like '%bd_type=EDMAUTOHOTLE20150109%'  or c.viewUrl like '%bd_type=EDMAUTOTRAVEL20150422%')))
);

#11.酒店看过会员EDM的人
select count(distinct c.utrace) from T_DBP_PASS_USER_VIEW c
where c.staytime > '2016-01-01'
      and c.staytime < '2016-06-01'
      and c.viewtype = 'HOTEL'
      and c.ip!='116.236.229.42'
      and c.ip!='124.74.27.86'
      and (c.viewUrl like '%#RE%' or (c.viewUrl like '%bd_time=%' and c.viewUrl not like '%bd_type=hotle_web%'  and c.viewUrl not like '%bd_type=travel_web_home%' and c.viewUrl not like '%bd_type=travel_web_detail%' and c.viewUrl not like '%bd_type=travel_web_search%' and c.viewUrl not like '%bd_type=EDMAUTOHOTLE20150109%' and c.viewUrl not like '%bd_type=EDMAUTOTRAVEL20150422%'));

#11.1.酒店看过会员EDM并且点击预定按钮的人
select count(distinct a.utrace) from T_DBP_PASS_USER_VIEW a
where (upper(a.viewurl) like upper('%neworder%') or upper(a.viewurl) like upper('%travel/order%') or upper(a.viewurl) like upper('%bookingnew%'))
      and a.staytime > '2016-01-01'
      and a.staytime < '2016-06-01'
      and a.viewtype = 'HOTEL'
      and a.ip!='116.236.229.42'
      and a.ip!='124.74.27.86'
      and a.utrace in (
        select distinct c.utrace from T_DBP_PASS_USER_VIEW c
		where c.staytime > '2016-01-01'
		      and c.staytime < '2016-06-01'
		      and c.viewtype = 'HOTEL'
		      and c.ip!='116.236.229.42'
		      and c.ip!='124.74.27.86'
		      and (c.viewUrl like '%#RE%' or (c.viewUrl like '%bd_time=%' and c.viewUrl not like '%bd_type=hotle_web%'  and c.viewUrl not like '%bd_type=travel_web_home%' and c.viewUrl not like '%bd_type=travel_web_detail%' and c.viewUrl not like '%bd_type=travel_web_search%' and c.viewUrl not like '%bd_type=EDMAUTOHOTLE20150109%' and c.viewUrl not like '%bd_type=EDMAUTOTRAVEL20150422%'))
);

#11.2.酒店看过会员EDM并且成功预定的人
select count(distinct a.utrace) from T_DBP_PASS_USER_VIEW a
where (upper(a.viewurl) like upper('%ordersucceed%') or upper(a.viewurl) like upper('%payment?orderno%') or upper(a.viewurl) like upper('%travel/order/save%'))
      and a.staytime > '2016-01-01'
      and a.staytime < '2016-06-01'
      and a.viewtype = 'HOTEL'
      and a.ip!='116.236.229.42'
      and a.ip!='124.74.27.86'
      and a.utrace in (
        select distinct c.utrace from T_DBP_PASS_USER_VIEW c
		where c.staytime > '2016-01-01'
		      and c.staytime < '2016-06-01'
		      and c.viewtype = 'HOTEL'
		      and c.ip!='116.236.229.42'
		      and c.ip!='124.74.27.86'
		      and (c.viewUrl like '%#RE%' or (c.viewUrl like '%bd_time=%' and c.viewUrl not like '%bd_type=hotle_web%'  and c.viewUrl not like '%bd_type=travel_web_home%' and c.viewUrl not like '%bd_type=travel_web_detail%' and c.viewUrl not like '%bd_type=travel_web_search%' and c.viewUrl not like '%bd_type=EDMAUTOHOTLE20150109%' and c.viewUrl not like '%bd_type=EDMAUTOTRAVEL20150422%'))
);

#12.看过热门排行的用户数
select count(distinct c.utrace) from T_DBP_PASS_USER_VIEW c
where c.staytime > '2016-01-01'
      and c.staytime < '2016-06-01'
      and c.ip!='116.236.229.42'
      and c.ip!='124.74.27.86'
      and c.viewUrl like '%bd_type=travel_web_home%'
      and c.viewUrl like '%bd_source=1_2%';\

#12.1.看过热门排行并点过预定的用户数
select count(distinct a.utrace) from T_DBP_PASS_USER_VIEW a
where (upper(a.viewurl) like upper('%neworder%') or upper(a.viewurl) like upper('%travel/order%') or upper(a.viewurl) like upper('%bookingnew%'))
      and a.staytime > '2016-01-01'
      and a.staytime < '2016-06-01'
      and a.ip!='116.236.229.42'
      and a.ip!='124.74.27.86'
      and a.utrace in (
            select distinct c.utrace from T_DBP_PASS_USER_VIEW c
            where c.staytime > '2016-01-01'
                  and c.staytime < '2016-06-01'
                  and c.ip!='116.236.229.42'
                  and c.ip!='124.74.27.86'
                  and c.viewUrl like '%bd_type=travel_web_home%'
                  and c.viewUrl like '%bd_source=1_2%'
      );

#12.2.看过热门排行并且成功下单过的用户数
select count(distinct a.utrace) from T_DBP_PASS_USER_VIEW a
where (upper(a.viewurl) like upper('%ordersucceed%') or upper(a.viewurl) like upper('%payment?orderno%') or upper(a.viewurl) like upper('%order/save%'))
      and a.staytime > '2016-01-01'
      and a.staytime < '2016-06-01'
      and a.ip!='116.236.229.42'
      and a.ip!='124.74.27.86'
      and a.utrace in (
            select distinct c.utrace from T_DBP_PASS_USER_VIEW c
            where c.staytime > '2016-01-01'
                  and c.staytime < '2016-06-01'
                  and c.ip!='116.236.229.42'
                  and c.ip!='124.74.27.86'
                  and c.viewUrl like '%bd_type=travel_web_home%'
                  and c.viewUrl like '%bd_source=1_2%'
      );

#13.看过旅游猜您喜欢的用户数
select count(distinct c.utrace) from T_DBP_PASS_USER_VIEW c
where c.staytime > '2016-01-01'
      and c.staytime < '2016-06-01'
      and c.ip!='116.236.229.42'
      and c.ip!='124.74.27.86'
      and c.viewUrl like '%bd_type=travel_web_detail%'
      and c.viewUrl like '%bd_source=1_4%';

#13.1.看过旅游猜你喜欢并点击过预定按钮的用户数
select count(distinct a.utrace) from T_DBP_PASS_USER_VIEW a
where (upper(a.viewurl) like upper('%neworder%') or upper(a.viewurl) like upper('%travel/order%') or upper(a.viewurl) like upper('%bookingnew%'))
      and a.staytime > '2016-01-01'
      and a.staytime < '2016-06-01'
      and a.ip!='116.236.229.42'
      and a.ip!='124.74.27.86'
      and a.utrace in (
            select distinct c.utrace from T_DBP_PASS_USER_VIEW c
            where c.staytime > '2016-01-01'
                  and c.staytime < '2016-06-01'
                  and c.ip!='116.236.229.42'
                  and c.ip!='124.74.27.86'
                  and c.viewUrl like '%bd_type=travel_web_detail%'
                  and c.viewUrl like '%bd_source=1_4%'
      );

#13.2.看过旅游猜你喜欢并成功下单的用户数
select count(distinct a.utrace) from T_DBP_PASS_USER_VIEW a
where (upper(a.viewurl) like upper('%ordersucceed%') or upper(a.viewurl) like upper('%payment?orderno%') or upper(a.viewurl) like upper('%order/save%'))
      and a.staytime > '2016-01-01'
      and a.staytime < '2016-06-01'
      and a.ip!='116.236.229.42'
      and a.ip!='124.74.27.86'
      and a.utrace in (
      select distinct c.utrace from T_DBP_PASS_USER_VIEW c
      where c.staytime > '2016-01-01'
            and c.staytime < '2016-06-01'
            and c.ip!='116.236.229.42'
            and c.ip!='124.74.27.86'
            and c.viewUrl like '%bd_type=travel_web_detail%'
            and c.viewUrl like '%bd_source=1_4%'
);

#14.看过看了又看的用户数
select count(distinct c.utrace) from T_DBP_PASS_USER_VIEW c
where c.staytime > '2016-01-01'
      and c.staytime < '2016-06-01'
      and c.ip!='116.236.229.42'
      and c.ip!='124.74.27.86'
      and c.viewUrl like '%bd_type=travel_web_detail%'
      and c.viewUrl like '%bd_source=1_3%';

#14.1.看过看了又看并且点击过预定按钮的用户数
select count(distinct a.utrace) from T_DBP_PASS_USER_VIEW a
where (upper(a.viewurl) like upper('%neworder%') or upper(a.viewurl) like upper('%travel/order%') or upper(a.viewurl) like upper('%bookingnew%'))
      and a.staytime > '2016-01-01'
      and a.staytime < '2016-06-01'
      and a.ip!='116.236.229.42'
      and a.ip!='124.74.27.86'
      and a.utrace in (
            select distinct c.utrace from T_DBP_PASS_USER_VIEW c
            where c.staytime > '2016-01-01'
                  and c.staytime < '2016-06-01'
                  and c.ip!='116.236.229.42'
                  and c.ip!='124.74.27.86'
                  and c.viewUrl like '%bd_type=travel_web_detail%'
                  and c.viewUrl like '%bd_source=1_3%'
      );

#14.2.看过看了有看并且下单的用户
select count(distinct a.utrace) from T_DBP_PASS_USER_VIEW a
where (upper(a.viewurl) like upper('%ordersucceed%') or upper(a.viewurl) like upper('%payment?orderno%') or upper(a.viewurl) like upper('%order/save%'))
      and a.staytime > '2016-01-01'
      and a.staytime < '2016-06-01'
      and a.ip!='116.236.229.42'
      and a.ip!='124.74.27.86'
      and a.utrace in (
      select distinct c.utrace from T_DBP_PASS_USER_VIEW c
      where c.staytime > '2016-01-01'
            and c.staytime < '2016-06-01'
            and c.ip!='116.236.229.42'
            and c.ip!='124.74.27.86'
            and c.viewUrl like '%bd_type=travel_web_detail%'
            and c.viewUrl like '%bd_source=1_3%'
);


#15.看过酒店猜您喜欢的用户数
select count(distinct c.utrace) from T_DBP_PASS_USER_VIEW c
where c.staytime > '2016-01-01'
      and c.staytime < '2016-06-01'
      and c.ip!='116.236.229.42'
      and c.ip!='124.74.27.86'
      and c.viewUrl like '%bd_type=hotle_web%'
      and c.viewUrl like '%bd_source=2_2%';

#15.1.看过酒店猜你喜欢并且点击预定按钮的用户数
select count(distinct a.utrace) from T_DBP_PASS_USER_VIEW a
where (upper(a.viewurl) like upper('%neworder%') or upper(a.viewurl) like upper('%travel/order%') or upper(a.viewurl) like upper('%bookingnew%'))
      and a.staytime > '2016-01-01'
      and a.staytime < '2016-06-01'
      and a.ip!='116.236.229.42'
      and a.ip!='124.74.27.86'
      and a.utrace in (
            select distinct c.utrace from T_DBP_PASS_USER_VIEW c
            where c.staytime > '2016-01-01'
                  and c.staytime < '2016-06-01'
                  and c.ip!='116.236.229.42'
                  and c.ip!='124.74.27.86'
                  and c.viewUrl like '%bd_type=hotle_web%'
                  and c.viewUrl like '%bd_source=2_2%'
      );

#15.2.看过酒店猜你喜欢并且下单的用户数
select count(distinct a.utrace) from T_DBP_PASS_USER_VIEW a
where (upper(a.viewurl) like upper('%ordersucceed%') or upper(a.viewurl) like upper('%payment?orderno%') or upper(a.viewurl) like upper('%travel/order/save%'))
      and a.staytime > '2016-01-01'
      and a.staytime < '2016-06-01'
      and a.ip!='116.236.229.42'
      and a.ip!='124.74.27.86'
      and a.utrace in (
      select distinct c.utrace from T_DBP_PASS_USER_VIEW c
      where c.staytime > '2016-01-01'
            and c.staytime < '2016-06-01'
            and c.ip!='116.236.229.42'
            and c.ip!='124.74.27.86'
            and c.viewUrl like '%bd_type=hotle_web%'
            and c.viewUrl like '%bd_source=2_2%'
);