package com.jje.bigdata.travel.taglibrary;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import com.jje.bigdata.travel.thesaurus.TourismHbaseOp;
import com.jje.bigdata.util.SceneConfig;
import com.jje.common.utils.StringUtils;

public class LineInformationNews {

	private String line_id = "";

	/*
	 * 遍历JJ000_WEBSITE_T_TBP_LINE_MAP表,取得其中的ID和需要的字段 根据线路的ID，获取线路对应的所有的团的信息 把信息进行整合，放入到一个Map中 ---- 一条线路对应一个key-value 去除文字中的数字,字母等信息，以防干扰
	 */

	public void getAllLineInfo(String tablename1, String tablename2, String tablename3, String tablename4, boolean increORfull) throws IOException {
		String lineinfo = "";
		int count = 0;
		// 所有线路信息的List
		List<String> linelist = new ArrayList<String>();
		// 遍历线路信息表,把线路ID添加到linelist中
		ResultScanner rs = TourismHbaseOp.getLineScan(tablename1);
		for (Result result : rs) {
			line_id = new String(result.getRow());
			linelist.add(line_id);
		}
		// 对increORfull进行判定,是全量还是增量的 true:全量 false:增量
		// if(increORfull){
		// //遍历table4 即线路信息整合后的表TB_REC_LINE_MID_MAP
		// rs = thOp.getLineScan(tablename4) ;
		// for(Result result : rs){
		// line_id = new String(result.getRow()) ;
		// linelist.remove(line_id) ;
		// }
		// }
		// 对linelist进行遍历,所有的line_id即为需要加入到HBase表中的线路信息
		// Result lineresult = null ;
		
		System.out.println(linelist.size());
		for (String string : linelist) {
			lineinfo = TourismHbaseOp.getJourneyInfo(string);
			if( !StringUtils.isBlank(lineinfo) ){
				TourismHbaseOp.putLineMapMidNew(string, lineinfo.trim());
				count ++;
			}
		}
		TourismHbaseOp.putAllLineMapNew();
		System.out.println("success num:" + count);
	}

	public static void main(String[] args) throws Exception {
		System.out.println(new Date());
		LineInformationNews li = new LineInformationNews();
		// 设定变量是增量跑,还是全量跑
		SceneConfig scon = SceneConfig.create();
		Properties p = scon.getPara("LineControl");
		Boolean b = new Boolean(p.getProperty("line_info_control"));
		if (b) {
			TourismHbaseOp.truncateTable("TB_REC_LINE_MID_MAP_NEW", new String[] { "info" });
			li.getAllLineInfo("JJ000_WEBSITE_T_TBP_LINE_MAP", "JJ000_WEBSITE_T_TBP_LINE_CHARACTERISTIC_MAP", "JJ000_WEBSITE_T_TBP_GROUP_MAP", "TB_REC_LINE_MID_MAP_NEW", b);
		} else {
			TourismHbaseOp.truncateTable("TB_REC_LINE_MID_MAP_NEW", new String[] { "info" });
			li.getAllLineInfo("JJ000_WEBSITE_T_TBP_LINE_MAP", "JJ000_WEBSITE_T_TBP_LINE_CHARACTERISTIC_MAP", "JJ000_WEBSITE_T_TBP_GROUP_MAP", "TB_REC_LINE_MID_MAP_NEW", b);
		}
		System.out.println(new Date());

	}

}
