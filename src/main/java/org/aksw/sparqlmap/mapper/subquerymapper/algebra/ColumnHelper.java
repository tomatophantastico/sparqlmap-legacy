package org.aksw.sparqlmap.mapper.subquerymapper.algebra;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;

public class ColumnHelper {
	public static String R2R_COL_SUFFIX = "_R2R";
	public static String TYPE_COL = R2R_COL_SUFFIX + "_1_TYPE";
	public static String RES_LENGTH_COL = R2R_COL_SUFFIX + "_2_RES_LENGTH";
	public static String LIT_TYPE_COL = R2R_COL_SUFFIX + "_3_LIT_TYPE";
	public static String RESOURCE_COL_SEGMENT = R2R_COL_SUFFIX + "_4_SEG";
	public static String LITERAL_COL_NUM = R2R_COL_SUFFIX + "_6_LIT_NUM";
	//public static String LITERAL_COL_FLOAT = R2R_COL_SUFFIX + "_LIT_FLOAT";
	public static String LITERAL_COL_STRING = R2R_COL_SUFFIX + "_5_LIT_STRING";
	public static String LITERAL_COL_DATE = R2R_COL_SUFFIX + "_7_LIT_DATE";
	public static String LITERAL_COL_BOOL = R2R_COL_SUFFIX + "_8_LIT_BOOL";
	//public static String LITERAL_SQL_TYPE = R2R_COL_SUFFIX + "_LIT_SQLTYPE";
	public static String ORDER_BY_COL = R2R_COL_SUFFIX + "_9_OBY"; 
	public static String COL_INTERNAL = "BTF";
	

	public static int COL_TYPE_RESOURCE = 1;
	public static int COL_TYPE_LITERAL = 2;
	public static int COL_TYPE_BLANK = 3;
	

	public static int COL_SQL_TYPE_RESOURCE = -9999;
	
	
	
	
	
	
	
	public static String colnameBelongsToVar(String colalias){
		return colalias.substring(0, colalias.indexOf(R2R_COL_SUFFIX));
	}
	
	
	
	public static List<Expression> getBaseExpressions(Integer type, Integer resLength, Integer sqlType, DataTypeHelper dth){
		List<Expression> baseExpressions = new ArrayList<Expression>();
		LongValue typeVal = new LongValue(Integer.toString(type));
		baseExpressions.add(FilterUtil.cast(typeVal,dth.getNumericCastType()));
		LongValue lengthVal = new LongValue(Integer.toString(resLength));
		baseExpressions.add(FilterUtil.cast(lengthVal,dth.getNumericCastType()));
		LongValue sqlTypeVal = new LongValue(Integer.toString(sqlType));
		baseExpressions.add(FilterUtil.cast(sqlTypeVal,dth.getNumericCastType()));
		return baseExpressions;
		
		
		
	}

	

}
