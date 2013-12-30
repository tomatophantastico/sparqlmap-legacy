package org.aksw.sparqlmap.core.mapper.translate;

import java.sql.Types;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.StringExpression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.PlainSelect;

import org.aksw.sparqlmap.core.config.syntax.r2rml.ColumnHelper;
import org.aksw.sparqlmap.core.db.DBAccess;
import org.springframework.beans.factory.annotation.Autowired;

import com.hp.hpl.jena.datatypes.RDFDatatype;
import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;
import com.hp.hpl.jena.sparql.algebra.op.OpSlice;

public abstract  class DataTypeHelper {
	
	static org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(DataTypeHelper.class);
	
	@Autowired
	private DBAccess dbaccess;
	
	public static Set<Class> constantValueExpressions;
	
	{
		constantValueExpressions =new HashSet<Class>();
		constantValueExpressions.add(StringValue.class);
		constantValueExpressions.add(StringExpression.class);
		constantValueExpressions.add(DateValue.class);
		constantValueExpressions.add(TimestampValue.class);
		constantValueExpressions.add(TimeValue.class);
		constantValueExpressions.add(LongValue.class);
		constantValueExpressions.add(DoubleValue.class);
		constantValueExpressions.add(NullValue.class);
		
		
	}
		
	Map<String,String> suffix2datatype = new HashMap<String, String>();
	
	public DataTypeHelper() {
		suffix2datatype.put(ColumnHelper.COL_NAME_LITERAL_BINARY,this.getBinaryDataType());
		suffix2datatype.put(ColumnHelper.COL_NAME_LITERAL_BOOL,this.getBooleanCastType());
		suffix2datatype.put(ColumnHelper.COL_NAME_LITERAL_DATE, this.getDateCastType());
		suffix2datatype.put(ColumnHelper.COL_NAME_LITERAL_LANG, this.getStringCastType());
		suffix2datatype.put(ColumnHelper.COL_NAME_LITERAL_NUMERIC,this.getNumericCastType());
		suffix2datatype.put(ColumnHelper.COL_NAME_LITERAL_STRING, this.getStringCastType());
		suffix2datatype.put(ColumnHelper.COL_NAME_LITERAL_TYPE, this.getStringCastType());
		suffix2datatype.put(ColumnHelper.COL_NAME_RDFTYPE, this.getNumericCastType());
		suffix2datatype.put(ColumnHelper.COL_NAME_RESOURCE_COL_SEGMENT, this.getStringCastType());
	}
	
	
	public static RDFDatatype getRDFDataType(int sdt) {
		
		
		if(sdt == Types.DECIMAL || sdt == Types.NUMERIC){
			return XSDDatatype.XSDdecimal;
		}
		
		if(sdt== Types.BIGINT || sdt == Types.INTEGER || sdt == Types.SMALLINT){
			return XSDDatatype.XSDinteger;
		}
		if( sdt == Types.FLOAT || sdt == Types.DOUBLE || sdt ==Types.REAL ){
			return XSDDatatype.XSDdouble;
		}
		if(sdt == Types.VARCHAR || sdt == Types.CHAR || sdt == Types.CLOB){
			return null; //XSDDatatype.XSDstring;
		}
		if(sdt == Types.DATE ){
			return XSDDatatype.XSDdate;

		}
		if(sdt == Types.TIME){
			return XSDDatatype.XSDtime;

		}
		if( sdt == Types.TIMESTAMP){
			return XSDDatatype.XSDdateTime;

		}
		//the jsbc driver makes no differentiation between bit and boolean, so wetake them both here
		if(sdt == Types.BOOLEAN || sdt == Types.BIT){
			return XSDDatatype.XSDboolean;
		}
		
		if(sdt == Types.BINARY || sdt ==  Types.VARBINARY ||sdt ==  Types.BLOB || sdt == Types.LONGVARBINARY){
			return XSDDatatype.XSDhexBinary;
		}
		
		
		if(sdt == ColumnHelper.COL_VAL_SQL_TYPE_CONSTLIT){
			return null;
		}
	
			log.info("encountered non-explicitly mapped sql type:" + sdt );
			return null; //XSDDatatype.XSDstring;
	
	}
	
	public String getCastTypeString(int sdt){
		return getCastTypeString(getRDFDataType(sdt));
	}
	
//	public String getColumnString(int sdt){
//		if(sdt == Types.DECIMAL || sdt == Types.NUMERIC || sdt== Types.BIGINT || sdt == Types.INTEGER || sdt == Types.SMALLINT ||  sdt == Types.FLOAT || sdt == Types.DOUBLE  || sdt == Types.REAL){
//			return ColumnHelper.COL_NAME_LITERAL_NUMERIC;
//		}
//		if(sdt == Types.VARCHAR || sdt == Types.CHAR || sdt == Types.CLOB){
//			return ColumnHelper.COL_NAME_LITERAL_STRING;
//		}
//		if(sdt == Types.DATE || sdt == Types.TIME || sdt == Types.TIMESTAMP){
//			return ColumnHelper.COL_NAME_LITERAL_DATE;
//		}
//		if(sdt == Types.BOOLEAN){
//			return ColumnHelper.COL_NAME_LITERAL_BOOL;
//		}
//		
//	
//		//fallback ;-)
//		throw new ImplementationException("Encountered unknown sql type, spec says, i sould use string, but me throw error");
//	}
	
	
	
	
	public String getCastTypeString(RDFDatatype datatype){
		if(XSDDatatype.XSDdecimal == datatype||XSDDatatype.XSDinteger ==datatype || XSDDatatype.XSDdouble == datatype){
			return getNumericCastType();
		}else if(XSDDatatype.XSDstring == datatype|| datatype ==null){
			return getStringCastType();
		}else if(XSDDatatype.XSDdateTime == datatype|| XSDDatatype.XSDdate == datatype ||  XSDDatatype.XSDtime == datatype){
			return getDateCastType();
		}else if(XSDDatatype.XSDboolean == datatype){
			return getBooleanCastType();
		}else if(XSDDatatype.XSDhexBinary == datatype){
			return getBinaryDataType();
		}else{
			return getStringCastType();
		}
	}
	
//	public Expression cast(String table, String col, String castTo) {
//		Function cast = new Function();
//		cast.setName("CAST");
//		ExpressionList exprlist = new ExpressionList();
//		exprlist.setExpressions(Arrays.asList(new CastStringExpression(table,
//				col, castTo)));
//		cast.setParameters(exprlist);
//		return cast;
//	}

	public Expression castNull(String castTo) {
		
		return new CastExpression(new NullValue(), castTo);
		
	}
	
	
	String getDataType(Expression expr){
		
		log.warn("Called getDataType. Refactor to not use direct col access");
		
		
		if(expr instanceof Column){
			String colname = ((Column) expr).getColumnName();
			String tablename = ((Column) expr).getTable().getName();
			
			//only shorten, if the table is not from a subselect
			if(!tablename.contains("subsel_")){
//				if(tablename.contains("_dupVar_")){
//					tablename=tablename.substring(0,tablename.lastIndexOf("_dupVar_"));
//				}
//				//remove the variable part
//				tablename = tablename.substring(0,tablename.lastIndexOf("_"));
				
				return this.getCastTypeString(dbaccess.getDataType(tablename, colname));
			}
			
		}
		if(expr instanceof StringValue){
			
			Scanner scanner = new Scanner(((StringValue) expr).getValue());

			if(scanner.hasNextBigDecimal()){
				return this.getNumericCastType();
			}
			
			
			return this.getStringCastType();
		}

		
		if(expr instanceof CastExpression){
			return ((CastExpression) expr).getTypeName();
		}
		
		return null;
		
	}
	
	public Expression cast(Expression expr, String castTo) {
		if(castTo == null){
			return expr;
		}
		
		if(expr instanceof Column){
			
			if (needsSpecialCastForBinary()) {
				// get the datatype
				Column col = (Column) expr;
				Integer datatypeint = dbaccess.getDataType(col.getTable()
						.getAlias(), col.getColumnName());
				if (datatypeint != null && getRDFDataType(datatypeint)!=null && getRDFDataType(datatypeint).equals(XSDDatatype.XSDhexBinary)) {
					// we need to wrap the cast additionally in a substring
						
					expr = binaryCastPrep(expr);

				}
			}else if (needsSpecialCastForChar()) {
				Column col = (Column) expr;
				Integer datatypeint = dbaccess.getDataType(col.getTable()
						.getAlias(), col.getColumnName());
				if (datatypeint != null  &&  datatypeint == Types.CHAR) {
					expr = charCastPrep(expr, dbaccess.getPrecision(col.getTable()
						.getAlias(), col.getColumnName()));

				}
			}
			
			
			
			//we have special casting needs for non-varchar and binary types.
		}
		

		return new CastExpression(expr,
				castTo);
	}
	
	/**
	 * if the expressions expr is a cast, the cast expression is returned,
	 * otherwise the expr parameter is returned
	 * 
	 * @param expr
	 */
	public static Expression uncast(Expression expr) {

		if(expr instanceof CastExpression){
			expr = ((CastExpression)expr).getCastedExpression();
		}
		
		return expr;
	}
	
	public static String getCastType(Expression expr) {

		String type = null;
		if (expr instanceof CastExpression) {
			type = ((CastExpression)expr).getTypeName();
		}
		return type;
	}
	
	
	public byte[] binaryResultSetTreatment(byte[] bytes){
		return bytes;
	}
	
	
	
	public abstract String getBinaryDataType();

	public abstract String getStringCastType();
	
	public abstract String getNumericCastType();
	
	public abstract String getBooleanCastType();
	
	public abstract String getDateCastType();

	public abstract String getIntCastType();
	
	public abstract boolean needsSpecialCastForBinary();
	

	
	public abstract Expression binaryCastPrep(Expression expr);
	
	public abstract boolean needsSpecialCastForChar();
	
	public abstract Expression charCastPrep(Expression expr, Integer fieldlength);
	
	public abstract boolean hasRowIdFunction();
	

	public List<Expression> getRowIdFunction(String fromAlias) {
		return null;
	}

	public PlainSelect slice(PlainSelect toModify, OpSlice slice) {
		Limit limit = new Limit();
		if (slice.getStart() >= 0) {
			limit.setOffset(slice.getStart());
		}
		if (slice.getLength() >= 0) {
			limit.setRowCount(slice.getLength());
		}

		toModify.setLimit(limit);
		return toModify;
	}

	public String getValidateFromQuery(String from) {
		return "SELECT * FROM " + from + " LIMIT 1";
	}

	public String getColnameQuery(String colname, String from) {
		return 			"SELECT " + colname + " FROM " +from+  " LIMIT 1";
	}
	public String getDataTypeQuery(String colname, String from) {
		return 		getColnameQuery("\""+colname+"\"", from);
	}
	
	public String getCastType(String colname){
		
		for(String suffix: this.suffix2datatype.keySet()){
			if(colname.endsWith(suffix)){
				return (suffix2datatype.get(suffix));
			}
		}
		return null;
		
		
	}
	
	public Expression asNumeric(Integer intVal){
		return cast(new LongValue(intVal.toString()), getNumericCastType());
	}
	
	
	


}
