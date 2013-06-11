package org.aksw.sparqlmap.core.db.impl;

import java.util.Arrays;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringExpression;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SubSelect;

import org.aksw.sparqlmap.core.mapper.translate.DataTypeHelper;

import com.hp.hpl.jena.reasoner.rulesys.builtins.LessThan;
import com.hp.hpl.jena.sparql.algebra.op.OpSlice;

public class OracleDataTypeHelper extends DataTypeHelper {

	@Override
	public String getBinaryDataType() {
		return "LONGVARBINARY";
	}

	@Override
	public String getStringCastType() {
		return "VARCHAR2(4000)";
	}

	@Override
	public String getNumericCastType() {
		return "DECIMAL";
	}

	@Override
	public String getBooleanCastType() {
		return"BOOLEAN";
	}

	@Override
	public String getDateCastType() {
		return "DATE";
	}

	@Override
	public String getIntCastType() {
		return "INTEGER";
	}

	@Override
	public boolean needsSpecialCastForBinary() {
		return false;
	}


	@Override
	public Expression binaryCastPrep(Expression expr) {
		return null;
	}

	@Override
	public boolean needsSpecialCastForChar() {
		return false;
	}

	@Override
	public Expression charCastPrep(Expression expr, Integer fieldlength) {
		return null;
	}

	@Override
	public boolean hasRowIdFunction() {
		
		return false;
	}
	@Override
	public PlainSelect slice(PlainSelect toModify, OpSlice slice) {
		
		long start = slice.getStart();
		long end = start+slice.getLength();
		
		PlainSelect sliceps = null;
		
		if(slice.getStart()>0){
			sliceps = new PlainSelect();
			SelectExpressionItem rownum = new SelectExpressionItem();
			rownum.setExpression(new StringExpression("ROWNUM"));
			rownum.setAlias("rown");
			sliceps.setSelectItems(Arrays.asList((SelectItem) new AllTableColumns(new Table(null,"\"slice_inner\"")),rownum));
			GreaterThanEquals gte = new GreaterThanEquals();
			gte.setLeftExpression(new StringExpression("ROWNUM"));
			gte.setRightExpression(new LongValue(""+slice.getStart()));
			sliceps.setWhere(gte);
			SubSelect subs = new SubSelect();
			subs.setSelectBody(toModify);
			subs.setAlias("slice_inner");
			sliceps.setFromItem(subs);
			
			if(slice.getLength()>=0){
				// wrap around sliceps
				PlainSelect sliceExt = new PlainSelect();
				sliceExt.setSelectItems(Arrays.asList((SelectItem) new AllColumns()));
				SubSelect sliceExtSubs =new  SubSelect();
				sliceExtSubs.setSelectBody(sliceExt);
				sliceExtSubs.setAlias("slice_ext");
				sliceExt.setFromItem(sliceExtSubs);			
				GreaterThan gte2 = new GreaterThan();
				gte2.setLeftExpression(new LongValue(""+slice.getStart()+slice.getLength()));
				gte2.setRightExpression(new StringExpression("rown"));
				sliceps.setWhere(gte);
				sliceps = sliceExt;
			}
			
		} else {
			
			sliceps = new PlainSelect();
			SelectExpressionItem rownum = new SelectExpressionItem();
			rownum.setExpression(new StringExpression("ROWNUM"));
			rownum.setAlias("rown");
			sliceps.setSelectItems(Arrays.asList((SelectItem) new AllTableColumns(new Table(null,"\"slice_inner\"")),rownum));
			GreaterThanEquals gte = new GreaterThanEquals();
			gte.setLeftExpression(new LongValue(""+slice.getLength()));
			gte.setRightExpression(new StringExpression("ROWNUM"));
			sliceps.setWhere(gte);
			SubSelect subs = new SubSelect();
			subs.setSelectBody(toModify);
			subs.setAlias("slice_inner");
			sliceps.setFromItem(subs);
		}
		
		
		
		
		
		
	
		
		
		
		
		
		return sliceps;
	}
	
	

//	@Override
//	public List<Expression> getRowIdFunction(String fromAlias) {
//		List<Expression> expressions=  new ArrayList<Expression>();
//		if(fromAlias!=null){
//			StringValue fromAliasValue = new StringValue("\"" + fromAlias + "\"");
//			expressions.add(cast(fromAliasValue, getStringCastType()));
//			
//		}else{
//			StringValue fromAliasValue = new StringValue("\"\"");
//			expressions.add(cast(fromAliasValue, getStringCastType()));
//		}
//		
//		Function rownum = new Function();
//		rownum.setName("ROWNUM");
//		expressions.add(cast(rownum, getStringCastType()));
//		return expressions;
//
//	}
	
	
	@Override
	public String getColnameQuery(String colname, String from) {
		
		return "SELECT "+colname+" FROM "+from +" WHERE ROWNUM <=1 ";
	}
	
	@Override
	public String getDataTypeQuery(String colname, String from) {
	
		return getColnameQuery("\""+colname+"\"", from);
	}
	
	@Override
	public String getValidateFromQuery(String from) {
		
		return getColnameQuery("*", from);
	}



}
