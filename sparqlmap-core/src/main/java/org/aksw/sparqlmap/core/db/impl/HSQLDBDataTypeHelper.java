package org.aksw.sparqlmap.core.db.impl;

import net.sf.jsqlparser.expression.Expression;

import org.aksw.sparqlmap.core.mapper.translate.DataTypeHelper;

public class HSQLDBDataTypeHelper extends DataTypeHelper {

	@Override
	public String getBinaryDataType() {
		return "LONGVARBINARY";
	}

	@Override
	public String getStringCastType() {
		return "LONGVARCHAR";
	}

	@Override
	public String getNumericCastType() {
		return "DOUBLE";
	}

	@Override
	public String getBooleanCastType() {
		return"BOOLEAN";
	}

	@Override
	public String getDateCastType() {
		return "DATETIME";
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



}
