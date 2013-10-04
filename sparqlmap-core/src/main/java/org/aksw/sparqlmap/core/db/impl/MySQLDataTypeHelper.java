package org.aksw.sparqlmap.core.db.impl;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.StringExpression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Column;

import org.aksw.sparqlmap.core.config.syntax.r2rml.ColumnHelper;
import org.aksw.sparqlmap.core.mapper.translate.DataTypeHelper;

public class MySQLDataTypeHelper extends DataTypeHelper {

	@Override
	public String getStringCastType() {
		return "CHAR";
	}

	@Override
	public String getNumericCastType() {
		return "DECIMAL";
	}

	@Override
	public String getBooleanCastType() {
		return "CHAR";
	}

	@Override
	public String getDateCastType() {
		return "DATETIME";
	}

	@Override
	public String getIntCastType() {
		return "INT";
	}

	@Override
	public String getBinaryDataType() {
		return "VARBINARY";
	}

	@Override
	public boolean needsSpecialCastForBinary() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Expression binaryCastPrep(Expression expr) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean needsSpecialCastForChar() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Expression charCastPrep(Expression expr,Integer length) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public byte[] binaryResultSetTreatment(byte[] bytes) {
		return bytes;
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
//		StringExpression rowid = new StringExpression("_rowid");
//		expressions.add(cast(rowid, getStringCastType()));
//		return expressions;
//
//	}

}
