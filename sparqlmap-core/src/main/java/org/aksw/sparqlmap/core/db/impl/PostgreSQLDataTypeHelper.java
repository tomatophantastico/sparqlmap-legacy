package org.aksw.sparqlmap.core.db.impl;

import java.util.Arrays;

import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionWithString;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;

import org.aksw.sparqlmap.core.mapper.translate.DataTypeHelper;
import org.springframework.stereotype.Component;


public class PostgreSQLDataTypeHelper extends DataTypeHelper {
	
	
	static public String getDBName() {
		return PostgeSQLConnector.POSTGRES_DBNAME;
	}

	@Override
	public String getStringCastType() {
		return "TEXT";
	}

	@Override
	public String getNumericCastType() {
		return "NUMERIC";
	}

	@Override
	public String getBooleanCastType( ) {
		return "BOOLEAN";
	}

	@Override
	public String getDateCastType( ) {
		return "TIMESTAMP";
	}

	@Override
	public String getIntCastType() {
		
		return "INT";
	}

	@Override
	public String getBinaryDataType() {
	
		return "BYTEA";
	}

	@Override
	public boolean needsSpecialCastForBinary() {
		
		return true;
	}

	
	@Override
	public Expression binaryCastPrep(Expression expr) {
		
		CastExpression cast = new CastExpression(expr, getStringCastType());
		
		Function substring = new Function();
		substring.setName("SUBSTRING");
		ExpressionList subexprlist = new ExpressionList();
		subexprlist.setExpressions(Arrays.asList((Expression)new ExpressionWithString(cast," FROM 3" )));
		substring.setParameters(subexprlist);
		
		
		Function upper = new Function();
		upper.setName("UPPER");
		
		
		ExpressionList upexprlist = new ExpressionList(Arrays.asList((Expression)substring));
		upper.setParameters(upexprlist);
		
		return upper;
	}

	@Override
	public boolean needsSpecialCastForChar() {
		
		return true;
	}

	@Override
	public Expression charCastPrep(Expression expr,Integer fieldlength) {
		
		Function rpad = new Function();
		rpad.setName("RPAD");
		ExpressionList padexprlist = new ExpressionList();
		padexprlist.setExpressions(Arrays.asList((Expression) expr, (Expression) new LongValue(fieldlength.toString()) ));
		rpad.setParameters(padexprlist);
		
		
		
		
		return rpad;
	}

	@Override
	public byte[] binaryResultSetTreatment(byte[] bytes) {
		return Arrays.copyOfRange(bytes, 4, bytes.length);
	}

	@Override
	public boolean hasRowIdFunction() {
		// TODO Auto-generated method stub
		return false;
	}




}
