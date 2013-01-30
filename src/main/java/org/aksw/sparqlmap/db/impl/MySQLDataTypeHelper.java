package org.aksw.sparqlmap.db.impl;

import net.sf.jsqlparser.expression.Expression;

import org.aksw.sparqlmap.mapper.translate.DataTypeHelper;

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
		return "BOOLEAN";
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


}
