package org.aksw.sparqlmap.db.impl;

import net.sf.jsqlparser.expression.Expression;

import org.aksw.sparqlmap.mapper.translate.DataTypeHelper;

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
		return "NUMERIC";
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
	public byte[] binaryResultSetTreatment(byte[] bytes) {
		return null;
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

}
