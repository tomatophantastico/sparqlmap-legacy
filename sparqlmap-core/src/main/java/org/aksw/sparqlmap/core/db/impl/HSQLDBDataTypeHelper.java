package org.aksw.sparqlmap.core.db.impl;

import net.sf.jsqlparser.expression.Expression;

import org.aksw.sparqlmap.core.mapper.translate.DataTypeHelper;
import org.springframework.stereotype.Component;

public class HSQLDBDataTypeHelper extends DataTypeHelper {
	
	
	public static String getDBName() {
		return HSQLDBConnector.HSQLDB_NAME;
	}

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




}
