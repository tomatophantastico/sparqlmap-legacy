package org.aksw.sparqlmap.config.syntax;

import org.aksw.sparqlmap.mapper.subquerymapper.algebra.DataTypeHelper;

public class MysqlDataTypeHelper extends DataTypeHelper {

	@Override
	public String getStringCastType() {
		return "CHAR";
	}

	@Override
	public String getNumericCastType() {
		// TODO Auto-generated method stub
		return "NUMERIC";
	}

	@Override
	public String getBooleanCastType() {
		// TODO Auto-generated method stub
		return "BOOLEAN";
	}

	@Override
	public String getDateCastType() {
		// TODO Auto-generated method stub
		return "DATETIME";
	}

	@Override
	public String getIntCastType() {
		// TODO Auto-generated method stub
		return "INT";
	}

	@Override
	public String getBinaryDataType() {
		return "VARBINARY";
	}


}
