package org.aksw.sparqlmap.config.syntax;

import org.aksw.sparqlmap.mapper.subquerymapper.algebra.DataTypeHelper;

public class PostgresDataTypeHandler extends DataTypeHelper {

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



}
