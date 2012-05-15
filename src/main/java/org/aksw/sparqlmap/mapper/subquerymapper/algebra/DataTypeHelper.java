package org.aksw.sparqlmap.mapper.subquerymapper.algebra;

import java.sql.Types;

import com.hp.hpl.jena.datatypes.RDFDatatype;
import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;

public abstract  class DataTypeHelper {
	
	
	public static RDFDatatype getRDFDataType(int sdt) {
		
		
		if(sdt== Types.BIGINT || sdt == Types.INTEGER || sdt == Types.FLOAT || sdt == Types.DOUBLE || sdt == Types.DECIMAL || sdt == Types.NUMERIC){
			return XSDDatatype.XSDdecimal;
		}
		if(sdt == Types.VARCHAR || sdt == Types.CHAR || sdt == Types.CLOB){
			return XSDDatatype.XSDstring;
		}
		if(sdt == Types.DATE || sdt == Types.TIME || sdt == Types.TIMESTAMP){
			return XSDDatatype.XSDdateTime;
		}
		if(sdt == Types.BOOLEAN){
			return XSDDatatype.XSDboolean;
		}
		
		//fallback ;-)
		throw new ImplementationException("Encountered unknown sql type, spec says, i sould use string, but me throw error");
	}
	
	public String getCastTypeString(int sdt){
		return getCastTypeString(getRDFDataType(sdt));
	}
	
	
	public String getCastTypeString(RDFDatatype datatype){
		if(XSDDatatype.XSDdecimal == datatype){
			return getNumericCastType();
		}else if(XSDDatatype.XSDstring == datatype){
			return getStringCastType();
		}else if(XSDDatatype.XSDdateTime == datatype){
			return getDateCastType();
		}else if(XSDDatatype.XSDboolean == datatype){
			return getBooleanCastType();
		}else{
			throw new ImplementationException("Cannot map " + datatype.toString());
		}
	}
	
	
	
	
	
	public abstract String getStringCastType();
	
	public abstract String getNumericCastType();
	
	public abstract String getBooleanCastType();
	
	public abstract String getDateCastType();

	public abstract String getIntCastType();
	


}
