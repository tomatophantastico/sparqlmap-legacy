package org.aksw.sparqlmap.mapper.subquerymapper.algebra;

import java.sql.Types;

import org.aksw.sparqlmap.config.syntax.r2rml.ColumnHelper;

import com.hp.hpl.jena.datatypes.RDFDatatype;
import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;

public abstract  class DataTypeHelper {
	
	static org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(DataTypeHelper.class);
	
	
	public static RDFDatatype getRDFDataType(int sdt) {
		
		
		if(sdt == Types.DECIMAL || sdt == Types.NUMERIC){
			return XSDDatatype.XSDdecimal;
		}
		
		if(sdt== Types.BIGINT || sdt == Types.INTEGER || sdt == Types.SMALLINT){
			return XSDDatatype.XSDinteger;
		}
		if( sdt == Types.FLOAT || sdt == Types.DOUBLE || sdt ==Types.REAL ){
			return XSDDatatype.XSDdouble;
		}
		if(sdt == Types.VARCHAR || sdt == Types.CHAR || sdt == Types.CLOB){
			return null; //XSDDatatype.XSDstring;
		}
		if(sdt == Types.DATE ){
			return XSDDatatype.XSDdate;

		}
		if(sdt == Types.TIME){
			return XSDDatatype.XSDtime;

		}
		if( sdt == Types.TIMESTAMP){
			return XSDDatatype.XSDdateTime;

		}
		//the jsbc driver makes no differentiation between bit and boolean, so wetake them both here
		if(sdt == Types.BOOLEAN || sdt == Types.BIT){
			return XSDDatatype.XSDboolean;
		}
		
		if(sdt == Types.BINARY || sdt ==  Types.VARBINARY ||sdt ==  Types.BLOB){
			return XSDDatatype.XSDhexBinary;
		}
		
		
		if(sdt == ColumnHelper.COL_VAL_SQL_TYPE_CONSTLIT){
			return null;
		}
	
		
		
			log.info("encountered non-explicitly mapped sql type:" + sdt );
			return null; //XSDDatatype.XSDstring;
	
	}
	
	public String getCastTypeString(int sdt){
		return getCastTypeString(getRDFDataType(sdt));
	}
	
	public String getColumnString(int sdt){
		if(sdt == Types.DECIMAL || sdt == Types.NUMERIC || sdt== Types.BIGINT || sdt == Types.INTEGER || sdt == Types.SMALLINT ||  sdt == Types.FLOAT || sdt == Types.DOUBLE  || sdt == Types.REAL){
			return ColumnHelper.COL_NAME_LITERAL_NUMERIC;
		}
		if(sdt == Types.VARCHAR || sdt == Types.CHAR || sdt == Types.CLOB){
			return ColumnHelper.COL_NAME_LITERAL_STRING;
		}
		if(sdt == Types.DATE || sdt == Types.TIME || sdt == Types.TIMESTAMP){
			return ColumnHelper.COL_NAME_LITERAL_DATE;
		}
		if(sdt == Types.BOOLEAN){
			return ColumnHelper.COL_NAME_LITERAL_BOOL;
		}
		
	
		//fallback ;-)
		throw new ImplementationException("Encountered unknown sql type, spec says, i sould use string, but me throw error");
	}
	
	
	
	
	public String getCastTypeString(RDFDatatype datatype){
		if(XSDDatatype.XSDdecimal == datatype||XSDDatatype.XSDinteger ==datatype || XSDDatatype.XSDdouble == datatype){
			return getNumericCastType();
		}else if(XSDDatatype.XSDstring == datatype|| datatype ==null){
			return getStringCastType();
		}else if(XSDDatatype.XSDdateTime == datatype|| XSDDatatype.XSDdate == datatype ||  XSDDatatype.XSDtime == datatype){
			return getDateCastType();
		}else if(XSDDatatype.XSDboolean == datatype){
			return getBooleanCastType();
		}else if(XSDDatatype.XSDhexBinary == datatype){
			return getBinaryDataType();
		}else{
			throw new ImplementationException("Cannot map " + datatype.toString());
		}
	}
	
	
	
	
	
	public abstract String getBinaryDataType();

	public abstract String getStringCastType();
	
	public abstract String getNumericCastType();
	
	public abstract String getBooleanCastType();
	
	public abstract String getDateCastType();

	public abstract String getIntCastType();
	


}
