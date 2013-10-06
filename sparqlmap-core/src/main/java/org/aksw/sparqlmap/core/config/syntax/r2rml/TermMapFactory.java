package org.aksw.sparqlmap.core.config.syntax.r2rml;

import java.util.ArrayList;
import java.util.List;

import org.aksw.sparqlmap.core.mapper.translate.DataTypeHelper;
import com.google.common.collect.Lists;

import net.sf.jsqlparser.expression.Expression;


public class TermMapFactory {
	
	
	public TermMapFactory(DataTypeHelper dth) {
		super();
		this.dth = dth;
	}

	DataTypeHelper dth;
	
	public List<Expression> createLiteralFromTemplate(){
		return null;
	}
	
	
	public List<Expression> createLiteralFromColumn(){
		return null;
	}
	

	public class Term{
		
		Expression termType = dth.castNull(dth.getNumericCastType());
		Expression literalType = dth.castNull(dth.getStringCastType());
		Expression literalLang= dth.castNull(dth.getStringCastType());
		Expression literalValString= dth.castNull(dth.getStringCastType());
		Expression literalValNumeric  = dth.castNull(dth.getNumericCastType());
		Expression literalValDate = dth.castNull(dth.getDateCastType());
		Expression literalValBool = dth.castNull(dth.getBooleanCastType());
		Expression literalValBinary = dth.castNull(dth.getBinaryDataType());
		
		List<Expression> resourceColSeg = new ArrayList<Expression>(); 

		public List<Expression> asList(){
			
			List<Expression> exprs = Lists.newArrayList(
					termType,
					literalType,
					literalLang,
					literalValString,
					literalValNumeric,
					literalValDate,
					literalValBool,
					literalValBinary);
			exprs.addAll(resourceColSeg);
			
			return exprs;
		}
	}
	
	
	

}
