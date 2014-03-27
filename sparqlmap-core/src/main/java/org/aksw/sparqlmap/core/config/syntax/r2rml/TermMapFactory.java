package org.aksw.sparqlmap.core.config.syntax.r2rml;

import java.sql.Timestamp;

import org.aksw.sparqlmap.core.ImplementationException;
import org.aksw.sparqlmap.core.mapper.translate.DataTypeHelper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimestampValue;

import com.hp.hpl.jena.datatypes.BaseDatatype;
import com.hp.hpl.jena.datatypes.RDFDatatype;
import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;
import com.hp.hpl.jena.datatypes.xsd.XSDDateTime;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.impl.LiteralLabel;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.vocabulary.RDFS;


@Component
public class TermMapFactory {
	
	@Autowired
	DataTypeHelper dth;
	public void setDth(DataTypeHelper dth) {
		this.dth = dth;
	}

	
	/**
	 * create a TermMap for a static node.
	 * @param node
	 * @return
	 */
	public TermMap createTermMap(Node node){
		
		TermMap tm = new TermMap(dth);
	
		if(node.isLiteral()){
			tm.setTermTyp(R2RML.Literal);
			RDFDatatype dt = node.getLiteralDatatype();
			LiteralLabel constLit = node.getLiteral();
			if(dt==null){
				tm.setLiteralDataType(RDFS.Literal.getURI());
			}else{
				tm.setLiteralDataType(dt.getURI());
			}
			
			
			// set the value here
			if(dth.getCastTypeString(dt).equals(dth.getStringCastType())){
				StringValue stringVal = new StringValue("'"+constLit.getLexicalForm()+"'");
				tm.literalValString = dth.cast( stringVal, dth.getStringCastType());
				
			}else if(dth.getCastTypeString(dt).equals(dth.getNumericCastType())){
				LongValue longValue  = new LongValue(constLit.getLexicalForm());
				tm.literalValNumeric = dth.cast(longValue, dth.getNumericCastType());
				
			}else if(dth.getCastTypeString(dt).equals(dth.getBinaryDataType())){
				StringValue binVal = new StringValue("'"+constLit.getLexicalForm()+"'");
				tm.literalValBinary = dth.cast(binVal, dth.getBinaryDataType());
				
			}else if(dth.getCastTypeString(dt).equals(dth.getDateCastType())){
				Long timestamp;
				Object value = constLit.getValue();
				if(value  instanceof XSDDateTime){
					
					timestamp  = ((XSDDateTime) value).asCalendar().getTimeInMillis();
				}else{
					throw new ImplementationException("Encountered unkown datatype as data:" + value.getClass());
				}
				
				
				TimestampValue dateValue = new TimestampValue(new Timestamp(timestamp)); 
				tm.literalValDate = dth.cast(dateValue, dth.getDateCastType());
				
			}else if(dth.getCastTypeString(dt).equals(dth.getBooleanCastType())){
				StringValue bool = new StringValue("'"+constLit.getLexicalForm()+"'");
				tm.literalValBool = dth.cast(bool, dth.getBooleanCastType());
			}
			 
			
		}else{
			//not a Literal, so it has to be a resource
			tm.getResourceColSeg().add(resourceToExpression(node.getURI()));
			
			if(node.isBlank()){
				tm.setTermTyp(R2RML.BlankNode);
			}else{
				tm.setTermTyp(R2RML.IRI);
			}
		}
		
		return tm;
	}
	
	
	
	public TermMap createBoolTermMap(Expression bool){
		TermMap tm = new TermMap(dth);
		tm.setTermTyp(R2RML.Literal);
		tm.setLiteralDataType(XSDDatatype.XSDboolean.getURI());
		tm.literalValBool = dth.cast(bool, dth.getBooleanCastType());
		
		return tm;
	}
	
	public TermMap createStringTermMap(Expression string){
		TermMap tm = new TermMap(dth);
		tm.setTermTyp(R2RML.Literal);
		tm.setLiteralDataType(RDFS.Literal.getURI());
		tm.literalValString = dth.cast(string, dth.getStringCastType());
		
		return tm;
	}
	
	public TermMap createNumericalTermMap(Expression numeric,Expression datatype){
		TermMap tm = new TermMap(dth);
		tm.setTermTyp(R2RML.Literal);
		tm.literalType = datatype;
		tm.literalValNumeric = dth.cast(numeric, dth.getNumericCastType());
		
		return tm;
	}
	
	
	
	private Expression resourceToExpression(String uri){
		return dth.cast(new StringValue("\"" + uri + "\""), dth.getStringCastType());	
	}
	


}
