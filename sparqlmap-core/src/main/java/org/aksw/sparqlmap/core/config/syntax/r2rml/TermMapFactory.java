package org.aksw.sparqlmap.core.config.syntax.r2rml;

import org.aksw.sparqlmap.core.mapper.translate.DataTypeHelper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;

import com.hp.hpl.jena.datatypes.BaseDatatype;
import com.hp.hpl.jena.datatypes.RDFDatatype;
import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;
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
				
				
				DateValue dateValue = new DateValue(constLit.getLexicalForm()); 
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
	
	public TermMap createTermMap(Resource tm, Model r2rml){
		
		return null;
	}
	
	
	public TermMap createBoolTermMap(Expression bool){
		TermMap tm = new TermMap(dth);
		tm.setTermTyp(R2RML.Literal);
		tm.setLiteralDataType(XSDDatatype.XSDboolean.getURI());
		tm.literalValBool = dth.cast(bool, dth.getBooleanCastType());
		
		return tm;
	}
	
	
	
	private Expression resourceToExpression(String uri){
		return dth.cast(new StringValue("\"" + uri + "\""), dth.getStringCastType());	
}

}
