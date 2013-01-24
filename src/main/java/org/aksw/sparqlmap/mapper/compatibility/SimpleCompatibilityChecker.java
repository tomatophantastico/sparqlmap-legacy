package org.aksw.sparqlmap.mapper.compatibility;

import java.util.Collection;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Column;

import org.aksw.sparqlmap.config.syntax.r2rml.ColumnHelper;
import org.aksw.sparqlmap.config.syntax.r2rml.TermMap;
import org.aksw.sparqlmap.mapper.translate.DataTypeHelper;
import org.aksw.sparqlmap.mapper.translate.FilterUtil;
import org.aksw.sparqlmap.mapper.translate.ImplementationException;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.impl.LiteralLabel;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.expr.E_Equals;
import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.expr.NodeValue;

public class SimpleCompatibilityChecker implements CompatibilityChecker{
	
	private TermMap termMap;
	
	public SimpleCompatibilityChecker(TermMap tm) {
		this.termMap = tm;
	}
	

	@Override
	public boolean isCompatible(TermMap termMap2) {

		if(termMap.getType().equals(termMap2.getType())){
			
			if(termMap.getType().equals(ColumnHelper.COL_VAL_TYPE_LITERAL)){
				//we check for datatype and language
				
				String datatype1  = termMap.getDataType();
				String datatype2  = termMap2.getDataType();
				
				if(datatype1!=null && datatype2!=null){
					if(!termMap.getDataType().equals(termMap2.getDataType())){
						return false;
					}
				}else if(datatype1!=null && datatype2==null||datatype2!=null && datatype1==null){
					return false;
				}
				
//				Expression lang1 = DataTypeHelper.uncast(termMap.getLanguage());
//				Expression lang2 = DataTypeHelper.uncast(termMap2.getLanguage());
//				
//				if(!lang1.getClass().equals(lang2.getClass())){
//					
//					return false;
//				}
//				
				
				
				
				
				
			}else {
				//is either blank node or resource, so we evaluate the resource constructor
				
				List<Expression> resourceExpr1 = termMap.getResourceExpressions();
				List<Expression> resourceExpr2 = termMap2.getResourceExpressions();
								
				
				
				
				
				
				//check for prefix
				String prefix1 = ((StringValue) DataTypeHelper.uncast(resourceExpr1.get(0))).getNotExcapedValue();
				String prefix2 = ((StringValue) DataTypeHelper.uncast(resourceExpr2.get(0))).getNotExcapedValue();
				
				//if they are constant and identical, they match
				if(resourceExpr1.size()==1 && resourceExpr2.size()==1){
					if(prefix1.equals(prefix2)){
						return true;
					}
				}
				
				//this prefix evaluation is not 100 percent precise, but should do the trick for now.
				if(!prefix1.isEmpty() && !prefix2.isEmpty()){
					//if present, they have to start with the same string an be followed by a column
					
					boolean can1generate2 = prefix1.startsWith(prefix2) && resourceExpr1.size()>1;
					boolean can2generate1 = prefix2.startsWith(prefix1) && resourceExpr2.size()>1;
					
					//if neither can generate the other, they are not compatible
					if(!(can1generate2||can2generate1)){
						return false;
					}
					
				}
				
				//and suffix, if there is any
				
				Expression suffixExpr1 = DataTypeHelper.uncast(DataTypeHelper.uncast(resourceExpr1.get(resourceExpr1.size()-1)));
				Expression suffixExpr2 = DataTypeHelper.uncast(DataTypeHelper.uncast(resourceExpr2.get(resourceExpr2.size()-1)));
				
				if(suffixExpr1 instanceof StringValue && suffixExpr2 instanceof StringValue){
					String suffix1 = ((StringValue)suffixExpr1).getNotExcapedValue();
					String suffix2 = ((StringValue)suffixExpr2).getNotExcapedValue();
					if(!(suffix1.endsWith(suffix2)||suffix2.endsWith(suffix1))){
						return false;
					}
				}
				
				
				
			}
			
			
			
			
		}
		
		//catch all. is filtered out by the database hopefully.
		return true;
		
		
		
	}
	
	
	@Override
	public boolean isCompatible(Node n) {
		if(n.isVariable()){
			return true;
			
		}else if(n.isLiteral()){
			
		
			
			return isCompatibleLiteral(n);
			
		}else if(n.isURI()){
			return isCompatibleUri(n);

			
		}else {
			throw new ImplementationException("Node type not supported, how did it get in there anyway? : " + n.toString());
		}
	}


	private boolean isCompatibleUri(Node n) {
		List<Expression> tmExprs = termMap.getResourceExpressions();
		
		if(tmExprs.isEmpty()){
			return false;
		}
		
		String nodeUri = n.getURI();
		int i = 0;
		while (nodeUri.length()>0&&i<termMap.getLength()){
			if(i%2==0){
				String tmString = ((net.sf.jsqlparser.expression.StringValue)DataTypeHelper.uncast(tmExprs.get(i))).getNotExcapedValue(); 
				if(nodeUri.startsWith(tmString)){
					nodeUri = nodeUri.substring(tmString.length());
				}else{
					return false;
				}
			}else{
				Column tmCol = ((Column)DataTypeHelper.uncast(tmExprs.get(i)));
				
				String potentialColContent; 
				if(tmExprs.size()>(i+2)){
					String nextString = ((net.sf.jsqlparser.expression.StringValue)DataTypeHelper.uncast(tmExprs.get(i+1))).getNotExcapedValue();
					if(!nodeUri.contains(nextString)){
						return false;
					}
					
					potentialColContent = nodeUri.substring(0,nodeUri.indexOf(nextString));
				}else{
					potentialColContent = nodeUri; 
				}
				// do some col testing here
				//if(tmcol.cancontain(potentialColContent)) ....
				//now it is just true...
				nodeUri = nodeUri.substring(potentialColContent.length());
			}
			i++;
		}
		// could be successfully reduced
		
		if(i==tmExprs.size()){
			return true;
		}else{
			return false;
		}
	}


	private boolean isCompatibleLiteral(Node n) {
		//if the term map has no literal expressions, it cannot produce a literal
		if(termMap.getLiteralExpressions().isEmpty()){
			return false;
		}
		
		//we check for the datatype
		//if exactly one of them is null, then it is false
		if((n.getLiteralDatatypeURI() != null && termMap.getDataType() ==null) ||(n.getLiteralDatatypeURI() == null && termMap.getDataType() !=null)){
			return false;
		}
		//if they are not null and different
		if((n.getLiteralDatatypeURI() != null && termMap.getDataType() !=null) && !n.getLiteralDatatypeURI().equals(termMap.getDataType())){
			return false;
		}
		// if the language does not match
		String lang = n.getLiteralLanguage();
		Expression langExpr = termMap.getLanguage();
		if(lang!=null){
			throw new ImplementationException("Implement language compatibility check.");
		}
		
		
		//otherwise we could give it a try, for now
		return true;
	}
	
	
	public boolean isColumnCompatible(Column c1, Column c2){
		return true;
	}

	/**
	 * simple implementation that checks only for simple equals statements 
	 */
	@Override
	public boolean isCompatible(String var, Collection<Expr> exprs) {
		boolean isCompatible = true;
		for(Expr expr : exprs){
			if(expr instanceof E_Equals){
				Var exprVar = null;
				Node exprValue = null;
				E_Equals equals = (E_Equals) expr;
				if(equals.getArg1() instanceof Var && equals.getArg2() instanceof NodeValue){
					exprVar = (Var) equals.getArg1();
					exprValue = (Node) equals.getArg2();
				}
				if(equals.getArg2() instanceof Var && equals.getArg1() instanceof NodeValue){
					exprVar = (Var) equals.getArg2();
					exprValue = (Node) equals.getArg1();
				}
				if(exprVar!=null && exprVar.getName().equals(var) && exprValue !=null){
					isCompatible = isCompatible && isCompatible(exprValue);
				}
				
			}
		}
		
		
		
		return isCompatible;
		
		
	}

	
	
	


}
