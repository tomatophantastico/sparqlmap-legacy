package org.aksw.sparqlmap.core.mapper.compatibility;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.FromItem;

import org.aksw.sparqlmap.core.ImplementationException;
import org.aksw.sparqlmap.core.config.syntax.r2rml.ColumnHelper;
import org.aksw.sparqlmap.core.config.syntax.r2rml.TermMap;
import org.aksw.sparqlmap.core.db.DBAccess;
import org.aksw.sparqlmap.core.mapper.translate.DataTypeHelper;
import org.aksw.sparqlmap.core.mapper.translate.FilterUtil;
import org.apache.commons.lang3.math.NumberUtils;


import com.google.common.base.Splitter;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.expr.E_Equals;
import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.expr.ExprVar;
import com.hp.hpl.jena.sparql.expr.NodeValue;
import com.hp.hpl.jena.sparql.expr.nodevalue.NodeValueNode;

public class SimpleCompatibilityChecker implements CompatibilityChecker{
	
	private TermMap termMap;
	private DataTypeHelper dth;
	//contains the cast type of the column, that would naturally be used (e.g. an tinyint will get for postgres NUMERIC)
	private Map<String,String> colname2castType = new HashMap<String, String>(); 
	
	public SimpleCompatibilityChecker(TermMap tm, DBAccess dba, DataTypeHelper dth) {
		this.termMap = tm;
		this.dth = dth;
		
		//we now create the colname2castType
		for(Expression expression: tm.getExpressions()){
			if(DataTypeHelper.uncast(expression) instanceof Column){
				Column column = (Column) DataTypeHelper.uncast(expression);
				String columnName = column.getColumnName();
				FromItem fi =null;
				
				for(FromItem fiToCheck : tm.getFromItems()){
					if(fiToCheck.getAlias().equals(column.getTable().getAlias())){
						fi = fiToCheck;
					}
				}
				Integer dt = dba.getDataType(fi, columnName);
				 
				 colname2castType.put( columnName ,dth.getCastTypeString(dt));
			}
		}
		
		
	}
	

	@Override
	public boolean isCompatible(TermMap termMap2) {
		
		
		boolean compatibleType = false;
		
		ColumnHelper.getResourceExpressions(termMap.getExpressions());
		
		Expression termMapType = ColumnHelper.getTermType(this.termMap.getExpressions());
		Expression termMap2Type = ColumnHelper.getTermType(termMap2.getExpressions());

		if(isCompatible(termMapType, termMap2Type)){
			
			if(termMapType.equals(ColumnHelper.COL_VAL_TYPE_LITERAL)){
				//we check for datatype and language
				
				Expression datatype1  = ColumnHelper.getDataType(termMap.getExpressions());
				Expression datatype2  = ColumnHelper.getDataType(termMap2.getExpressions());
				
				if(datatype1!=null && datatype2!=null){
					if(!isCompatible(datatype1, datatype2)){
						return false;
					}
				}else if(datatype1!=null && datatype2==null||datatype2!=null && datatype1==null){
					return false;
				}else{
					throw new ImplementationException("Check non considered case");
				}
				
			}else {
				//is either blank node or resource, so we evaluate the resource constructor
				
				List<Expression> resourceExpr1 = ColumnHelper.getResourceExpressions(termMap.getExpressions());
				List<Expression> resourceExpr2 = ColumnHelper.getResourceExpressions(termMap2.getExpressions());
								

				//the uri separator split list. Currently only split for  "/"
				List<Object> splitUri1 = split(resourceExpr1);
				List<Object> splitUri2 = split(resourceExpr2);
				
				return evaluateSplits(splitUri1,splitUri2);
		
			}
		}else{
			return false;
		}
		// fallback
		
		return true;
	}
	
	
	private boolean evaluateSplits(List<Object> splitUri1,
		List<Object> splitUri2) {
		
		
		//the basic structure of these splits has to be the same
		if(splitUri1.size()!=splitUri2.size()){
			return false;
		}
		
		
		Iterator<Object> iter1 = splitUri1.iterator();
		Iterator<Object> iter2 = splitUri2.iterator();
	
		
		
	
		
		while(iter1.hasNext() && iter2.hasNext()){
			
			Object current1 = iter1.next();
			Object current2 = iter2.next();
			
			if(current1 instanceof String && current2 instanceof String){
				//check if is the same separator
				if(!((String)current1).equals(((String)current1))){
					return false;
				}
			} else if(current1 instanceof List<?> && current2 instanceof List<?>){
				//we check the separator-free part.
				List<Object> list1 = (List<Object>) current1;
				List<Object> list2 = (List<Object>) current2;
				
				//we only have to check for the first and last parts.
				Object prefixExpr1 = list1.get(0);
				Object prefixExpr2 = list2.get(0);
				
				if(prefixExpr1 instanceof String && prefixExpr2 instanceof String){
					String prefix1 = ((String)prefixExpr1);
					String prefix2 = ((String)prefixExpr2);
					if(!(prefix1.startsWith(prefix2)||prefix2.startsWith(prefix1))){
						return false;
					}
				}

				Object suffixExpr1 = list1.get(list1.size()-1);
				Object suffixExpr2 = list2.get(list2.size()-1);
				
				if(suffixExpr1 instanceof String && suffixExpr2 instanceof String){
					String suffix1 = ((String)suffixExpr1);
					String suffix2 = ((String)suffixExpr2);
					if(!(suffix1.endsWith(suffix2)||suffix2.endsWith(suffix1))){
						return false;
					}
				}

			}else{
				throw new ImplementationException("Bad uri-separator split resource expressions");
			}
			
		}
		
		
		return true;
			
			
		
	

		
	}
	
	

	/**
	 * creates an list of objects, following this scheme: if a 
	 * string is present, than it is a uri separator. if there is a list in there, then we have a sequence of non-uri-separators and 
	 * 
	 * @param resourceExpr1
	 * @return
	 */
	private List<Object> split(List<Expression> resourceExpr) {
		
		List<Object> uriSeparatedSplit = new ArrayList<Object>();
		
		for(Expression expression : resourceExpr){
			if(DataTypeHelper.uncast(expression) instanceof StringValue){
				
				
				String stringResourcepart = ((StringValue)DataTypeHelper.uncast(expression)).getNotExcapedValue();
				StringBuilder nonSeparatorPart = new StringBuilder();
				for(char c : stringResourcepart.toCharArray()){
					if(c =='/'){
						//put the string builder content, if there is any into the list
						if(nonSeparatorPart.length()>0){
							
							putNonSeparatorInPlace(uriSeparatedSplit,
									nonSeparatorPart);
							nonSeparatorPart = new StringBuilder();
						}
						uriSeparatedSplit.add(String.valueOf(c));
					}else{
						nonSeparatorPart.append(c);
					}
				}
				//putting the end also in the arry
				if(nonSeparatorPart.length()>0){
					
					putNonSeparatorInPlace(uriSeparatedSplit,
							nonSeparatorPart);
				}
				Iterator<String> iter =  Splitter.on("/").split(stringResourcepart).iterator();	
				
			}else
			if (DataTypeHelper.uncast(expression)instanceof Column){
				
				Column col = (Column) DataTypeHelper.uncast(expression);
				
				//peek at the list, and attach at the right place.
				if(uriSeparatedSplit.size()==0 || uriSeparatedSplit.get(uriSeparatedSplit.size()-1) instanceof String){
					List<Object> nonSeparators = new  ArrayList<Object>();
					nonSeparators.add(col);
					uriSeparatedSplit.add(nonSeparators);
				}else{
					List<Object> nonSeparators = (List<Object>) uriSeparatedSplit.get(uriSeparatedSplit.size()-1);
					nonSeparators.add(col);
				}
			}
			
		}
		
		return uriSeparatedSplit;
	}


	private void putNonSeparatorInPlace(List<Object> uriSeparatedSplit,
			StringBuilder nonSeparatorPart) {
		//put the string in the right place
		if(uriSeparatedSplit.size()==0 || uriSeparatedSplit.get(uriSeparatedSplit.size()-1) instanceof String){
			List<Object> nonSeparators = new  ArrayList<Object>();
			nonSeparators.add(nonSeparatorPart.toString());
			uriSeparatedSplit.add(nonSeparators);
		}else{
			List<Object> nonSeparators = (List<Object>) uriSeparatedSplit.get(uriSeparatedSplit.size()-1);
			nonSeparators.add(nonSeparatorPart.toString());
		}
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
		List<Expression> tmExprs = termMap.getResourceColSeg();
		
		if(tmExprs.isEmpty()){
			return false;
		}
		
		String nodeUri = n.getURI();
		int i = 0;
		while (nodeUri.length()>0&&i<tmExprs.size()){
			if(i%2==0){
				if(DataTypeHelper.uncast(tmExprs.get(i)) instanceof StringValue){
					String tmString = ((net.sf.jsqlparser.expression.StringValue)DataTypeHelper.uncast(tmExprs.get(i))).getNotExcapedValue(); 
					if(nodeUri.startsWith(tmString)){
						nodeUri = nodeUri.substring(tmString.length());
					}else{
						return false;
					}
				}else{
					//as it is not a Stringvalue, it must be a column, which is allowed here for values not to be encoded
					//we can return true
					
					return true;
				}
			}else{
				//here be a column
				Column column = ((Column)DataTypeHelper.uncast(tmExprs.get(i))); 

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
				// do some col schema testing here
		
				//check if  col is a number, that the string can be cast to a number 
				String colNaturalCastType = colname2castType.get(column.getColumnName());
				if(colNaturalCastType.equals(dth.getNumericCastType())){
					if(!NumberUtils.isNumber(potentialColContent)){
						return false;
					}
				}
				
				if(potentialColContent.isEmpty()){
					return false;
				}
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
		if(ColumnHelper.getLiteralExpression(termMap.getExpressions()).isEmpty()){
			return false;
		}
		
		//we check for the datatype
		//if exactly one of them is null, then it is false
		if((n.getLiteralDatatypeURI() != null && ColumnHelper.getDataType(termMap.getExpressions()) ==null) 
				||(n.getLiteralDatatypeURI() == null && ColumnHelper.getDataType(termMap.getExpressions()) !=null)){
			return false;
		}
		//if they are not null and different
		if((n.getLiteralDatatypeURI() != null && ColumnHelper.getDataType(termMap.getExpressions()) !=null) && !n.getLiteralDatatypeURI().equals(ColumnHelper.getDataType(termMap.getExpressions()))){
			return false;
		}
		// if the language does not match
		String lang = n.getLiteralLanguage();
		Expression langExpr = ColumnHelper.getLanguage(termMap.getExpressions());
		if(lang!=null){
			throw new ImplementationException("Implement language compatibility check.");
		}
		
		
		//otherwise we could give it a try, for now
		return true;
	}
	
	
	public static boolean isCompatible(Expression e1, Expression e2){
		Expression v1 = DataTypeHelper.uncast(e1);
		Expression v2 = DataTypeHelper.uncast(e2);
		
		if(v1 instanceof Expression && v2 instanceof Expression){
			return v2.equals(v1);
			
		}
		
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
				ExprVar exprVar = null;
				NodeValueNode exprValue = null;
				E_Equals equals = (E_Equals) expr;
				if(equals.getArg1() instanceof ExprVar && equals.getArg2() instanceof NodeValue){
					exprVar = (ExprVar) equals.getArg1();
					exprValue = (NodeValueNode) equals.getArg2();
				}
				if(equals.getArg2() instanceof ExprVar && equals.getArg1() instanceof NodeValue){
					exprVar = (ExprVar) equals.getArg2();
					exprValue = (NodeValueNode) equals.getArg1();
				}
				if(exprVar!=null && exprVar.getVarName().equals(var) && exprValue !=null){
					
					//if an equals check is not true, this term map is not compatible to the expression presented here
					isCompatible = isCompatible && isCompatible(exprValue.asNode());
				}
				
			}
		}
		return isCompatible;
		
		
	}

	
	
	


}
