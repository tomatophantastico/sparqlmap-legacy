package org.aksw.sparqlmap.core.mapper.translate;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import org.aksw.sparqlmap.core.ImplementationException;
import org.aksw.sparqlmap.core.config.syntax.r2rml.TermMap;
import org.aksw.sparqlmap.core.config.syntax.r2rml.TermMapFactory;
import org.aksw.sparqlmap.core.db.DBAccess;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.protocol.HTTP;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.collect.Collections2;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;
import com.hp.hpl.jena.assembler.exceptions.NotExpectedTypeException;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringExpression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.FromItem;

@Component
public class FilterUtil {

	static org.slf4j.Logger log = org.slf4j.LoggerFactory
			.getLogger(FilterUtil.class);
	
	@Autowired
	private OptimizationConfiguration optConf;
	
	@Autowired
	private DBAccess dbaccess;
	
	@Autowired
	private DataTypeHelper dth;
	
	@Autowired
	private TermMapFactory tmf;
	

	
	public OptimizationConfiguration getOptConf() {
		return optConf;
	}
	
public static String CONCAT = "CONCAT";

private static BitSet RESERVED = new BitSet();

	public static Expression concat(Expression... expr) {
		
		if(expr.length==0){
			return new NullValue();
		}else if(expr.length==1){
			return expr[0];
		}
		
		Function concat = new Function();
		concat.setName(CONCAT);
		ExpressionList explist = new ExpressionList();
		explist.setExpressions(Arrays.asList(expr));
		concat.setParameters(explist);

		return concat;
	}
	public static Expression coalesce(Expression... expr) {
		if(expr.length==1){
			return expr[0];
		}else if(expr.length>1){
		
		Function concat = new Function();
		concat.setName("COALESCE");
		ExpressionList explist = new ExpressionList();
		explist.setExpressions(Arrays.asList(expr));
		concat.setParameters(explist);

		return concat;
		}else{
			return null;
		}
	}




	
	

	
	
	public static void splitFilters(Expression expr,List<Expression> putInto){
		
		if(expr instanceof AndExpression){
			splitFilters(((AndExpression) expr).getLeftExpression(), putInto);
			splitFilters(((AndExpression) expr).getRightExpression(), putInto);
		}else{
			putInto.add(expr);
		}
		

	}
	
	
	public static Expression conjunct(Collection<Expression> exprs) {
		exprs = new ArrayList<Expression>(exprs);
		if (exprs.isEmpty()) {
			return null;
		} else if (exprs.size() == 1) {
			return exprs.iterator().next();
		} else {
			Expression exp = exprs.iterator().next();
			exprs.remove(exp);
			AndExpression and = new AndExpression(exp, conjunct(exprs));
			return and;
		}
	}
	
	public static Expression disjunct(Collection<Expression> exprs) {
		exprs = new ArrayList<Expression>(exprs);
		if (exprs.isEmpty()) {
			return null;
		} else if (exprs.size() == 1) {
			return exprs.iterator().next();
		} else {
			Expression exp = exprs.iterator().next();
			exprs.remove(exp);
			OrExpression and = new OrExpression(exp, disjunct(exprs));
			return and;
		}
	}
	
	
	
	

	/**
	 * Here the join is created.
	 * 
	 * @param left
	 * @param right
	 * @param test
	 * @return
	 */
	
	public TermMap compareTermMaps(TermMap left, TermMap right, Class<? extends BinaryExpression> test){
		
		
		List<Expression> eqs = new ArrayList<Expression>();
		try{
		
		// and all the other fields that might be null
//		Expression literalTypeEquality = bothNullOrBinary(left.literalType, right.literalType, test.newInstance());
//		eqs.add(literalTypeEquality);
		if(!isAlwaysTrue(left.literalValBinary, right.literalValBinary)){
			Expression literalBinaryEquality = bothNullOrBinary(left.literalValBinary, right.literalValBinary, test.newInstance(),dth);
			eqs.add(literalBinaryEquality);
		}	
		if(!isAlwaysTrue(left.literalValBool, left.literalValBool)){
			Expression literalBoolEquality = bothNullOrBinary(left.literalValBool,right.literalValBool,test.newInstance(),dth);
			eqs.add(literalBoolEquality);
		}
		
		if(!isAlwaysTrue(left.literalValDate, right.literalValDate)){
			Expression literalDateEquality = bothNullOrBinary(left.literalValDate, right.literalValDate, test.newInstance(),dth);
			eqs.add(literalDateEquality);
		}
		
		if(!isAlwaysTrue(left.literalValNumeric, right.literalValNumeric)){
			Expression literalNumericEquality = bothNullOrBinary(left.literalValNumeric, right.literalValNumeric, test.newInstance(),dth);
			eqs.add(literalNumericEquality);
		}
		
		if(!isAlwaysTrue(left.literalValString, right.literalValString)){
			Expression literalStringEquality = bothNullOrBinary(left.literalValString,right.literalValString, test.newInstance(),dth);
			eqs.add(literalStringEquality);
		}
		
		//and check for the resources
		
		if(left.resourceColSeg.size()==0&&right.resourceColSeg.size()==0){
			//no need to do anything
		}else{
			if(test.equals(NotEqualsTo.class)||test.equals(EqualsTo.class)){
				Expression resourceEquality = compareResource(left, right, test);
				if(resourceEquality!=null){
					eqs.add(resourceEquality);
				}
				
			}else{
				//only equals and not-equals are defined.
				eqs.clear();
				eqs.add(new NullValue());
			}
		}
		
		// and check that not all of any side are null
		
		
		Expression leftNull = areAllNull(left.getExpressions());
		Expression rightNull = areAllNull(right.getExpressions());
		
		if(leftNull != null && rightNull != null){
			OrExpression anySideCompletelyNull = new OrExpression(
					new Parenthesis(leftNull), 
					new Parenthesis(rightNull));

			Parenthesis not = new Parenthesis();
			not.setNot();
			not.setExpression(anySideCompletelyNull);
			
			eqs.add(anySideCompletelyNull);
		}
		
		
		} catch (InstantiationException | IllegalAccessException e) {
			log.error("Error creating xpathtest",e);
		}
		
		if(eqs.isEmpty()){
			return tmf.createBoolTermMap( new StringExpression("true"));
		}else{
			return  tmf.createBoolTermMap( conjunct(eqs));
		}
		
	}
	
	/**
	 * 
	 * @param exprs
	 * @return null, if there is at least one constant value, otherwise an expression with the necessary checks.
	 */
	private Expression areAllNull(List<Expression> exprs){
		List<Expression> areNulls = Lists.newArrayList();
		for(Expression expr: exprs){
			
			expr = DataTypeHelper.uncast(expr);
			
			// check if we have an constant value 
			if(!(expr instanceof NullValue) && !(expr instanceof Column)){
				return null;
			}
			
			if(expr instanceof NullValue){
				// make sure there is at least one "true" value in it
				if (areNulls.isEmpty()) {
					areNulls.add(
							dth.cast(new StringValue("true"), 
									dth.getBooleanCastType()));
				}
			}else{
				IsNullExpression in = new IsNullExpression();
				in.setLeftExpression(expr);
				areNulls.add(in);
			}
						
			
		}
		
		return conjunct(areNulls);
			
	}
	
	
	private Expression compareResource(TermMap left, TermMap right,
			Class<? extends BinaryExpression> test)
			throws InstantiationException, IllegalAccessException {
		
		List<Expression> tests = new ArrayList<Expression>();
		
		
		//check if any of the term maps is produced by a subselect
		
		boolean isSubsel = hasSubsel(left) || hasSubsel(right);

		
		
		if(!optConf.shortcutFilters||isSubsel){
			BinaryExpression resourceEq=  test.newInstance();
			
			resourceEq.setLeftExpression(FilterUtil.concat(left.resourceColSeg.toArray(new Expression[0])));
			resourceEq.setRightExpression(FilterUtil.concat(right.resourceColSeg.toArray(new Expression[0])));
			
			Expression resourceEquality = bothNullOrBinary(FilterUtil.concat(left.resourceColSeg.toArray(new Expression[0])), FilterUtil.concat(right.resourceColSeg.toArray(new Expression[0])),resourceEq,dth);
			return resourceEquality;
		}else{
			
			Iterator<Expression> leftExprIter = left.getResourceColSeg().iterator();
			Iterator<Expression> rightExprIter = right.getResourceColSeg().iterator();
			Object currentLeft = null;
			Object currentRight = null;
			
			while( currentLeft!=null || leftExprIter.hasNext() ||  currentRight!=null|| rightExprIter.hasNext()){
				
				if(currentLeft==null && leftExprIter.hasNext()){
					currentLeft = DataTypeHelper.uncast(leftExprIter.next());
				}
				if(currentRight==null && rightExprIter.hasNext()){
					currentRight = DataTypeHelper.uncast(rightExprIter.next());
				}
				
				if(currentLeft==null||currentRight==null){
					// there is something left, for which the other part has nothing to match, so it is false;
					return new StringExpression("false");
				}
				
				
				
				
				if(currentLeft instanceof StringValue){
					String leftString = ((StringValue) currentLeft).getNotExcapedValue();
					if(currentRight instanceof StringValue){
						//both String case
						String rightString = ((StringValue) currentRight).getNotExcapedValue();
						String newLeftString = removeStringValue(leftString, rightString);
						
						
						if(newLeftString == null){
							return new NullValue();
						}else if(!newLeftString.isEmpty()){
							currentLeft = new StringValue("'"+newLeftString+"'");
						}else{
							currentLeft = null;
						}
						String newRightString = removeStringValue(rightString, leftString);
						if(newRightString == null){
							return new NullValue();
						}else	if(!newRightString.isEmpty()){
							currentRight = new StringValue("'"+newRightString+"'");
						}else{
							currentRight = null;
						}
						
					}else{
						// one col, one String case
						Column rightColumn = (Column) currentRight;
						//remove everything from the string, the column could produce
						String remains = removeColValue(leftString, rightColumn);
						
						currentRight = null;
						
						if(remains == null){
							return new NullValue();
						}
						String testVal = leftString.substring(0, leftString.length()-remains.length());
						BinaryExpression comp = test.newInstance();
						comp.setLeftExpression(rightColumn);
						comp.setRightExpression(new StringValue("'" + testVal+"'"));
						tests.add(comp);
						
						if(!remains.isEmpty()){
							currentLeft = new StringValue("'"+remains+"'");
						}else{
							currentLeft = null;
						}
						
					
					}
					
				}else{
					Column leftColumn = (Column) currentLeft;
					
					if(currentRight instanceof StringValue){
						String rightString = ((StringValue) currentRight).getNotExcapedValue();
						String remains = removeColValue(rightString,leftColumn );
						
						currentLeft = null;
						if(remains == null){
							return new NullValue();
						}
						String testVal = rightString.substring(0, rightString.length()-remains.length());
						BinaryExpression comp = test.newInstance();
						comp.setLeftExpression(leftColumn);
						comp.setRightExpression(new StringValue("'" + testVal+"'"));
						tests.add(comp);
						
						if(!remains.isEmpty()){
							currentRight = new StringValue("'"+remains+"'");
						}else{
							currentRight = null;
						}
						
						
					}else{
						//both Column, as no sophisticated check for their content is available, they are just assumed to be comparable
						Column rightColumn = (Column) currentRight;
						
						BinaryExpression comp = test.newInstance();
						comp.setLeftExpression(leftColumn);
						comp.setRightExpression(rightColumn);
						tests.add(comp);
						
						currentLeft =null;
						currentRight = null;
						
					}
					
				}
				
				
				
				

			}
			
						
			return FilterUtil.conjunct(tests);
		}
	}
	public boolean hasSubsel(TermMap left) {
		for (Expression expr : left.getResourceColSeg()){
			 if(expr instanceof Column){
				 //maybe not the best way to determine that.
				 if(((Column) expr).getTable().getName().startsWith(PlainSelectWrapper.SUBSEL_SUFFIX)){
					 return true;
				 }
			 }
		 }
		return false;
	}
	
	
	public String removeColValue(String uripart, Column col){
		
		Integer colDataType =  dbaccess.getDataType(col.getTable().getName(), col.getColumnName());
		
		
		if(colDataType==null || dth.getStringCastType().equals(dth.getCastTypeString(colDataType))){
			//we need to go to the next uri delimiter, which require going 
			byte[] chars = uripart.getBytes();
			int i = 0;
			
			
			for(;i<chars.length;i++){
				if(RESERVED.get(chars[i])){
					break;
				}
			}
			return uripart.substring(i);
			
		}else if(dth.getNumericCastType().equals(dth.getCastTypeString(colDataType))){
			char[] chars = uripart.toCharArray();
			int i = 0;
			for(;i<chars.length;i++){
				if(!Character.isDigit(chars[i])){
					break;
				}
			}
			return uripart.substring(i);

			
		}else{
			throw new ImplementationException("Check for non-String/Integer data types in uris");
		}
	
	}
	
	public String removeStringValue(String left, String right){
		if(left.length()<right.length() && right.startsWith(left)){
			return "";
		}else if(left.startsWith(right)){
			return left.substring(right.length());
		}else{
			return null;
		}
	}
	
	public boolean compareColumns(Column left, Column right){
		
		//check here for basic compati
		return true;
	}

	static {
	
	    RESERVED.set(';');
	    RESERVED.set('/');
	    RESERVED.set('?');
	    RESERVED.set(':');
	    RESERVED.set('@');
	    RESERVED.set('&');
	    RESERVED.set('=');
	    RESERVED.set('+');
	    RESERVED.set('$');
	    RESERVED.set(',');
	    RESERVED.set('[');
	    RESERVED.set(']');
	
	
	}
	
	
	
	
	//break expressions down
	
	
//	String toReduce = stringOrig.getNotExcapedValue();
//	List<Expression> parameter = new ArrayList<Expression>();
//	for(Object oexp: func.getParameters()
//			.getExpressions()){
//		parameter.add(DataTypeHelper.uncast((Expression) oexp));
//	}
//		
//	int psize = parameter.size();
//	
//	for (int i = 0; i < psize; i += 2) {
//		if (parameter.get(i) instanceof StringValue) {
//
//			String value = ((StringValue) parameter.get(i))
//					.getNotExcapedValue();
//			Column col = null;
//			String prereadsuffix = null;
//			if ((i + 1 < psize)
//					&& parameter.get(i + 1) instanceof Column) {
//				col = (Column) parameter.get(i + 1);
//			}
//			if ((i + 2 < psize)
//					&& parameter.get(i + 2) instanceof StringValue) {
//				prereadsuffix = ((StringValue) parameter
//						.get(i + 2)).getNotExcapedValue();
//			}
//
//			if (toReduce.startsWith(value)) {
//				// delete this part
//				toReduce = toReduce.substring(value.length());
//				reduced = true;
//			}else{
//				break;
//			}
//
//			if (col != null && prereadsuffix != null &&
//			// is the static value at the end?
//			 
//					toReduce.contains(prereadsuffix) ) {
//				// no, there is a static part following
//				String colvalue = toReduce.substring(0,
//						toReduce.indexOf(prereadsuffix));
//				toReduce = toReduce.substring(toReduce
//						.indexOf(prereadsuffix));
//				stringNew.add(new StringValue("\"" + colvalue
//						+ "\""));
//				compareto.add(col);
//				reduced = true;
//			} else if (col != null) {
//				// yes, we are finished
//				stringNew.add(new StringValue("\"" + toReduce
//						+ "\""));
//				compareto.add(col);
//				reduced = true;
//			}
//		} else {
//			
//			
//			log.debug("No Filtershortcutting for " + sqlExpression);
//			//							throw new ImplementationException(
////								"Should never come here, resource columns should always be string + col + string + col ...., but is: "
////										+ func);
//		}
//	}
//	return reduced;
//	
//	
	
	
	
	
	
	
	
	
	private Expression bothNullOrBinary(Expression expr1, Expression expr2, BinaryExpression function, DataTypeHelper dth){
		
		// odd, but left and right seems to be twisted
		function.setLeftExpression(expr2);
		function.setRightExpression(expr1);
		Parenthesis pt = new Parenthesis( bothNullOr(expr1, expr2, function,dth));
		
		return pt;
		
	}
	
	/**
	 * Check if the comparison is between two null values and will therefore always be true. Also check for same content for static values.
	 * @return
	 */
	private boolean isAlwaysTrue(Expression left, Expression right){
		if(optConf.shortcutFilters){
			left = DataTypeHelper.uncast(left);
			right = DataTypeHelper.uncast(right);
			//check for both null
			
			if(left instanceof NullValue && right instanceof NullValue){
				return true;
			}
			//check for both same constant value
			if(left instanceof StringValue && right instanceof StringValue){
				if (((StringValue)left).getNotExcapedValue().equals(((StringValue)right).getNotExcapedValue())){
					return true;
				}
			}
			if(left instanceof LongValue && right instanceof LongValue){
				if (((LongValue)left).getStringValue().equals(((LongValue)right).getStringValue())){
					return true;
				}
			}
		}
		return false;
	}
	
	
	
	
	
	
	
	private Expression bothNullOr(Expression expr1, Expression expr2, Expression function, DataTypeHelper dth){
		Expression uncast1 = DataTypeHelper.uncast(expr1);
		Expression uncast2 = DataTypeHelper.uncast(expr2);
		if(optConf.isShortcutFilters()){
			if(uncast1 instanceof NullValue && uncast2 instanceof NullValue){
				return dth.cast(new StringExpression("true"), dth.getBooleanCastType());
			}
		}
		
		List<Expression> isNulls = Lists.newArrayList();	
		if(!(uncast1 instanceof NullValue)|| !optConf.isShortcutFilters()){
			IsNullExpression literalTypeLeftIsNull = new IsNullExpression();
			literalTypeLeftIsNull.setLeftExpression(uncast1);
			isNulls.add(literalTypeLeftIsNull);
		}
		if(!(uncast2 instanceof NullValue) || !optConf.isShortcutFilters() ){
			IsNullExpression literalTypeRightIsNull = new IsNullExpression();
			literalTypeRightIsNull.setLeftExpression(uncast2);
			isNulls.add(literalTypeRightIsNull);
		}
		
		
		Expression isNullCheck =  FilterUtil.conjunct(isNulls);

		
		
		WhenClause bothNull = new WhenClause();
		bothNull.setWhenExpression(isNullCheck);
		bothNull.setThenExpression(dth.cast(new StringValue("'true'"),dth.getBooleanCastType()));
		
		
		CaseExpression caseExpr = new CaseExpression();
		caseExpr.setWhenClauses(Arrays.asList(((Expression)bothNull)));
		
		caseExpr.setElseExpression(function);
		
		
		return caseExpr;
	}
	
	
	
	

}
