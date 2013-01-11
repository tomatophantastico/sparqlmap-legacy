package org.aksw.sparqlmap.mapper.translate;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.FromItem;

public abstract class FilterUtil {

	static org.slf4j.Logger log = org.slf4j.LoggerFactory
			.getLogger(FilterUtil.class);




	public static List<EqualsTo> createEqualsTos(List<Expression> left,
			List<Expression> right) {
		List<EqualsTo> eqs = new ArrayList<EqualsTo>();

		left = new ArrayList<Expression>(left);
		right = new ArrayList<Expression>(right);

		// purge identical values
		List<Expression> lremove = new ArrayList<Expression>();
		List<Expression> rremove = new ArrayList<Expression>();
		for (int i = 0; i < left.size() && i < right.size(); i++) {
			if (left.get(i).toString().equals(right.get(i).toString())) {
				lremove.add(left.get(i));
				rremove.add(right.get(i));
			}
		}

		left.removeAll(lremove);
		right.removeAll(rremove);

		if (left.size() == 0 && right.size() == 0) {
			EqualsTo eq = new EqualsTo();
			eq.setLeftExpression(new StringValue("\"true\""));
			eq.setRightExpression(new StringValue("\"true\""));
			eqs.add(eq);
		} else if (left.size() != right.size()) {
			EqualsTo eq = new EqualsTo();
			eq.setLeftExpression(concat(left.toArray(new Expression[0])));
			eq.setRightExpression(concat(right.toArray(new Expression[0])));
			eqs.add( eq);
		} else {
			for (int i = 0; i < left.size(); i++) {
				EqualsTo eq = new EqualsTo();
				eq.setLeftExpression(left.get(i));
				eq.setRightExpression(right.get(i));
				eqs.add(eq);
				
			}
		}
		return  eqs;

	}

	public static Expression createEqualsTo(List<Expression> left,
			List<Expression> right) {
		
		Iterator<EqualsTo> eqs = createEqualsTos(left, right).iterator(); 
		Expression eq = eqs.next();
		while (eqs.hasNext()) {
			EqualsTo next = eqs.next();
			AndExpression and = new AndExpression(eq,next);
			eq = and;
		}
		return eq;
	}
	
	public static List<NotEqualsTo> createNotEqualsTos(List<Expression> left,
			List<Expression> right) {
		List<NotEqualsTo> neqs = new ArrayList<NotEqualsTo>();

		left = new ArrayList<Expression>(left);
		right = new ArrayList<Expression>(right);

		// purge identical values
		List<Expression> lremove = new ArrayList<Expression>();
		List<Expression> rremove = new ArrayList<Expression>();
		for (int i = 0; i < left.size() && i < right.size(); i++) {
			if (left.get(i).toString().equals(right.get(i).toString())) {
				lremove.add(left.get(i));
				rremove.add(right.get(i));
			}
		}

		left.removeAll(lremove);
		right.removeAll(rremove);

		if (left.size() == 0 && right.size() == 0) {
			NotEqualsTo neq = new NotEqualsTo();
			neq.setLeftExpression(new StringValue("\"true\""));
			neq.setRightExpression(new StringValue("\"true\""));
			neqs.add(neq);
		} else if (left.size() != right.size()) {
			NotEqualsTo neq = new NotEqualsTo();
			neq.setLeftExpression(concat(left.toArray(new Expression[0])));
			neq.setRightExpression(concat(right.toArray(new Expression[0])));
			neqs.add( neq);
		} else {
			for (int i = 0; i < left.size(); i++) {
				NotEqualsTo neq = new NotEqualsTo();
				neq.setLeftExpression(left.get(i));
				neq.setRightExpression(right.get(i));
				neqs.add(neq);
				
			}
		}
		return  neqs;

	}

	public static Expression createNotEqualsTo(List<Expression> left,
			List<Expression> right) {
		
		Iterator<NotEqualsTo> eqs = createNotEqualsTos(left, right).iterator(); 
		Expression neq = eqs.next();
		while (eqs.hasNext()) {
			NotEqualsTo next = eqs.next();
			AndExpression and = new AndExpression(neq,next);
			neq = and;
		}
		return neq;
	}

	

	

	
	/**
	 * this method does nothing to the expresssion,
	 * 
	 * @param sqlExpression
	 * @return
	 */
	private Expression checkCompatibility(Expression sqlExpression) {
		// TODO Auto-generated method stub
		return sqlExpression;
	}

	
	
	public static String CONCAT = "CONCAT";

	public static Expression concat(Expression... expr) {
		
		if(expr.length==1){
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

	//
	// private Column getCol(Map<String,String>
	// colstring2var,Map<ColumDefinition,String> colstring2col, String varname){
	// Column retcol = null;
	// for(ColumDefinition col :col2var.keySet()){
	// if(col2var.get(col).equals(varname)){
	// retcol = col.getColum();
	// }
	// }
	// return retcol;
	// }



	



	

	
	
	
	
	
	public static Expression conjunctFilters(Collection<Expression> exps) {
		if (exps.isEmpty()) {
			return null;
		} else if (exps.size() == 1) {
			return exps.iterator().next();
		} else {
			Expression exp = exps.iterator().next();
			exps.remove(exp);
			AndExpression and = new AndExpression(exp, conjunctFilters(exps));
			return and;
		}

	}
	/**
	 * use this method to get the other from item in a join condition
	 * @param equalsTo
	 * @param fi
	 * @return
	 */

	public static FromItem getOtherFromItem(EqualsTo equalsTo, FromItem fi) {
		
		FromItem fi1 = ((Column)DataTypeHelper.uncast(equalsTo.getLeftExpression())).getTable();
		FromItem fi2 = ((Column)DataTypeHelper.uncast(equalsTo.getRightExpression())).getTable();
		
		if(fi.getAlias().equals(fi1.getAlias())){
			return fi2;
		}else if (fi.getAlias().equals(fi2.getAlias())){
			return fi1;
		}else{
			throw new ImplementationException("not a correct join condition ");
		}
		
	}
	
	
	

}
