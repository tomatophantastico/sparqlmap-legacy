package org.aksw.sparqlmap.core.mapper.translate;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.OrderByExpressionElement;

import org.aksw.sparqlmap.core.config.syntax.r2rml.TermMap;
import org.apache.commons.lang.reflect.MethodUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.collect.BiMap;
import com.hp.hpl.jena.graph.Node_URI;
import com.hp.hpl.jena.query.SortCondition;
import com.hp.hpl.jena.sparql.algebra.op.OpOrder;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.expr.E_Add;
import com.hp.hpl.jena.sparql.expr.E_Bound;
import com.hp.hpl.jena.sparql.expr.E_Equals;
import com.hp.hpl.jena.sparql.expr.E_Function;
import com.hp.hpl.jena.sparql.expr.E_GreaterThan;
import com.hp.hpl.jena.sparql.expr.E_Lang;
import com.hp.hpl.jena.sparql.expr.E_LangMatches;
import com.hp.hpl.jena.sparql.expr.E_LessThan;
import com.hp.hpl.jena.sparql.expr.E_LessThanOrEqual;
import com.hp.hpl.jena.sparql.expr.E_LogicalAnd;
import com.hp.hpl.jena.sparql.expr.E_LogicalNot;
import com.hp.hpl.jena.sparql.expr.E_LogicalOr;
import com.hp.hpl.jena.sparql.expr.E_NotEquals;
import com.hp.hpl.jena.sparql.expr.E_Regex;
import com.hp.hpl.jena.sparql.expr.E_SameTerm;
import com.hp.hpl.jena.sparql.expr.E_Str;
import com.hp.hpl.jena.sparql.expr.E_Subtract;
import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.expr.ExprFunction;
import com.hp.hpl.jena.sparql.expr.ExprFunction2;
import com.hp.hpl.jena.sparql.expr.ExprVar;
import com.hp.hpl.jena.sparql.expr.nodevalue.NodeValueDT;
import com.hp.hpl.jena.sparql.expr.nodevalue.NodeValueDouble;
import com.hp.hpl.jena.sparql.expr.nodevalue.NodeValueInteger;
import com.hp.hpl.jena.sparql.expr.nodevalue.NodeValueNode;
import com.hp.hpl.jena.sparql.expr.nodevalue.NodeValueString;
import com.hp.hpl.jena.vocabulary.XSD;


/**
 * This class allows the conversion of SPARQL Expressions into SQL Expression
 * @author joerg
 *
 */
@Component
public class ExpressionConverter {
	
	static org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(ExpressionConverter.class);
	
	@Autowired
	DataTypeHelper dth;
	
	@Autowired
	FilterOptimizer fopt;
	
	private  Expression getBestExpression(Expr expr,
			BiMap<String, String> colstring2var,
			Map<String, TermMap> colstring2col) {
		Expression expression = null;

		if (expr.isVariable()) {

			TermMap tc = colstring2col.get(colstring2var.inverse().get(
					expr.getVarName()));
			if(tc !=null){
				expression = tc.getExpression();
			}else{
				expression = new NullValue();
			}
			

		} else if (expr instanceof NodeValueNode
				&& ((NodeValueNode) expr).getNode() instanceof Node_URI) {

			Node_URI nodeU = (Node_URI) ((NodeValueNode) expr).getNode();
			// String id =this.mconf.getIdForInstanceUri(nodeU.getURI());
			// if(id!=null){
			// expression = new StringValue("'"+id+"'");
			// }else{
			expression = new StringValue("'" + nodeU.getURI() + "'");
			// }

		} else if (expr instanceof NodeValueDT) {
			NodeValueDT nvdt = (NodeValueDT) expr;

			Timestamp ts = new Timestamp(nvdt.getDateTime().toGregorianCalendar().getTime().getTime());
			expression = new TimestampValue("'" + ts.toString() + "'");

		} else if (expr instanceof NodeValueInteger) {
			expression = new LongValue(((NodeValueInteger) expr).getInteger()
					.toString());

		} else if (expr instanceof NodeValueDouble) {
			expression = new LongValue(String.valueOf(((NodeValueDouble) expr)
					.getDouble()));
		} else if (expr instanceof NodeValueString) {
			expression = new StringValue(
					((NodeValueString) expr).asQuotedString());
		} else if (expr instanceof NodeValueDouble) {
			expression = new LongValue(String.valueOf(((NodeValueDouble) expr)
					.getDouble()));

		}  else {
			expression = getSQLExpression(expr, colstring2var, colstring2col);
//			log.warn("encountered unknown variable data type: "
//					+ expr.getClass().getCanonicalName());
		}

		// just to be sure:

		return expression;

	}
	
	/**
	 * simple implementation of the order by expression
	 * 
	 * @param opo
	 * @param var2col
	 * @return
	 */

	public List<OrderByElement> convert(OpOrder opo,
			BiMap<String, String> colstring2var,
			Map<String, TermMap> colstring2col) {

		List<OrderByElement> obys = new ArrayList<OrderByElement>();
		for (SortCondition soCond : opo.getConditions()) {
			
			Expr expr = soCond.getExpression();

			if (expr instanceof ExprVar) {
				
				String var = expr.getVarName();
				TermMap tc  = colstring2col.get(colstring2var.inverse().get(var));

				for(Expression exp :tc.getExpressions()){
					OrderByExpressionElement ob = new OrderByExpressionElement(exp);
					ob.setAsc(soCond.getDirection() == 1 ? false : true);
					obys.add(ob);
				}

			} else if (expr instanceof ExprFunction) {

				Expression expression = getBestExpression(expr,
						colstring2var, colstring2col);

				OrderByExpressionElement ob = new OrderByExpressionElement(expression);
				obys.add(ob);

			} else {
				log.error("Cannot handle " + expr.toString() + " in order by");
			}
			
		}
		return obys;
	}
	
	
	public Expression getSQLExpression(Expr exp,
			BiMap<String, String> colstring2var,
			Map<String, TermMap> colstring2col) {

		Expression sqlExpression = null;
		if (exp instanceof E_GreaterThan) {
			E_GreaterThan sparqlGt = (E_GreaterThan) exp;
			GreaterThan gt = new GreaterThan();
			gt.setLeftExpression(getBestExpression(sparqlGt.getArg1(),
					colstring2var, colstring2col));
			gt.setRightExpression(getBestExpression(sparqlGt.getArg2(),
					colstring2var, colstring2col));
			sqlExpression = gt;

		} else if (exp instanceof E_LessThan) {
			E_LessThan sparqlLt = (E_LessThan) exp;
			MinorThan mt = new MinorThan();
			mt.setLeftExpression(getBestExpression(sparqlLt.getArg1(),
					colstring2var, colstring2col));
			mt.setRightExpression(getBestExpression(sparqlLt.getArg2(),
					colstring2var, colstring2col));
			sqlExpression = mt;
		} else
//		// check for special case of not bound
//		if (exp instanceof E_LogicalNot) {
//			Expr negatedExp = ((E_LogicalNot) exp).getArg();
//			if (negatedExp instanceof E_Bound) {
//				E_Bound boundExpr = (E_Bound) negatedExp;
//				IsNullExpression isNull = new IsNullExpression();
//				Expression left = colstring2col.get(
//						colstring2var.inverse().get(
//								boundExpr.getArg().getVarName()))
//						.getExpression();
//				isNull.setLeftExpression(left);
//				isNull.setNot(true);
//				sqlExpression = isNull;
//
//			} else {
//				throw new ImplementationException(
//						"unsupported negated filter encountered");
//			}
//
//		} else 
			if (exp instanceof E_LangMatches) {
			//check against the lang 
			
			E_LangMatches lm = (E_LangMatches) exp;
			
			
			String lang = ((NodeValueString)lm.getArg2()).asUnquotedString();
			
			
			Function toLower = new Function();
			toLower.setName("lower");
			Expression lang2 = getSQLExpression(lm.getArg1(),colstring2var, colstring2col);
			toLower.setParameters(new ExpressionList(Arrays.asList(lang2)));
			
			EqualsTo eq = new EqualsTo();
			
			eq.setLeftExpression(toLower);
			eq.setRightExpression(new StringValue("\"" + lang.toLowerCase() + "\""));
			sqlExpression = eq;
			
			
			
			
		} else if (exp instanceof E_Lang) {
			//check against the lang 
			
			E_Lang lang = (E_Lang) exp;
			
			Expr arg =  lang.getArg();
			
			if(arg instanceof ExprVar){
				Var var = ((ExprVar) arg).asVar();
				//resolve var directly to take the 
				TermMap tc = colstring2col.get(colstring2var.inverse().get(var.getName()));
				
				sqlExpression = tc.getLanguage();
				
				
				// in the odd case, lang() is applied on a literal, do this
			}else if(arg instanceof NodeValueNode ){
				sqlExpression = new StringValue(((NodeValueNode)arg).asNode().getLiteralLanguage());
			}else{
				throw new ImplementationException("Should not happen");
			}
			

		} else if (exp instanceof E_Bound) {
			E_Bound bound = (E_Bound) exp;
			Expression tobebound = getBestExpression(bound.getArg(), colstring2var, colstring2col);
			
			IsNullExpression isnull  = new IsNullExpression();
			isnull.setNot(true);
			isnull.setLeftExpression(tobebound);
			
			sqlExpression = isnull;
		} else if (exp instanceof E_LogicalNot) {
			E_LogicalNot not = (E_LogicalNot) exp;
			Expression expr = getSQLExpression(not.getArg(), colstring2var, colstring2col);
			Expression negatedExpr = null;
			

			//using reflection, as there are no interfaces to do the job
			
			for (Method method : (expr.getClass().getDeclaredMethods())) {
				
				
				
				
				if(method.getName().equals("setNot")){
					try {
						
						//get the old value
						
						
						
						boolean oldValue = (Boolean) MethodUtils.invokeExactMethod(expr, "isNot", null);
						
						method.invoke(expr, new Boolean((!oldValue)));
					} catch (IllegalArgumentException e) {
						// TODO Auto-generated catch block
						log.error("Error:",e);
					} catch (IllegalAccessException e) {
						// TODO Auto-generated catch block
						log.error("Error:",e);
					} catch (InvocationTargetException e) {
						// TODO Auto-generated catch block
						log.error("Error:",e);
					} catch (NoSuchMethodException e) {
						// TODO Auto-generated catch block
						log.error("Error:",e);
					}
					negatedExpr = expr;
				}
			}
			
			if(negatedExpr !=null){
				sqlExpression = negatedExpr;
			}else{
				log.error("Unable to negate expr" + exp.toString() + " nevertheless continuing");
				sqlExpression = expr;
			}
			
			
		
			
			
			
			
		} else if (exp instanceof E_Regex) {
			E_Regex regex = (E_Regex) exp;

			LikeExpression like = new LikeExpression();

			Expression leftExp = null;
			String var = regex.getArg(1).toString().substring(1);

			leftExp = colstring2col.get(colstring2var.inverse().get(var))
					.getExpression();

			like.setLeftExpression(leftExp);
			like.setRightExpression(new StringValue("'%"
					+ regex.getArg(2)
							.toString()
							.substring(1,
									regex.getArg(2).toString().length() - 1)
					+ "%'"));

			sqlExpression = like;
		} else if (exp instanceof E_NotEquals) {
			E_NotEquals ne = (E_NotEquals) exp;

			NotEqualsTo sqlNe = new NotEqualsTo();
			sqlNe.setLeftExpression(getBestExpression(ne.getArg1(),
					colstring2var, colstring2col));
			sqlNe.setRightExpression(getBestExpression(ne.getArg2(),
					colstring2var, colstring2col));
			sqlExpression = sqlNe;

		} else if (exp instanceof E_LogicalAnd) {
			E_LogicalAnd and = (E_LogicalAnd) exp;

			Expression left = getBestExpression(and.getArg1(), colstring2var,
					colstring2col);
			Expression right = getBestExpression(and.getArg2(), colstring2var,
					colstring2col);

			sqlExpression = new AndExpression(left, right);

		} else if (exp instanceof E_LogicalOr) {

			E_LogicalOr or = (E_LogicalOr) exp;
			Expression left = getBestExpression(or.getArg1(), colstring2var,
					colstring2col);
			Expression right = getBestExpression(or.getArg2(), colstring2var,
					colstring2col);
			sqlExpression = new Parenthesis(new OrExpression(left, right));

		} else if (exp instanceof E_Add) {

			E_Add add = (E_Add) exp;
			Expression left = getBestExpression(add.getArg1(), colstring2var,
					colstring2col);
			Expression right = getBestExpression(add.getArg2(), colstring2var,
					colstring2col);

			Addition sqlAdd = new Addition();
			sqlAdd.setLeftExpression(left);
			sqlAdd.setRightExpression(right);
			sqlExpression = sqlAdd;

		} else if (exp instanceof E_Subtract) {

			E_Subtract sub = (E_Subtract) exp;
			Expression left = getBestExpression(sub.getArg1(), colstring2var,
					colstring2col);
			Expression right = getBestExpression(sub.getArg2(), colstring2var,
					colstring2col);

			Subtraction sqlSub = new Subtraction();
			sqlSub.setLeftExpression(left);
			sqlSub.setRightExpression(right);
			sqlExpression = sqlSub;

		} else if (exp instanceof E_Equals || exp instanceof E_SameTerm) {

			ExprFunction2 eq = (ExprFunction2) exp;
			Expression left = getBestExpression(eq.getArg1(), colstring2var,
					colstring2col);
			Expression right = getBestExpression(eq.getArg2(), colstring2var,
					colstring2col);

			EqualsTo sqlEq = new EqualsTo();
			sqlEq.setLeftExpression(left);
			sqlEq.setRightExpression(right);
			sqlExpression = sqlEq;

		} else if (exp instanceof E_LessThanOrEqual) {

			E_LessThanOrEqual leq = (E_LessThanOrEqual) exp;
			Expression left = getBestExpression(leq.getArg1(), colstring2var,
					colstring2col);
			Expression right = getBestExpression(leq.getArg2(), colstring2var,
					colstring2col);

			MinorThanEquals mt = new MinorThanEquals();

			mt.setLeftExpression(left);
			mt.setRightExpression(right);
			sqlExpression = mt;

		} else if (exp instanceof E_Function) {
			E_Function func = (E_Function) exp;
			if (func.getFunctionIRI().equals(XSD.xdouble.toString())) {

				sqlExpression = dth.cast(
						getBestExpression(func.getArg(1), colstring2var,
								colstring2col), dth.getNumericCastType());
			} else {
				throw new ImplementationException("E_Function for "
						+ func.getFunctionIRI() + " not yet implemented");
			}

		} else if (exp instanceof E_Str) {
			E_Str str = (E_Str) exp;

			sqlExpression = dth.cast(
					getBestExpression(str.getArg(), colstring2var,
							colstring2col), dth.getStringCastType());
		}  else {

			throw new ImplementationException("Filter " + exp.toString()
					+ " not yet implemented");
		}
		//sqlExpression = checkCompatibility(sqlExpression);

		sqlExpression = fopt.shortCut(sqlExpression);

		return sqlExpression;
	}

}
