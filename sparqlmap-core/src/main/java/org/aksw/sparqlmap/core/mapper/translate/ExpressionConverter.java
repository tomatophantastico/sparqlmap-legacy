package org.aksw.sparqlmap.core.mapper.translate;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.OrderByExpressionElement;

import org.aksw.sparqlmap.core.config.syntax.r2rml.ColumnHelper;
import org.aksw.sparqlmap.core.config.syntax.r2rml.TermMap;
import org.aksw.sparqlmap.core.config.syntax.r2rml.TermMapFactory;
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
import com.hp.hpl.jena.sparql.expr.E_GreaterThanOrEqual;
import com.hp.hpl.jena.sparql.expr.E_IsURI;
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
import com.hp.hpl.jena.sparql.expr.ExprFunction0;
import com.hp.hpl.jena.sparql.expr.ExprFunction1;
import com.hp.hpl.jena.sparql.expr.ExprFunction2;
import com.hp.hpl.jena.sparql.expr.ExprVar;
import com.hp.hpl.jena.sparql.expr.ExprVisitor;
import com.hp.hpl.jena.sparql.expr.ExprVisitorBase;
import com.hp.hpl.jena.sparql.expr.ExprWalker;
import com.hp.hpl.jena.sparql.expr.ExprWalker.WalkerBottomUp;
import com.hp.hpl.jena.sparql.expr.NodeValue;
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
	
	@Autowired
	TermMapFactory tmf;
	
	
	/**
	 * simple implementation of the order by expression
	 * 
	 * @param opo
	 * @param var2col
	 * @return
	 */

	public List<OrderByElement> convert(OpOrder opo,
			BiMap<String, TermMap> var2termMap) {

		List<OrderByElement> obys = new ArrayList<OrderByElement>();
		for (SortCondition soCond : opo.getConditions()) {
			
			Expr expr = soCond.getExpression();

			if (expr instanceof ExprVar) {
				
				String var = expr.getVarName();
				TermMap tc  = var2termMap.get(var);

				for(Expression exp :tc.getExpressions()){
					OrderByExpressionElement ob = new OrderByExpressionElement(exp);
					ob.setAsc(soCond.getDirection() == 1 ? false : true);
					obys.add(ob);
				}

			} else if (expr instanceof ExprFunction) {
				
				List<Expr> subexprs = ((ExprFunction) expr).getArgs();
				
				for (Expr subexpr : subexprs) {
					List<Expression> subobyExprs =  asTermMap(subexpr,
							var2termMap).getExpressions();
					for(Expression subobyExpr: subobyExprs){
						OrderByExpressionElement ob = new OrderByExpressionElement(subobyExpr);
						obys.add(ob);
					}
					
					
				}

				
				

			} else {
				log.error("Cannot handle " + expr.toString() + " in order by");
			}
			
		}
		return obys;
	}
	
	
//	/**
//	 * convert SPARQL expressions into a SQL expression that evaluates into a boolean value;
//	 * @param exp
//	 * @param colstring2var
//	 * @param colstring2col
//	 * @return
//	 */
//	public Expression getSQLWhereExpression(Expr exp,BiMap<String, String> colstring2var,
//			Map<String, TermMap> colstring2col){
//		
//		return ColumnHelper.getLiteralBoolExpression(getSQLExpression(exp, colstring2var, colstring2col));
//		
//		
//	}
	
	
	public Expression asFilter(Expr expr, BiMap<String, TermMap> var2termMap){
		
		TermMap tm = asTermMap(expr, var2termMap);
		return DataTypeHelper.uncast(tm.literalValBool);
		
		
		
	}
	
	
	public TermMap asTermMap(Expr expr,BiMap<String, TermMap> var2termMap){
		ExprToTermapVisitor ettm = new ExprToTermapVisitor(var2termMap);
		
		ExprWalker.walk(ettm, expr);
		
			
		
		return ettm.tms.pop();
		
	}
	
	
	public class ExprToTermapVisitor extends ExprVisitorBase{
		Stack<TermMap> tms=  new Stack<TermMap>();
		BiMap<String, TermMap> var2termMap;
	
		
		
		public ExprToTermapVisitor(BiMap<String, TermMap> var2termMap) {
			super();
			this.var2termMap = var2termMap;
		}


		@Override
		public void visit(ExprFunction0 func) {
			// TODO Auto-generated method stub
			super.visit(func);
		}
		
		
		@Override
		public void visit(ExprFunction1 func) {
				throw new ImplementationException("Implement Conversion for " + func.toString());
		}
		
		
		@Override 
		public void visit(ExprFunction2 func) {
			TermMap left = tms.pop();
			TermMap right = tms.pop();
			
			if(func instanceof E_Equals){
				
				putXpathTestOnStack(left, right, EqualsTo.class );
				
			}else if(func instanceof E_NotEquals){
				putXpathTestOnStack(left, right, NotEqualsTo.class);
			}else if(func instanceof E_LessThan){
				putXpathTestOnStack(left, right, MinorThan.class );
			}else if (func instanceof E_LessThanOrEqual){
				putXpathTestOnStack(left, right, MinorThanEquals.class);
			}else if(func instanceof E_GreaterThan){
				putXpathTestOnStack(left, right, GreaterThan.class);
			}else if(func instanceof E_GreaterThanOrEqual){
				putXpathTestOnStack(left, right, GreaterThanEquals.class);
			}
			
			
			else{
				throw new ImplementationException("Expression not implemented:" + func.toString());
			}
			
		}


		public void putXpathTestOnStack(TermMap left, TermMap right, Class<? extends BinaryExpression> test) {
	
				try {
					List<Expression> eqs = new ArrayList<Expression>();
								
					
					// and all the other fields that might be null
//					Expression literalTypeEquality = bothNullOrBinary(left.literalType, right.literalType, test.newInstance());
//					eqs.add(literalTypeEquality);
					
					Expression literalBinaryEquality = bothNullOrBinary(left.literalValBinary, right.literalValBinary, test.newInstance());
					eqs.add(literalBinaryEquality);
					
					Expression literalBoolEquality = bothNullOrBinary(left.literalValBool,right.literalValBool,test.newInstance());
					eqs.add(literalBoolEquality);
					
					Expression literalDateEquality = bothNullOrBinary(left.literalValDate, right.literalValDate, test.newInstance());
					eqs.add(literalDateEquality);
					
					Expression literalNumericEquality = bothNullOrBinary(left.literalValNumeric, right.literalValNumeric, test.newInstance());
					eqs.add(literalNumericEquality);
					
					Expression literalStringEquality = bothNullOrBinary(left.literalValString,right.literalValString, test.newInstance());
					eqs.add(literalStringEquality);
					
					
					//and check for the resources
					
					if(left.resourceColSeg.size()==0&&right.resourceColSeg.size()==0){
						//no need to do anything
					}else{
						BinaryExpression resourceEq=  test.newInstance();
						
						resourceEq.setLeftExpression(FilterUtil.concat(left.resourceColSeg.toArray(new Expression[0])));
						resourceEq.setRightExpression(FilterUtil.concat(right.resourceColSeg.toArray(new Expression[0])));
						
						Expression resourceEquality = bothNullOrBinary(FilterUtil.concat(left.resourceColSeg.toArray(new Expression[0])), FilterUtil.concat(right.resourceColSeg.toArray(new Expression[0])),resourceEq);
						eqs.add(resourceEquality);
					}
					TermMap eqTermMap = tmf.createBoolTermMap(new Parenthesis(FilterUtil.conjunct(eqs)));
					tms.push(eqTermMap);
					
				} catch (InstantiationException | IllegalAccessException e) {
					log.error("Error creating xpathtest",e);
				}
			
		}
		
		Expression bothNullOrBinary(Expression expr1, Expression expr2, BinaryExpression function){
			
			
			function.setLeftExpression(expr1);
			function.setRightExpression(expr2);
			Parenthesis pt = new Parenthesis( bothNullOr(expr1, expr2, function));
			
			return pt;
			
		}
		
		
		Expression bothNullOr(Expression expr1, Expression expr2, Expression function){
			
			IsNullExpression literalTypeLeftIsNull = new IsNullExpression();
			literalTypeLeftIsNull.setLeftExpression(expr1);
			IsNullExpression literalTypeRightIsNull = new IsNullExpression();
			literalTypeRightIsNull.setLeftExpression(expr2);
			
			Expression isNullCheck =  FilterUtil.conjunct(Arrays.asList((Expression)literalTypeLeftIsNull,(Expression)literalTypeRightIsNull));

			
			
			WhenClause bothNull = new WhenClause();
			bothNull.setWhenExpression(isNullCheck);
			bothNull.setThenExpression(dth.cast(new StringValue("'true'"),dth.getBooleanCastType()));
			
			
			CaseExpression caseExpr = new CaseExpression();
			caseExpr.setWhenClauses(Arrays.asList(((Expression)bothNull)));
			
			caseExpr.setElseExpression(function);
			
			
			return caseExpr;
		}
		
		
		@Override
		public void visit(NodeValue nv) {		
			tms.push(tmf.createTermMap(nv.asNode()));
		}
		
		
		@Override
		public void visit(ExprVar nv) {
			tms.push( var2termMap.get( nv.asVar().getName()));

		}
		
		
		
	}
	
	
	
//	public  Expression getSQLExpression(Expr exp,
//			BiMap<String, String> colstring2var,
//			Map<String, TermMap> colstring2col) {
//
//		Expression sqlExpressions = null;
//		if (exp instanceof E_GreaterThan) {
//			E_GreaterThan sparqlGt = (E_GreaterThan) exp;
//			GreaterThan gt = new GreaterThan();
//			gt.setLeftExpression(getSQLExpression(sparqlGt.getArg1(),
//					colstring2var, colstring2col));
//			gt.setRightExpression(getSQLExpression(sparqlGt.getArg2(),
//					colstring2var, colstring2col));
//			sqlExpressions = gt;
//
//		} else if (exp instanceof E_LessThan) {
//			E_LessThan sparqlLt = (E_LessThan) exp;
//			MinorThan mt = new MinorThan();
//			mt.setLeftExpression(getSQLExpression(sparqlLt.getArg1(),
//					colstring2var, colstring2col));
//			mt.setRightExpression(getSQLExpression(sparqlLt.getArg2(),
//					colstring2var, colstring2col));
//			sqlExpressions = mt;
//		} else	if (exp instanceof E_LangMatches) {
//			//check against the lang 
//			
//			E_LangMatches lm = (E_LangMatches) exp;
//			
//			
//			String lang = ((NodeValueString)lm.getArg2()).asUnquotedString();
//			
//			
//			Function toLower = new Function();
//			toLower.setName("lower");
//			Expression lang2 = getSQLExpression(lm.getArg1(),colstring2var, colstring2col);
//			toLower.setParameters(new ExpressionList(Arrays.asList(lang2)));
//			
//			EqualsTo eq = new EqualsTo();
//			
//			eq.setLeftExpression(toLower);
//			eq.setRightExpression(new StringValue("\"" + lang.toLowerCase() + "\""));
//			sqlExpressions = eq;
//			
//			
//			
//			
//		} else if (exp instanceof E_Lang) {
//			//check against the lang 
//			
//			E_Lang lang = (E_Lang) exp;
//			
//			Expr arg =  lang.getArg();
//			
//			if(arg instanceof ExprVar){
//				Var var = ((ExprVar) arg).asVar();
//				//resolve var directly to take the 
//				TermMap tc = colstring2col.get(colstring2var.inverse().get(var.getName()));
//				
//				sqlExpressions = ColumnHelper.getLanguage(tc.getExpressions());
//				
//				
//				// in the odd case, lang() is applied on a literal, do this
//			}else if(arg instanceof NodeValueNode ){
//				sqlExpressions = new StringValue(((NodeValueNode)arg).asNode().getLiteralLanguage());
//			}else{
//				throw new ImplementationException("Should not happen");
//			}
//			
//
//		} else if (exp instanceof E_Bound) {
//			E_Bound bound = (E_Bound) exp;
//			Expression tobebound = getSQLExpression(bound.getArg(), colstring2var, colstring2col);
//			
//			IsNullExpression isnull  = new IsNullExpression();
//			isnull.setNot(true);
//			isnull.setLeftExpression(tobebound);
//			
//			sqlExpressions = isnull;
//		} else if (exp instanceof E_LogicalNot) {
//			E_LogicalNot not = (E_LogicalNot) exp;
//			Expression expr = getSQLExpression(not.getArg(), colstring2var, colstring2col);
//			Expression negatedExpr = null;
//			
//
//			//using reflection, as there are no interfaces to do the job
//			
//			for (Method method : (expr.getClass().getDeclaredMethods())) {
//				
//				
//				
//				
//				if(method.getName().equals("setNot")){
//					try {
//						
//						//get the old value
//						
//						
//						
//						boolean oldValue = (Boolean) MethodUtils.invokeExactMethod(expr, "isNot", null);
//						
//						method.invoke(expr, new Boolean((!oldValue)));
//					} catch (IllegalArgumentException e) {
//						// TODO Auto-generated catch block
//						log.error("Error:",e);
//					} catch (IllegalAccessException e) {
//						// TODO Auto-generated catch block
//						log.error("Error:",e);
//					} catch (InvocationTargetException e) {
//						// TODO Auto-generated catch block
//						log.error("Error:",e);
//					} catch (NoSuchMethodException e) {
//						// TODO Auto-generated catch block
//						log.error("Error:",e);
//					}
//					negatedExpr = expr;
//				}
//			}
//			
//			if(negatedExpr !=null){
//				sqlExpressions = negatedExpr;
//			}else{
//				log.error("Unable to negate expr" + exp.toString() + " nevertheless continuing");
//				sqlExpressions = expr;
//			}
//			
//			
//		} else if (exp instanceof E_Regex) {
//			E_Regex regex = (E_Regex) exp;
//
//			LikeExpression like = new LikeExpression();
//
//			Expression leftExp = null;
//			String var = regex.getArg(1).toString().substring(1);
//
//			leftExp = FilterUtil.concat(colstring2col.get(colstring2var.inverse().get(var))
//					.getExpressions().toArray(new Expression[0]));
//
//			like.setLeftExpression(leftExp);
//			like.setRightExpression(new StringValue("'%"
//					+ regex.getArg(2)
//							.toString()
//							.substring(1,
//									regex.getArg(2).toString().length() - 1)
//					+ "%'"));
//
//			sqlExpressions = like;
//		} else if (exp instanceof E_NotEquals) {
//			E_NotEquals ne = (E_NotEquals) exp;
//
//			NotEqualsTo sqlNe = new NotEqualsTo();
//			sqlNe.setLeftExpression(getSQLExpression(ne.getArg1(),
//					colstring2var, colstring2col));
//			sqlNe.setRightExpression(getSQLExpression(ne.getArg2(),
//					colstring2var, colstring2col));
//			sqlExpressions = sqlNe;
//
//		} else if (exp instanceof E_LogicalAnd) {
//			E_LogicalAnd and = (E_LogicalAnd) exp;
//
//			Expression left = getSQLExpression(and.getArg1(), colstring2var,
//					colstring2col);
//			Expression right = getSQLExpression(and.getArg2(), colstring2var,
//					colstring2col);
//
//			sqlExpressions = new AndExpression(left, right);
//
//		} else if (exp instanceof E_LogicalOr) {
//
//			E_LogicalOr or = (E_LogicalOr) exp;
//			Expression left = getSQLExpression(or.getArg1(), colstring2var,
//					colstring2col);
//			Expression right = getSQLExpression(or.getArg2(), colstring2var,
//					colstring2col);
//			sqlExpressions = new Parenthesis(new OrExpression(left, right));
//
//		} else if (exp instanceof E_Add) {
//
//			E_Add add = (E_Add) exp;
//			Expression left = getSQLExpression(add.getArg1(), colstring2var,
//					colstring2col);
//			Expression right = getSQLExpression(add.getArg2(), colstring2var,
//					colstring2col);
//
//			Addition sqlAdd = new Addition();
//			sqlAdd.setLeftExpression(left);
//			sqlAdd.setRightExpression(right);
//			sqlExpressions = sqlAdd;
//
//		} else if (exp instanceof E_Subtract) {
//
//			E_Subtract sub = (E_Subtract) exp;
//			Expression left = getSQLExpression(sub.getArg1(), colstring2var,
//					colstring2col);
//			Expression right = getSQLExpression(sub.getArg2(), colstring2var,
//					colstring2col);
//
//			Subtraction sqlSub = new Subtraction();
//			sqlSub.setLeftExpression(left);
//			sqlSub.setRightExpression(right);
//			sqlExpressions = sqlSub;
//
//		} else if (exp instanceof E_Equals || exp instanceof E_SameTerm) {
//
//			ExprFunction2 eq = (ExprFunction2) exp;
//			Expression left = getSQLExpression(eq.getArg1(), colstring2var,
//					colstring2col);
//			Expression right = getSQLExpression(eq.getArg2(), colstring2var,
//					colstring2col);
//
//			EqualsTo sqlEq = new EqualsTo();
//			sqlEq.setLeftExpression(left);
//			sqlEq.setRightExpression(right);
//			sqlExpressions = sqlEq;
//
//		} else if (exp instanceof E_LessThanOrEqual) {
//
//			E_LessThanOrEqual leq = (E_LessThanOrEqual) exp;
//			Expression left = getSQLExpression(leq.getArg1(), colstring2var,
//					colstring2col);
//			Expression right = getSQLExpression(leq.getArg2(), colstring2var,
//					colstring2col);
//
//			MinorThanEquals mt = new MinorThanEquals();
//
//			mt.setLeftExpression(left);
//			mt.setRightExpression(right);
//			sqlExpressions = mt;
//
//		} else if (exp instanceof E_Function) {
//			E_Function func = (E_Function) exp;
//			if (func.getFunctionIRI().equals(XSD.xdouble.toString())) {
//
//				sqlExpressions = dth.cast(
//						getSQLExpression(func.getArg(1), colstring2var,
//								colstring2col), dth.getNumericCastType());
//			} else {
//				throw new ImplementationException("E_Function for "
//						+ func.getFunctionIRI() + " not yet implemented");
//			}
//
//		} else if (exp instanceof E_Str) {
//			E_Str str = (E_Str) exp;
//
//			sqlExpressions = dth.cast(
//					getSQLExpression(str.getArg(), colstring2var,
//							colstring2col), dth.getStringCastType());
//		} else if (exp instanceof E_IsURI) { 
//			E_IsURI isUri = (E_IsURI) exp;
//			
//			Expression tocheck = getSQLExpression(isUri.getArg(), colstring2var, colstring2col);
//			
//			tocheck.toString();
//			
//			
//			
//			
//		} else if (exp.isVariable()) {
//
//			TermMap tc = colstring2col.get(colstring2var.inverse().get(
//					exp.getVarName()));
//			if(tc !=null){
//				sqlExpressions = FilterUtil.concat(tc.getExpressions().toArray(new Expression[0]));
//			}else{
//				sqlExpressions = new NullValue();
//			}
//			
//
//		} else if (exp instanceof NodeValueNode
//				&& ((NodeValueNode) exp).getNode() instanceof Node_URI) {
//
//			Node_URI nodeU = (Node_URI) ((NodeValueNode) exp).getNode();
//			
//			sqlExpressions = new StringValue("'" + nodeU.getURI() + "'");
//
//		} else if (exp instanceof NodeValueDT) {
//			NodeValueDT nvdt = (NodeValueDT) exp;
//
//			Timestamp ts = new Timestamp(nvdt.getDateTime().toGregorianCalendar().getTime().getTime());
//			sqlExpressions = new TimestampValue("'" + ts.toString() + "'");
//
//		} else if (exp instanceof NodeValueInteger) {
//			sqlExpressions = new LongValue(((NodeValueInteger) exp).getInteger()
//					.toString());
//
//		} else if (exp instanceof NodeValueDouble) {
//			sqlExpressions = new LongValue(String.valueOf(((NodeValueDouble) exp)
//					.getDouble()));
//		} else if (exp instanceof NodeValueString) {
//			sqlExpressions = new StringValue(
//					((NodeValueString) exp).asQuotedString());
//		} else if (exp instanceof NodeValueDouble) {
//			sqlExpressions = new LongValue(String.valueOf(((NodeValueDouble) exp)
//					.getDouble()));
//
//		} else {
//
//			throw new ImplementationException("Filter " + exp.toString()
//					+ " not yet implemented");
//		}
//		//sqlExpression = checkCompatibility(sqlExpression);
//
//		sqlExpressions = fopt.shortCut(sqlExpressions);
//
//		return sqlExpressions;
//	}
//	
	
	
	


}
