package org.aksw.sparqlmap.mapper.subquerymapper.algebra;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.CastStringExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
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
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.OrderByExpressionElement;

import org.aksw.sparqlmap.config.syntax.r2rml.R2RMLModel;
import org.aksw.sparqlmap.config.syntax.r2rml.TermMap;
import org.apache.commons.lang.reflect.MethodUtils;

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
import com.hp.hpl.jena.sparql.expr.E_Str;
import com.hp.hpl.jena.sparql.expr.E_Subtract;
import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.expr.ExprFunction;
import com.hp.hpl.jena.sparql.expr.ExprVar;
import com.hp.hpl.jena.sparql.expr.nodevalue.NodeValueDT;
import com.hp.hpl.jena.sparql.expr.nodevalue.NodeValueDouble;
import com.hp.hpl.jena.sparql.expr.nodevalue.NodeValueInteger;
import com.hp.hpl.jena.sparql.expr.nodevalue.NodeValueNode;
import com.hp.hpl.jena.sparql.expr.nodevalue.NodeValueString;
import com.hp.hpl.jena.vocabulary.XSD;

public class FilterUtil {

	static org.slf4j.Logger log = org.slf4j.LoggerFactory
			.getLogger(FilterUtil.class);

	private DataTypeHelper dth;

	private R2RMLModel r2rmodel;

	// Op query;

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

	public FilterUtil(DataTypeHelper dth, R2RMLModel r2rmodel) {
		this.r2rmodel = r2rmodel;
		this.dth = dth;
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
			toLower.setParameters(new ExpressionList(Arrays.asList(getSQLExpression(lm.getArg1(),colstring2var, colstring2col))));
			
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
			sqlExpression = new OrExpression(left, right);

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

		} else if (exp instanceof E_Equals) {

			E_Equals eq = (E_Equals) exp;
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

				sqlExpression = cast(
						getBestExpression(func.getArg(1), colstring2var,
								colstring2col), dth.getNumericCastType());
			} else {
				throw new ImplementationException("E_Function for "
						+ func.getFunctionIRI() + " not yet implemented");
			}

		} else if (exp instanceof E_Str) {
			E_Str str = (E_Str) exp;

			sqlExpression = cast(
					getBestExpression(str.getArg(), colstring2var,
							colstring2col), dth.getStringCastType());
		} else {

			throw new ImplementationException("Filter " + exp.toString()
					+ " not yet implemented");
		}
		sqlExpression = checkCompatibility(sqlExpression);

		sqlExpression = shortCut(sqlExpression);

		return sqlExpression;
	}

	public static Expression shortCut(Expression sqlExpression) {

		// handle the 'uri' == concat('uri', column) case
		if (sqlExpression instanceof EqualsTo
				|| sqlExpression instanceof NotEqualsTo) {
			BinaryExpression bex = (BinaryExpression) sqlExpression;
			Function func = null;
			StringValue stringOrig = null;
			List<Expression> stringNew = new ArrayList<Expression>();
			List<Expression> compareto = new ArrayList<Expression>();
			boolean reduced = false;

			if (bex.getLeftExpression() instanceof Function) {
				func = (Function) bex.getLeftExpression();
			}
			if (bex.getLeftExpression() instanceof StringValue) {
				stringOrig = (StringValue) bex.getLeftExpression();
			}
			if (bex.getRightExpression() instanceof Function) {
				func = (Function) bex.getRightExpression();
			}
			if (bex.getRightExpression() instanceof StringValue) {
				stringOrig = (StringValue) bex.getRightExpression();
			}
			if (func != null && stringOrig != null) {
				if (func.getName().toLowerCase().equals("concat")) {
					String toReduce = stringOrig.getNotExcapedValue();
					List<Expression> parameter = new ArrayList<Expression>();
					for(Object oexp: func.getParameters()
							.getExpressions()){
						parameter.add(FilterUtil.uncast((Expression) oexp));
					}
						
					int psize = parameter.size();
					
					for (int i = 0; i < psize; i += 2) {
						if (parameter.get(i) instanceof StringValue) {

							String value = ((StringValue) parameter.get(i))
									.getNotExcapedValue();
							Column col = null;
							String prereadsuffix = null;
							if ((i + 1 < psize)
									&& parameter.get(i + 1) instanceof Column) {
								col = (Column) parameter.get(i + 1);
							}
							if ((i + 2 < psize)
									&& parameter.get(i + 2) instanceof StringValue) {
								prereadsuffix = ((StringValue) parameter
										.get(i + 2)).getNotExcapedValue();
							}

							if (toReduce.startsWith(value)) {
								// delete this part
								toReduce = toReduce.substring(value.length());
								reduced = true;
							}else{
								break;
							}

							if (col != null && prereadsuffix != null &&
							// is the static value at the end?
							 
									toReduce.contains(prereadsuffix) ) {
								// no, there is a static part following
								String colvalue = toReduce.substring(0,
										toReduce.indexOf(prereadsuffix));
								toReduce = toReduce.substring(toReduce
										.indexOf(prereadsuffix));
								stringNew.add(new StringValue("\"" + colvalue
										+ "\""));
								compareto.add(col);
								reduced = true;
							} else if (col != null) {
								// yes, we are finished
								stringNew.add(new StringValue("\"" + toReduce
										+ "\""));
								compareto.add(col);
								reduced = true;
							}
						} else {
							throw new ImplementationException(
									"Should never come here, resource constructiosn should always be String + col + string + col ...., but is: "
											+ func);
						}
					}
				}
			}

			if (reduced) {

				if (stringNew.size() > 0 && compareto.size() > 0) {
					
					//if(bex instanceof EqualsTo){
						
					sqlExpression = FilterUtil.createEqualsTo(
							new ArrayList<Expression>(stringNew),
							new ArrayList<Expression>(compareto));
//					} else if (bex instanceof NotEqualsTo){
//						sqlExpression = FilterUtil.createNotEqualsTo(
//								new ArrayList<Expression>(stringNew),
//								new ArrayList<Expression>(compareto));
//					}
				} else {
					
					
//					if(bex instanceof EqualsTo){
						EqualsTo eq = new EqualsTo();
						eq.setLeftExpression(new StringValue("\"true\""));
						eq.setRightExpression(new StringValue("\"true\""));

						sqlExpression = eq;
						
//						} else if (bex instanceof NotEqualsTo){
//							NotEqualsTo neq = new NotEqualsTo();
//							neq.setLeftExpression(new StringValue("\"true\""));
//							neq.setRightExpression(new StringValue("\"true\""));
//
//							sqlExpression = neq;
//						}

					// fallback, if everything was eleminated

					
				}

			}
			
			if(bex instanceof NotEqualsTo){
				
				sqlExpression = new Parenthesis(sqlExpression);
				((Parenthesis)sqlExpression).setNot();
				
			}

		}
		
		
		

		// TODO Auto-generated method stub
		return sqlExpression;
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

	private Expression getBestExpression(Expr expr,
			BiMap<String, String> colstring2var,
			Map<String, TermMap> colstring2col) {
		Expression expression = null;

		if (expr.isVariable()) {

			TermMap tc = colstring2col.get(colstring2var.inverse().get(
					expr.getVarName()));

			expression = tc.getExpression();

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
	
	public static String CONCAT = "CONCAT";

	public static Function concat(Expression... expr) {
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

	public static Column createColumn(String table, String column) {
		return createColumn(null, table, column);
	}

	public static Column createColumn(String schema, String table, String column) {
		Column col = new Column();
		col.setColumnName(column);
		Table tab = new Table();
		tab.setName(table);
		tab.setAlias(table);
		if (schema != null) {
			tab.setSchemaName(schema);

		}
		col.setTable(tab);

		return col;

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

	/**
	 * if the expressions expr is a cast, the cast expression is returned,
	 * otherwise the expr parameter is returned
	 * 
	 * @param expr
	 */
	public static Expression uncast(Expression expr) {

		if (expr instanceof Function) {
			if (((Function) expr).getName().toLowerCase().equals("cast")
					&& ((Function) expr).getParameters().getExpressions()
							.get(0) instanceof CastStringExpression) {
				CastStringExpression cass = (CastStringExpression) ((Function) expr)
						.getParameters().getExpressions().get(0);
				expr = cass.getExpr();
			}
		}

		return expr;
	}

	public static String getCastType(Expression expr) {

		String type = null;
		if (expr instanceof Function) {
			if (((Function) expr).getName().toLowerCase().equals("cast")
					&& ((Function) expr).getParameters().getExpressions()
							.get(0) instanceof CastStringExpression) {
				CastStringExpression cass = (CastStringExpression) ((Function) expr)
						.getParameters().getExpressions().get(0);
				type = cass.getCastto();
			}
		}
		return type;
	}

	public static Expression cast(Expression expr, String castTo) {
		if(castTo == null){
			return expr;
		}
		
		Function cast = new Function();
		cast.setName("CAST");
		ExpressionList exprlist = new ExpressionList();
		exprlist.setExpressions(Arrays.asList(new CastStringExpression(expr,
				castTo)));
		cast.setParameters(exprlist);
		return cast;
	}

	public static Expression cast(String table, String col, String castTo) {
		Function cast = new Function();
		cast.setName("CAST");
		ExpressionList exprlist = new ExpressionList();
		exprlist.setExpressions(Arrays.asList(new CastStringExpression(table,
				col, castTo)));
		cast.setParameters(exprlist);
		return cast;
	}

	public Expression castNull(String castTo) {
		Function cast = new Function();
		cast.setName("CAST");
		ExpressionList exprlist = new ExpressionList();
		exprlist.setExpressions(Arrays.asList(new CastStringExpression(castTo)));
		cast.setParameters(exprlist);
		return cast;
	}
	
	
	private String getDataType(Expression expr){
		
		log.warn("Called getDataType. Refactor to not use direct col access");
		
		
		if(expr instanceof Column){
			String colname = ((Column) expr).getColumnName();
			String tablename = ((Column) expr).getTable().getName();
			
			//only shorten, if the table is not from a subselect
			if(!tablename.contains("subsel_")){
//				if(tablename.contains("_dupVar_")){
//					tablename=tablename.substring(0,tablename.lastIndexOf("_dupVar_"));
//				}
//				//remove the variable part
//				tablename = tablename.substring(0,tablename.lastIndexOf("_"));
				
				return dth.getCastTypeString(r2rmodel.getSqlDataType(tablename, colname));
			}
			
		}
		if(expr instanceof StringValue){
			
			Scanner scanner = new Scanner(((StringValue) expr).getValue());

			if(scanner.hasNextBigDecimal()){
				return dth.getNumericCastType();
			}
			
			
			return dth.getStringCastType();
		}

		
		if(expr instanceof CastStringExpression){
			return ((CastStringExpression) expr).getCastto();
		}
		
		return null;
		
	}
	
	
	public Expression shortCutFilter(Expression expr){
		if (expr instanceof EqualsTo) {
			EqualsTo eq = (EqualsTo) expr;
			Expression left = eq.getLeftExpression();
			Expression right = eq.getRightExpression();
			
			if(getDataType(uncast(left)).equals(getDataType(uncast(right)))){
				expr = new EqualsTo();
				((EqualsTo) expr).setLeftExpression(uncast(left));
				((EqualsTo) expr).setRightExpression(uncast(right));
			}
			
		}
		
		
		return expr;
				
	}
	
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
		
		FromItem fi1 = ((Column)FilterUtil.uncast(equalsTo.getLeftExpression())).getTable();
		FromItem fi2 = ((Column)FilterUtil.uncast(equalsTo.getRightExpression())).getTable();
		
		if(fi.toString().equals(fi1.toString())){
			return fi2;
		}else if (fi.toString().equals(fi2.toString())){
			return fi1;
		}else{
			throw new ImplementationException("not a correct join condition ");
		}
		
	}
	
	
	

}
