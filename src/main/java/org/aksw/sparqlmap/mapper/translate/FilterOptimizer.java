package org.aksw.sparqlmap.mapper.translate;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.PostConstruct;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;

import org.openjena.atlas.logging.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

@Component
public class FilterOptimizer {
	
	boolean shortcutFilters = true;
	
	@Autowired
	DataTypeHelper dtutil;
	
	@Autowired
	Environment env;
	
	@PostConstruct
	public void setOptimize(){
		shortcutFilters = new Boolean(env.getProperty("sm.opt.shortcutfilters"));
	}
	
	private Logger log = LoggerFactory.getLogger(FilterOptimizer.class);
	
	


	public Expression shortCutFilter(Expression expr){
		if (expr instanceof EqualsTo) {
			EqualsTo eq = (EqualsTo) expr;
			Expression left = eq.getLeftExpression();
			Expression right = eq.getRightExpression();
			
			if(dtutil.getDataType(DataTypeHelper.uncast(left)).equals(dtutil.getDataType(DataTypeHelper.uncast(right)))){
				expr = new EqualsTo();
				((EqualsTo) expr).setLeftExpression(DataTypeHelper.uncast(left));
				((EqualsTo) expr).setRightExpression(DataTypeHelper.uncast(right));
			}
			
		}
		return expr;
				
	}
	
	
	public Expression shortCut(Expression sqlExpression) {
		if(!this.shortcutFilters){
			return sqlExpression;
		}
		
		// handle the cast("y" as x) = cast ("z" as v) statements
		
		if (sqlExpression instanceof EqualsTo){
			EqualsTo eqto = (EqualsTo) sqlExpression;
			
		}
		
		

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
						parameter.add(DataTypeHelper.uncast((Expression) oexp));
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
							
							
							log.debug("No Filtershortcutting for " + sqlExpression);
							return sqlExpression;
//							throw new ImplementationException(
//									"Should never come here, resource columns should always be string + col + string + col ...., but is: "
//											+ func);
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

}
