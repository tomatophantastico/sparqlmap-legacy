package org.aksw.sparqlmap.core.mapper.translate;

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

import org.aksw.sparqlmap.core.config.syntax.r2rml.ColumnHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

@Component
public class OptimizationConfiguration {
	
	boolean shortcutFilters = true;
	boolean optimizeSelfJoin = true;
	boolean optimizeSelfLeftJoin = true;
	boolean optimizeSelfUnion = true;
	boolean optimizeProjectPush = true;


	@Autowired
	private DataTypeHelper dtutil;
	
	@Autowired
	private Environment env;
	
	@PostConstruct
	public void setOptimize(){
		shortcutFilters = new Boolean(env.getProperty("sm.opt.shortcutfilter"));
		log.info("Filter shortcutting is: " + (shortcutFilters?"on":"off"));
		optimizeSelfJoin = new Boolean(env.getProperty("sm.opt.optimizeSelfJoin"));
		log.info("Selfjoinopt is: " + (optimizeSelfJoin?"on":"off"));
		optimizeSelfUnion = new Boolean(env.getProperty("sm.opt.optimizeSelfUnion"));
		log.info("Self Union  is: " + (optimizeSelfUnion?"on":"off"));
		optimizeSelfLeftJoin = new Boolean(env.getProperty("sm.opt.optimizeSelfLeftJoin"));
		log.info("Self left join Opt is: " + (optimizeSelfLeftJoin?"on":"off"));
		optimizeProjectPush = new Boolean(env.getProperty("sm.opt.optimizeProjectPush"));
		log.info("Project pushing is is: " + (optimizeProjectPush?"on":"off"));
	}
	
	private Logger log = LoggerFactory.getLogger(OptimizationConfiguration.class);
	
	



	
	
	public Expression _shortCut(Expression sqlExpression) {
		if(!this.shortcutFilters){
			return sqlExpression;
		}
		
		
		// if it ""
		if (sqlExpression instanceof EqualsTo){
			EqualsTo eq = (EqualsTo) sqlExpression;
			boolean sameTerm = DataTypeHelper.uncast(eq.getLeftExpression()).equals(DataTypeHelper.uncast(eq.getRightExpression()));
			String castType1 = DataTypeHelper.getCastType(eq.getLeftExpression());
			String castType2 = DataTypeHelper.getCastType(eq.getRightExpression());
			boolean sameType = false;
			if(castType1==null&&castType2==null){
				sameType = true;
			}else if(castType1!=null&&castType2!=null&&castType1.equals(castType2)){
				sameType = true;
			}
			
			
			if(sameTerm && sameType){
				return null;
			}
		}
		
		
		// handle the cast("y" as x) = cast ("z" as v) statements
		
		if (sqlExpression instanceof BinaryExpression){
			BinaryExpression be = (BinaryExpression) sqlExpression;
			Expression left = DataTypeHelper.uncast(be.getLeftExpression());
			Expression right = DataTypeHelper.uncast(be.getRightExpression());
			
			be.setLeftExpression(left);
			be.setRightExpression(right);
		}
		
		
//		public Expression shortCutFilter(Expression expr){
//		if (expr instanceof EqualsTo) {
//			EqualsTo eq = (EqualsTo) expr;
//			Expression left = eq.getLeftExpression();
//			Expression right = eq.getRightExpression();
//			
//			if(dtutil.getDataType(DataTypeHelper.uncast(left)).equals(dtutil.getDataType(DataTypeHelper.uncast(right)))){
//				expr = new EqualsTo();
//				((EqualsTo) expr).setLeftExpression(DataTypeHelper.uncast(left));
//				((EqualsTo) expr).setRightExpression(DataTypeHelper.uncast(right));
//			}
//			
//		}
//		return expr;
//				
//	}		
		
		

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
					reduced = _shortcutConcatExpressionStringBased(
							sqlExpression, func, stringOrig, stringNew,
							compareto);
				}
			}

			if (reduced) {

				if (stringNew.size() > 0 && compareto.size() > 0) {
					
					//if(bex instanceof EqualsTo){
						
//					sqlExpression = FilterUtil.createEqualsTo(
//							new ArrayList<Expression>(stringNew),
//							new ArrayList<Expression>(compareto));
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
	

	private boolean _shortcutConcatExpressionStringBased(
			Expression sqlExpression, Function func, StringValue stringOrig,
			List<Expression> stringNew, List<Expression> compareto ) {
		boolean reduced = false;
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
				//							throw new ImplementationException(
//									"Should never come here, resource columns should always be string + col + string + col ...., but is: "
//											+ func);
			}
		}
		return reduced;
	}
	
	public boolean isOptimizeSelfJoin() {
		return optimizeSelfJoin;
	}
	public boolean isOptimizeSelfLeftJoin() {
		return optimizeSelfLeftJoin;
	}
	public boolean isOptimizeSelfUnion() {
		return optimizeSelfUnion;
	}
	public boolean isShortcutFilters() {
		return shortcutFilters;
	}


	public boolean isOptimizeProjectPush() {
		return optimizeProjectPush;
	}

}
