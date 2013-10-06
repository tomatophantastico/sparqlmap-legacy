package org.aksw.sparqlmap.core.mapper.translate;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnalyticExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.ExpressionWithString;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.InverseExpression;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringExpression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseAnd;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseOr;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseXor;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Modulo;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.Matches;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.Distinct;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.util.BaseSelectVisitor;

import org.aksw.sparqlmap.core.config.syntax.r2rml.ColumnHelper;
import org.aksw.sparqlmap.core.config.syntax.r2rml.TermMap;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import com.hp.hpl.jena.sparql.expr.E_Equals;
import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.expr.nodevalue.NodeValueNode;

public class PlainSelectWrapper implements Wrapper {

	public static int lj_count = 0;

	static org.slf4j.Logger log = org.slf4j.LoggerFactory
			.getLogger(PlainSelectWrapper.class);
	public static int subsel_count = 0;
	
	
	

	private Multimap<String, EqualsTo> _fromItem2joincondition = LinkedListMultimap
			.create();

	/**
	 * simply adds the from item to the query. no join on conditions given
	 * 
	 * @param fi
	 */
	private Map<String, FromItem> _fromItems = new LinkedHashMap<String, FromItem>();

	private Multimap<String, EqualsTo> _optFromItem2joincondition = LinkedListMultimap
			.create();

	private Map<String, FromItem> _optFromItems = new LinkedHashMap<String, FromItem>();

	private Map<String, TermMap> colstring2TermMap = new HashMap<String, TermMap>();

	// private Multimap<String, Mapping> ldpMappings = HashMultimap.create();

	private BiMap<String, String> colstring2var = HashBiMap.create();

	private TermMap crc;

	private DataTypeHelper dth;

	private int dupcounter = 0;

	ExpressionConverter exprconv;

	private Map<String, Expression> filters = new HashMap<String, Expression>();

	private FilterOptimizer fopt;

	private final PlainSelect plainSelect = new PlainSelect();

	private Map<SubSelect, Wrapper> subselects = new HashMap<SubSelect, Wrapper>();
	

	

	private Map<String, String> varEqualsUriMap = new HashMap<String, String>();

	public PlainSelectWrapper(Map<SelectBody, Wrapper> registerTo,
			DataTypeHelper dth, ExpressionConverter exprconv,
			FilterOptimizer fopt) {
		super();
		this.exprconv = exprconv;
		this.dth = dth;
		this.fopt = fopt;
		plainSelect.setSelectItems(new ArrayList<SelectItem>());
		plainSelect.setJoins(new ArrayList());
		registerTo.put(plainSelect, this);
	}

	private void addColumn(TermMap tc, String tcAlias, boolean isOptional) {
		TermMap term = tc;

		// we check if the column is already in use.
		// if the column then is used to be bound to another variable
		// then we have to duplicate it an join it to again to the query
		if (colstring2var.containsKey(term.toString())) {
			String var = colstring2var.get(term.toString());
			if (!var.equals(tcAlias)) {
				term = cloneColOnDuplicateUsage(term, tcAlias);
			} else {

				// col already in there, no need to do anything
				return;
			}
		}

		// check if the variable is already bound to a column.
		// if so, the variable does not need to be added to be selected, but
		// just be
		// an equals statement is required.
		if (colstring2var.inverse().containsKey(tcAlias)) {
			// the column is already in there, so just add an equals expression
			// as this only happends, when a join is created, we add this to the
			// join conditions
			String inThereColString = colstring2var.inverse().get(tcAlias);
			if (!colstring2TermMap.get(inThereColString).equals(term)) {

				boolean alreadyInJoinCond = false;

				// for (Expression oldeq : joincondition.get(column.getMapp()
				// .getFromPart())) {
				// if
				// (oldeq.getRightExpression().toString().equals(column.getColumnExpression().toString())
				// && oldeq.getLeftExpression().toString().equals(
				// colstring2Col.get(
				// colstring2var.inverse().get(
				// colalias)).getColumnExpression().toString())) {
				// alreadyInJoinCond = true;
				// }
				// }

				if (!alreadyInJoinCond) {
					TermMap oldTc = colstring2TermMap.get(colstring2var
							.inverse().get(tcAlias));
					List<EqualsTo> eqs = FilterUtil.createEqualsTos(
							oldTc.getExpressions(), term.getExpressions());

					for (int i = 0; i < eqs.size(); i++) {
						addJoinCondition(eqs.get(i));
						// fromItem2joincondition.put(((Column)DataTypeHelper.uncast(eqs.get(i).getLeftExpression())).getTable(),
						// eqs.get(i));
						// fromItem2joincondition.put(((Column)DataTypeHelper.uncast(eqs.get(i).getRightExpression())).getTable(),
						// eqs.get(i));
						// fromItem2joincondition.put(term.getFromItems().get(i),
						// eqs.get(i));
					}

				}

			}

		} else {
			colstring2var.put(term.toString(), tcAlias);
			colstring2TermMap.put(term.toString(), term);
			// adds the type information
			// add the resource and literal columns
			plainSelect.getSelectItems().addAll(
					term.getSelectExpressionItems(tcAlias));

			if (!isOptional) {
				// create the not null condition in the filters
				createNotNull(term, tcAlias);
			}

		}

		// create the select items

		// we create for each variable a bunch of columns

		// create the string representation. In the case of an litereal, this is
		// just the cast to an char
		// in case of an resource, we create the full uri by casting it to
		// string, and prefixing it

		// take care of the from part
		addMappingstoQuery(term);
	}

	public void addFilterExpression(Collection<Expr> exprs) {

		for (Expr expr : exprs) {
			boolean dupe = false;
			if (expr instanceof E_Equals
					&& ((E_Equals) expr).getArg1().isVariable()
					&& ((E_Equals) expr).getArg2() instanceof NodeValueNode) {
				String varname = ((E_Equals) expr).getArg1().getVarName();
				String constValue = ((NodeValueNode) ((E_Equals) expr)
						.getArg2()).toString();

				if (varEqualsUriMap.containsKey(varname)
						&& varEqualsUriMap.get(varname).equals(constValue)) {
					dupe = true;
				} else {
					varEqualsUriMap.put(varname, constValue);
				}
			}

			if (!dupe) {

				Expression sqlEx = exprconv.getSQLExpression(expr,
						colstring2var, colstring2TermMap);
				if (sqlEx != null) {
					addSQLFilter(sqlEx);
				} else {
					log.warn("Unmappable filter condition encountered: "
							+ expr.toString());
				}

			}
		}
	}

	private void addFromItem(FromItem fi) {
		if (!_fromItems.containsKey(fi.getAlias())) {
			_fromItems.put(fi.getAlias(), fi);
		}
		putFromItems();
	}

	private void addJoinCondition(EqualsTo eq) {
		addJoinCondition(eq.getLeftExpression(), eq.getRightExpression(), false);
	}

	private void addJoinCondition(Expression exp1, Expression exp2) {
		addJoinCondition(exp1, exp2, false);
	}

	private void addJoinCondition(Expression exp1, Expression exp2,
			boolean isOptional) {
		// we create here an equals join condition out of two expression, of one
		// or both columns are and the rest are fixed values.
		FromItem fi1 = null;
		FromItem fi2 = null;

		// ((Column)DataTypeHelper.uncast(exp1)).getTable();
		// ((Column)DataTypeHelper.uncast(exp2)).getTable();

		if (DataTypeHelper.uncast(exp1) instanceof Column) {
			fi1 = ((Column) DataTypeHelper.uncast(exp1)).getTable();
			if (DataTypeHelper.uncast(exp2) instanceof Column) {
				fi2 = ((Column) DataTypeHelper.uncast(exp2)).getTable();
			}
		} else if (DataTypeHelper.uncast(exp2) instanceof Column) {
			fi1 = ((Column) DataTypeHelper.uncast(exp2)).getTable();
		}

		EqualsTo eq = new EqualsTo();
		eq.setLeftExpression(exp1);
		eq.setRightExpression(exp2);

		// construct the inverse
		EqualsTo eqinv = new EqualsTo();
		eqinv.setLeftExpression(exp2);
		eqinv.setRightExpression(exp1);

		// we only need to check for one From ITem here, as we always put the
		// data in for both. So if it is in there for one, so it is in for both
		Collection<EqualsTo> otherEqs = new ArrayList<EqualsTo>();
		if (isOptional && fi1 != null) {
			otherEqs = _fromItem2joincondition.get(fi1.getAlias());
		} else if (fi1 != null) {
			otherEqs = _optFromItem2joincondition.get(fi1.getAlias());
		}

		boolean isAlreadyInThere = false;
		// we now check, if it is already in there
		for (EqualsTo otherEq : otherEqs) {
			if (otherEq.toString().equals(eq.toString())
					&& otherEq.toString().equals(eqinv.toString())) {
				isAlreadyInThere = true;
				break;
			}
		}

		if (!isAlreadyInThere) {
			// not in there, add the join condition
			if (isOptional) {
				_optFromItem2joincondition.put(fi1.getAlias(), eq);
				if (fi2 != null) {
					_optFromItem2joincondition.put(fi2.getAlias(), eq);
				}
			} else {
				_fromItem2joincondition.put(fi1.getAlias(), eq);
				if (fi2 != null) {
					_fromItem2joincondition.put(fi2.getAlias(), eq);
				}

			}

		}

	}

	/**
	 * extracts the from item of the expressions and adds them to the select
	 * 
	 * @param expression
	 */
	private void addMappingstoQuery(TermMap tc) {

		for (EqualsTo eq : tc.getFromJoins()) {
			addJoinCondition(eq);
			// fromItem2joincondition.put(((Column)
			// eq.getLeftExpression()).getTable(), eq);
			// fromItem2joincondition.put(((Column)
			// eq.getRightExpression()).getTable(), eq);
		}

		for (FromItem fi : tc.getFromItems()) {
			addFromItem(fi);
		}

	}

	private void addOptFromItem(FromItem fi) {
		if (!_optFromItems.containsKey(fi.getAlias())) {
			_optFromItems.put(fi.getAlias(), fi);
		}
		putFromItems();
	}

	private void addOptJoinCondition(EqualsTo eq) {
		addJoinCondition(eq.getLeftExpression(), eq.getRightExpression(), true);
	}

	private void addOptJoinCondition(Expression exp1, Expression exp2) {
		addJoinCondition(exp1, exp2, true);
	}

	public void addSQLFilter(Expression sqlEx) {
		
		sqlEx = fopt.shortCut(sqlEx);
		
		if (sqlEx!=null && !filters.containsKey(sqlEx.toString())) {
			filters.put(sqlEx.toString(), sqlEx);
			plainSelect
					.setWhere(FilterUtil
							.conjunctFilters(new HashSet<Expression>(filters
									.values())));

		}
	}

	/**
	 * adds a subselect to the plain select. will create the join conditions.
	 * 
	 * @param right
	 * @param optional
	 *            when true, the plain select will be added with a left join
	 */
	public void addSubselect(Wrapper right, boolean optional) {

		// check if the subselect queries only for a single triple and is
		// optional.
		// then we can add it directly to the plain select.
		boolean shortcutted = false;

		if (fopt.optimizeSelfLeftJoin == true && right instanceof PlainSelectWrapper
				&& ((PlainSelectWrapper) right).getVarsMentioned().size() == 3
				&& ((PlainSelectWrapper) right).subselects.size() == 0) {
			PlainSelectWrapper ps = (PlainSelectWrapper) right;

			// we require the not shared variable to be either constant resource
			// or a literal. column generated resources can be troublesome, if
			// generated from more than one column.
			for (String var : ps.getVarsMentioned()) {

				TermMap rightVarTc = ps.getColstring2Col().get(
						ps.getColstring2Var().inverse().get(var));
				// variables already there and equal can be ignored
				// if not equal, we cannot use this optimization
				TermMap thisVarTc = colstring2TermMap.get(this.colstring2var
						.inverse().get(var));
				if (thisVarTc != null) {
					// compare it

					if (thisVarTc.toString().equals(rightVarTc.toString())) {
						// every thing is fine
					} else {
						
						break;
					}

				} else {
					// add it if all from items are already in the plain select
					// we use the alias to check this.
					Set<String> fiAliases = new HashSet<String>();

					for (FromItem fi : rightVarTc.getFromItems()) {
						fiAliases.add(fi.getAlias());
					}

					if (this._fromItems.keySet().containsAll(fiAliases)) {
						addColumn(rightVarTc, var, true);
						shortcutted = true;
					}
				}
			}
		}

		if (!shortcutted) {
			// create a new subselect
			SubSelect subsell = new SubSelect();
			subsell.setSelectBody(right.getSelectBody());
			subsell.setAlias("subsel_" + subsel_count++);

			List<SelectExpressionItem> newSeis = new ArrayList<SelectExpressionItem>();

			Map<String, SelectExpressionItem> alias2sei = new HashMap<String, SelectExpressionItem>();
			Map<String, Expression> alias2expression = new HashMap<String, Expression>();
			for (Object selectItemObject : this.plainSelect.getSelectItems()) {
				SelectExpressionItem lsei = (SelectExpressionItem) selectItemObject;
				alias2expression.put(lsei.getAlias(), lsei.getExpression());
				alias2sei.put(lsei.getAlias(), lsei);
			}

			List<SelectItem> right_seis = new ArrayList<SelectItem>(
					right.getSelectExpressionItems());
			Map<String, SelectExpressionItem> right_alias2sei = new HashMap<String, SelectExpressionItem>();
			Map<String, Expression> right_alias2expression = new LinkedHashMap<String, Expression>();
			for (SelectItem rSelectItem : right_seis) {
				SelectExpressionItem rsei = (SelectExpressionItem) rSelectItem;
				right_alias2expression.put(rsei.getAlias(),
						rsei.getExpression());
				right_alias2sei.put(rsei.getAlias(), rsei);
			}
			Multimap<String, Column> newColGroups = ArrayListMultimap.create();

			// now iterate over all right select items and check, if they are
			// already present in the query.
			Set<EqualsTo> joinon = new HashSet<EqualsTo>();
			for (String right_alias : right_alias2expression.keySet()) {
				if (alias2sei.containsKey(right_alias)) {
					// duplicate variable, add equals expression
					Expression ljExpression = DataTypeHelper
							.uncast(right_alias2expression.get(right_alias));
					Expression expression = DataTypeHelper
							.uncast(alias2expression.get(right_alias));

					if (ljExpression instanceof StringValue
							&& expression instanceof StringValue
							&& ((StringValue) ljExpression)
									.getNotExcapedValue().equals(
											((StringValue) expression)
													.getNotExcapedValue())) {
						// equals, no need to add anything
					} else if (!(ljExpression instanceof StringValue)
							&& expression instanceof StringValue) {
						EqualsTo eq = new EqualsTo();
						eq.setLeftExpression(alias2expression.get(right_alias));
						eq.setRightExpression(dth.cast(ColumnHelper
								.createColumn(subsell.getAlias(), right_alias)

						, dth.getStringCastType()));
						joinon.add(eq);

					} else {
						EqualsTo eq = new EqualsTo();
						eq.setLeftExpression(alias2expression.get(right_alias));
						eq.setRightExpression(ColumnHelper.createCol(
								subsell.getAlias(), right_alias));
						joinon.add(eq);
					}

				} else {
					// new variable, add to the select items list
					SelectExpressionItem sei = new SelectExpressionItem();
					sei.setAlias(right_alias);

					Column columnProjection = ColumnHelper.createCol(
							subsell.getAlias(), right_alias);
					sei.setExpression(columnProjection);
					newSeis.add(sei);
					newColGroups.put(
							ColumnHelper.colnameBelongsToVar(right_alias),
							columnProjection);

				}
			}

			this.plainSelect.getSelectItems().addAll(newSeis);

			for (String var : newColGroups.keySet()) {

				List<Expression> expressions = (List) newColGroups.get(var);
				TermMap sstc = null;
				// the var is already defined, we therefore need to modify the
				// exisiting Term Map.
				if (colstring2var.inverse().get(var) != null) {
					List<Expression> exprsToBeExtended = new ArrayList<Expression>(
							colstring2TermMap.get(
									colstring2var.inverse().get(var))
									.getExpressions());
					exprsToBeExtended.addAll(expressions);
					sstc = new TermMap(dth, exprsToBeExtended);

				} else {
					// not in there, we can create a new
					sstc = new TermMap(dth, expressions);
				}

				colstring2TermMap.put(sstc.toString(), sstc);

				colstring2var.forcePut(sstc.toString(), var);
			}

			subselects.put(subsell, right);

			if (optional) {
				for (EqualsTo eq : joinon) {
					addOptJoinCondition(eq);
				}
				addOptFromItem(subsell);

			} else {
				for (EqualsTo eq : joinon) {
					addJoinCondition(eq);
				}

				addFromItem(subsell);

			}

		}
	}

	public void addTripleQuery(TermMap origSubject, String subjectAlias,
			TermMap origPredicate, String predicateAlias, TermMap origObject,
			String objectAlias, boolean isOptional) {

		String suffix = "_" + subjectAlias;
		TermMap subject = origSubject.clone(suffix);
		addColumn(subject, subjectAlias, isOptional);
		TermMap object = origObject.clone(suffix);
		addColumn(object, objectAlias, isOptional);
		// if(origPredicate!=null){
		TermMap predicate = origPredicate.clone(suffix);
		addColumn(predicate, predicateAlias, isOptional);
		// }

	}

	private TermMap cloneColOnDuplicateUsage(TermMap term, String tcAlias) {
		// same col used for an other variable, so we have to clone

		TermMap cloneTerm = term.clone("_dup" + dupcounter++);
		
		Map<String,String> oldAlias2newAlias = new HashMap<String, String>(); 
		Set<EqualsTo> oldEqs = new HashSet<EqualsTo>();
		Set<EqualsTo> newEqs = new HashSet<EqualsTo>();
		
		for (int fromItemCount = 0; fromItemCount < term.getFromItems().size(); fromItemCount++) {
			FromItem fri = term.getFromItems().get(fromItemCount);
			FromItem clfri = cloneTerm.getFromItems().get(fromItemCount);
			oldAlias2newAlias.put(fri.getAlias(), clfri.getAlias());
			oldEqs.addAll(getFromItem2joincondition().get(fri.getAlias()));
		}
		
		for(EqualsTo oldEq: oldEqs){
			
			EqualsTo newEq = new EqualsTo();
			newEq.setLeftExpression(cloneEqualsExpression(oldEq.getLeftExpression(), oldAlias2newAlias));
			newEq.setRightExpression(cloneEqualsExpression(oldEq.getRightExpression(), oldAlias2newAlias));
			newEqs.add(newEq);
		}
		
		if(!newEqs.isEmpty()){
			for(EqualsTo eq : newEqs){
				addJoinCondition(eq);
			}
			
			return cloneTerm;
		}else{
			//assuming multiple usage of the same column in a subrequest
			return term;
		}
		
		
		
		
		
//		for (int fromItemCount = 0; fromItemCount < term.getFromItems().size(); fromItemCount++) {
//			FromItem fri = term.getFromItems().get(fromItemCount);
//			FromItem clfri = cloneTerm.getFromItems().get(fromItemCount);
//
//			for (EqualsTo eq : getFromItem2joincondition().get(fri.getAlias())) {
//				
//				
//				Expression rightUncast = DataTypeHelper.uncast(eq
//						.getRightExpression());
//
//				if (rightUncast instanceof Column) {
//					EqualsTo clonedEq = new EqualsTo();
//					clonedEq.setLeftExpression(cloneEqualsExpression(
//							eq.getLeftExpression(), fri, clfri));
//
//					clonedEq.setRightExpression(cloneEqualsExpression(
//							eq.getRightExpression(), fri, clfri));
//
//					newEqs.add(clonedEq);
//				}
//			}
//		}
//		
		
//		for (EqualsTo clonedEq : newEqs) {
//			addJoinCondition(clonedEq);
//			_fromItem2joincondition.put(((Column) DataTypeHelper.uncast(clonedEq
//					.getLeftExpression())).getTable().getAlias(), clonedEq);
//			_fromItem2joincondition.put(((Column) DataTypeHelper.uncast(clonedEq
//					.getRightExpression())).getTable().getAlias(), clonedEq);
//		}

	
	}

	private Expression cloneEqualsExpression(Expression castedCol,
			Map<String,String> old2new) {
		if (DataTypeHelper.uncast(castedCol) instanceof Column) {
			Column col = (Column) DataTypeHelper.uncast(castedCol);
			if (old2new.containsKey(col.getTable().getAlias())) {
				String origCastType = DataTypeHelper.getCastType(castedCol);

				Column clonedcolumn = ColumnHelper.createColumn(
						old2new.get(col.getTable().getAlias()),
						((Column) DataTypeHelper.uncast(castedCol))
								.getColumnName());

				// new Column((Table)newFi, ((Column)
				// DataTypeHelper.uncast(castedCol)).getColumnName());

				//
				// = ColumnHelper.createColumn(newFi.getAlias(), ((Column)
				// DataTypeHelper.uncast(castedCol)).getColumnName());
				return dth.cast(clonedcolumn, origCastType);
			}
		}
		return castedCol;

	}

	private void createNotNull(TermMap term, String termAlias) {

		for (Expression colExpr : term.getExpressions()) {
			colExpr = DataTypeHelper.uncast(colExpr);
			if (colExpr instanceof Column) {
				IsNullExpression notnull = new IsNullExpression();
				notnull.setNot(true);
				notnull.setLeftExpression(colExpr);
				addSQLFilter(notnull);
			}

		}

	}

	private void depthFirst(FromItem fi, List<String> connected,
			Map<String, FromItem> toLookAt) {
		// get the join connections
		Collection<EqualsTo> joinconds = _fromItem2joincondition.get(fi
				.getAlias());
		connected.add(fi.getAlias());

		for (EqualsTo equalsTo : joinconds) {
			FromItem theOtherInTheJoin = FilterUtil.getOtherFromItem(equalsTo,
					fi);
			if (theOtherInTheJoin!=null&& toLookAt.containsKey(theOtherInTheJoin.getAlias())) {
				toLookAt.remove(theOtherInTheJoin.getAlias());
				depthFirst(theOtherInTheJoin, connected, toLookAt);
			}
		}
	}

	/**
	 * checks if a column aliased varnull exists. if not a null column with that
	 * name is added. useful for aligning unions.
	 * 
	 * @param varNull
	 * @return true if sth. was added, otherwise false
	 */
	public boolean fillWithNullColumn(String varNull) {
		for (String var : colstring2var.values()) {
			if (var.equals(varNull)) {
				// column already present, do nothing
				return false;
			}
		}

		// we're here, so it is not already there
		SelectExpressionItem sei = new SelectExpressionItem();
		sei.setExpression(new NullValue());
		sei.setAlias(varNull);
		plainSelect.getSelectItems().add(sei);
		return true;

	}

	public Map<String, TermMap> getColstring2Col() {
		return colstring2TermMap;
	}

	public BiMap<String, String> getColstring2Var() {
		return colstring2var;
	}

	public Multimap<String, EqualsTo> getFromItem2joincondition() {
		return _fromItem2joincondition;
	}

	public PlainSelect getPlainSelect() {

		return plainSelect;
	}

	@Override
	public SelectBody getSelectBody() {

		return plainSelect;
	}

	@Override
	public List<SelectItem> getSelectExpressionItems() {
		return plainSelect.getSelectItems();
	}

	public Map<SubSelect, Wrapper> getSubselects() {
		return subselects;
	}

	@Override
	public Set<String> getVarsMentioned() {

		return new HashSet<String>(this.getColstring2Var().values());
	}

	/**
	 * this method pushes the fromItems with their join conditions from this
	 * object into the plainselect, thus creating the actual join structure.
	 */
	private void putFromItems() {

		// we start with purging the previously created stuff
		plainSelect.setFromItem(null);
		plainSelect.setJoins(new ArrayList<Join>());

		// determine the order of the from items
		Map<String, FromItem> tooLookAt = new TreeMap<String, FromItem>(
				_fromItems);

		// contains the lists of connected blocks
		List<List<String>> connectedJoinBlock = new ArrayList<List<String>>();

		// take the first to start with
		while (tooLookAt.size() > 0) {

			FromItem rootFi = tooLookAt.values().iterator().next();
			tooLookAt.remove(rootFi.getAlias());
			List<String> connected = new ArrayList<String>();
			depthFirst(rootFi, connected, tooLookAt);
			connectedJoinBlock.add(connected);
		}

		// we put the blocks into the query.

		Set<String> inTheQuery = new HashSet<String>();

		for (List<String> joinBlock : connectedJoinBlock) {

			for (String fromItemString : joinBlock) {
				inTheQuery.add(fromItemString);
				FromItem fi = _fromItems.get(fromItemString);

				if (plainSelect.getFromItem() == null) {
					plainSelect.setFromItem(fi);
				} else {
					// we create a join
					Collection<EqualsTo> eqs = _fromItem2joincondition.get(fi
							.getAlias());
					Collection<EqualsTo> eqsWeCanUse = new HashSet<EqualsTo>();
					for (EqualsTo equalsTo : eqs) {
						if (FilterUtil.getOtherFromItem(
								equalsTo, fi)!=null && inTheQuery.contains(FilterUtil.getOtherFromItem(
								equalsTo, fi).getAlias())) {
							eqsWeCanUse.add(equalsTo);
						}
					}

					Join join = new Join();
					join.setRightItem(fi);

					if (eqsWeCanUse.size() > 0) {

						List<Expression> simplified = new ArrayList<Expression>();

						for (EqualsTo equalsTo : eqsWeCanUse) {
							simplified
									.add(fopt.shortCut((Expression) equalsTo));
						}

						join.setOnExpression(FilterUtil
								.conjunctFilters(new ArrayList<Expression>(
										simplified)));
					} else {
						join.setSimple(true);
					}
					plainSelect.getJoins().add(join);

				}
			}

		}

		// adding the optionals here
		for (FromItem ofi : _optFromItems.values()) {
			Collection<EqualsTo> joincond = _optFromItem2joincondition.get(ofi
					.getAlias());

			Join ojoin = new Join();
			ojoin.setLeft(true);
			ojoin.setOnExpression(FilterUtil
					.conjunctFilters(new ArrayList<Expression>(joincond)));
			ojoin.setRightItem(ofi);
			plainSelect.getJoins().add(ojoin);
		}

	}

	public void setOptional() {
		Expression where = plainSelect.getWhere();
		List<Expression> filters  = new ArrayList<Expression>();
		final List<Expression> toRetain = new ArrayList<Expression>();

		FilterUtil.splitFilters(where, filters);
		for (Expression expression : filters) {
			if(!(expression instanceof IsNullExpression)){
				toRetain.add(expression);
			}
		}
		plainSelect.setWhere(FilterUtil.conjunctFilters(toRetain));
		
	}
	
	public void setDistinct(boolean distinct){
		if(distinct){
			this.plainSelect.setDistinct(new Distinct());
		}else{
			this.plainSelect.setDistinct(null);
		}
		
	}
	

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("*************\n");
		sb.append("JOIN Conditions are:\n");

		for (String fis : _fromItem2joincondition.keySet()) {
			sb.append("  FromItem: " + fis + "\n");
			for (EqualsTo eq : _fromItem2joincondition.get(fis)) {

				sb.append("         " + eq.toString() + "\n");
			}

		}

		return sb.toString();
	}

	public void setLimit(int i) {
		Limit lim = new Limit();
		lim.setRowCount(i);
		this.plainSelect.setLimit(lim);
	}

	public void addNullTriple(String s, String p, String o) {
		
	}


}
