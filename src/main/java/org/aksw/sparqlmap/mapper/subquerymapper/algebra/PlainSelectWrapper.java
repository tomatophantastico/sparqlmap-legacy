package org.aksw.sparqlmap.mapper.subquerymapper.algebra;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SubSelect;

import org.aksw.sparqlmap.config.syntax.ColumnTermCreator;
import org.aksw.sparqlmap.config.syntax.MappingConfiguration;
import org.aksw.sparqlmap.config.syntax.SubSelectTermCreator;
import org.aksw.sparqlmap.config.syntax.TermCreator;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import com.hp.hpl.jena.sparql.expr.E_Equals;
import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.expr.nodevalue.NodeValueNode;

public class PlainSelectWrapper implements Wrapper {

	static org.slf4j.Logger log = org.slf4j.LoggerFactory
			.getLogger(PlainSelectWrapper.class);

	private DataTypeHelper dataTypeHelper;

	private int dupcounter = 0;

	private PlainSelect plainSelect = new PlainSelect();

	private FilterUtil filterUtil;

	private ColumnTermCreator crc;

	// private Multimap<String, Mapping> ldpMappings = HashMultimap.create();

	private Map<SubSelect, Wrapper> subselects = new HashMap<SubSelect, Wrapper>();

	private BiMap<String, String> colstring2var = HashBiMap.create();

	private Map<String, TermCreator> colstring2Col = new HashMap<String, TermCreator>();
	
	private MappingConfiguration mappingConfiguration;

	public PlainSelectWrapper(Map<SelectBody, Wrapper> registerTo,
			MappingConfiguration mappingConfiguration) {

		
		super();
		this.mappingConfiguration = mappingConfiguration;
		this.dataTypeHelper = mappingConfiguration.getR2rconf().getDbConn()
				.getDataTypeHelper();
		this.filterUtil = mappingConfiguration.getFilterUtil();

		plainSelect.setSelectItems(new ArrayList<SelectExpressionItem>());
		plainSelect.setJoins(new ArrayList());
		registerTo.put(plainSelect, this);
	}

	public void addTripleQuery(TermCreator subject, String subjectAlias,
			TermCreator object, String objectAlias, boolean isOptional) {
		addTripleQuery(subject, subjectAlias, null, null, object, objectAlias,
				isOptional);
	}

	public void addTripleQuery(TermCreator origSubject, String subjectAlias,
			TermCreator origPredicate, String predicateAlias,
			TermCreator origObject, String objectAlias, boolean isOptional) {

		String suffix = "_" + subjectAlias;
		TermCreator subject = origSubject.clone(suffix);
		addColumn(subject, subjectAlias,  isOptional);
		TermCreator object = origObject.clone(suffix);
		addColumn(object, objectAlias,  isOptional);
		if(origPredicate!=null){
			TermCreator predicate = origPredicate.clone(suffix);
			addColumn(predicate, predicateAlias,  isOptional);	
		}
		

	}

	private void addColumn(TermCreator tc, String tcAlias,
			boolean isOptional) {
		TermCreator term = tc;

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
			if (!colstring2Col.get(inThereColString).equals(term)) {

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
					TermCreator oldTc = colstring2Col.get(colstring2var
							.inverse().get(tcAlias));
					List<EqualsTo> eqs = FilterUtil.createEqualsTos(
							oldTc.getExpressions(), term.getExpressions());

						for (int i = 0; i < eqs.size(); i++) {
							fromItem2joincondition.put(((Column)FilterUtil.uncast(eqs.get(i).getLeftExpression())).getTable(), eqs.get(i));
							fromItem2joincondition.put(((Column)FilterUtil.uncast(eqs.get(i).getRightExpression())).getTable(), eqs.get(i));
							//fromItem2joincondition.put(term.getFromItems().get(i), eqs.get(i));
						}


				}

			}

		} else {
			colstring2var.put(term.toString(), tcAlias);
			colstring2Col.put(term.toString(), term);
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

	private TermCreator cloneColOnDuplicateUsage(TermCreator term,
			String tcAlias) {
		// same col used for an other variable, so we have to clone

		TermCreator cloneTerm = term.clone("_dupVar_" + dupcounter++);

		// for (int i = 0; i < dupvarCol.getMapp().getIdColumn()
		// .getIdColumns().size(); i++) {
		// // we need to join this new mapping to with the one we
		// // just
		// // cloned
		// List<EqualsTo> eqs = new ArrayList<EqualsTo>();
		//
		// // get the join to from the original column of our clone
		// Collection<Expression> origeqs = joincondition
		// .get(term.getMapp().getFromPart());
		// if (origeqs == null || origeqs.isEmpty()) {
		// EqualsTo eq = new EqualsTo();
		// eq.setRightExpression(dupvarCol.getMapp()
		// .getIdColumn().getColumnExpression());
		// eq.setLeftExpression(term.getMapp().getIdColumn()
		// .getColumnExpression());
		// eqs.add(eq);
		// } else {
		// for (Expression origeq : origeqs) {
		// // this one is flaky
		//
		// EqualsTo eq = new EqualsTo();
		//
		// if (origeq instanceof EqualsTo) {
		// Expression exprToModify = ((BinaryExpression) origeq)
		// .getRightExpression();
		//
		// // we find out at which place the
		// // exprToMidify is in the resources of the
		// // old one and we replace it with the new
		// // one
		//
		// for (int j = 0; j < term.getMapp()
		// .getIdColumn()
		// .getColumnExpressions().size(); j++) {
		// if (term
		// .getMapp()
		// .getIdColumn()
		// .getColumnExpressions()
		// .get(j)
		// .toString()
		// .equals(exprToModify.toString())) {
		// eq.setRightExpression(dupvarCol
		// .getMapp().getIdColumn()
		// .getColumnExpressions()
		// .get(j));
		// }
		// }
		//
		// // we have to replace all the join from the
		// // old construct with the new construct
		// ((EqualsTo) origeq).getRightExpression();
		// eq.setLeftExpression(((EqualsTo) origeq)
		// .getLeftExpression());
		// } else {
		// throw new ImplementationException(
		// "Implement and join conditions");
		// }
		// eqs.add(eq);
		// }
		// }
		//
		// for (EqualsTo eq : eqs) {
		// eq = (EqualsTo) FilterUtil.shortCut(eq);
		// joincondition.put(
		// dupvarCol.getMapp().getFromPart(), eq);
		// }
		//
		// }
		
		
		//get the equals filters for that term
		Set<EqualsTo> newEqs = new HashSet<EqualsTo>();
		
		for(int fromItemCount = 0;fromItemCount <term.getFromItems().size();fromItemCount++){
			FromItem fri = term.getFromItems().get(fromItemCount);
			FromItem clfri = cloneTerm.getFromItems().get(fromItemCount);
	
		
			
			for(EqualsTo eq : fromItem2joincondition.get(fri)){
				Expression rightUncast = FilterUtil.uncast(eq.getRightExpression());
				
				if(rightUncast instanceof Column){
					EqualsTo clonedEq =  new EqualsTo();					
					clonedEq.setLeftExpression(cloneEqualsExpression(eq.getLeftExpression(),fri,clfri));
					
					clonedEq.setRightExpression(cloneEqualsExpression(eq.getRightExpression(),fri,clfri));
					
					newEqs.add(clonedEq);	
				}
			}
		}
		for (EqualsTo clonedEq : newEqs) {
			fromItem2joincondition.put(((Column)FilterUtil.uncast(clonedEq.getLeftExpression())).getTable(), clonedEq);
			fromItem2joincondition.put(((Column)FilterUtil.uncast(clonedEq.getRightExpression())).getTable(), clonedEq);
		}
		
		return cloneTerm;
	}
	
	
	private Expression cloneEqualsExpression(Expression castedCol, FromItem oldFi, FromItem newFi){
		if( FilterUtil.uncast(castedCol) instanceof Column) {
			Column col  = (Column) FilterUtil.uncast(castedCol);
			if(col.getTable().toString().equals(oldFi.toString())){
				String origCastType = FilterUtil.getCastType(castedCol);
				
				
				Column clonedcolumn = new Column((Table)newFi, ((Column) FilterUtil.uncast(castedCol)).getColumnName());
		 //= FilterUtil.createColumn(newFi.getAlias(), ((Column) FilterUtil.uncast(castedCol)).getColumnName());
				return FilterUtil.cast(clonedcolumn, origCastType);
			}
		}
		return castedCol;
		
	}
	
	
	
	 
	
	

	private void createNotNull(TermCreator term, String termAlias) {

		for (Expression colExpr : term.getExpressions()) {
			colExpr = FilterUtil.uncast(colExpr);
			if (colExpr instanceof Column) {
				IsNullExpression notnull = new IsNullExpression();
				notnull.setNot(true);
				notnull.setLeftExpression(colExpr);
				addSQLFilter(notnull);
			}

		}

	}

	public static int lj_count = 0;
	public static int subsel_count = 0;

	/**
	 * adds a subselect to the plain select. will create the join conditions.
	 * 
	 * @param right
	 * @param optional
	 *            when true, the plain select will be added with a left join
	 */
	public void addSubselect(Wrapper right, boolean optional) {
		
		//check if the subselect queries only for a single triple and is optional.
		// then we can add it directly to the plain select.
		boolean shortcutted = false;
		
		if(optional == true && right instanceof PlainSelectWrapper && ( (PlainSelectWrapper) right).getVarsMentioned().size() == 2 && ( (PlainSelectWrapper) right).subselects.size()==0){
			PlainSelectWrapper ps = (PlainSelectWrapper) right;
			
			//we require the not shared variable to be either constant resource or a literal. column generated resources can be troublesome, if generated from more than one column.
			
			for(String var: ps.getVarsMentioned()){
				
				TermCreator rightVarTc = ps.getColstring2Col().get( ps.getColstring2Var().inverse().get(var));
				//variables already there and equal can be ignored
				// if not equal, we cannot use this optimization
				TermCreator thisVarTc = colstring2Col.get(this.colstring2var.inverse().get(var));
				if(thisVarTc!=null){
					//compare it
					
					if(thisVarTc.toString().equals(rightVarTc.toString())){
						//every thing is fine
					}else{
						throw new ImplementationException("Should not happen, some assumption is wrong");
					}
					
					
					
				}else{
					//add it if all from items are already in the plain select
					//we use the alias to check this.
					Set<String> fiAliases = new HashSet<String>();
					
					for(FromItem fi : rightVarTc.getFromItems()){
						fiAliases.add(fi.getAlias());
					}
					
					if(this.fromItems.keySet().containsAll(fiAliases)){
						addColumn(rightVarTc, var, true);
						shortcutted = true;
					}
					
					
					
				}
				
				
				
				
			}
			
			
			
		}
	
		
		
		
		if(!shortcutted){
		

		SubSelect subsell = new SubSelect();
		subsell.setSelectBody(right.getSelectBody());
		subsell.setAlias("subsel_" + subsel_count++);


		Set<EqualsTo> joinon = new HashSet<EqualsTo>();
		List<SelectExpressionItem> newSeis = new ArrayList<SelectExpressionItem>();

		List<SelectItem> right_seis = new ArrayList<SelectItem>(
				right.getSelectExpressionItems());

		Map<String, SelectExpressionItem> alias2sei = new HashMap<String, SelectExpressionItem>();
		Map<String, Expression> alias2expression = new HashMap<String, Expression>();
		for (Object selectItemObject : this.plainSelect.getSelectItems()) {
			SelectExpressionItem lsei = (SelectExpressionItem) selectItemObject;
			alias2expression.put(lsei.getAlias(), lsei.getExpression());
			alias2sei.put(lsei.getAlias(), lsei);
		}
		Map<String, SelectExpressionItem> right_alias2sei = new HashMap<String, SelectExpressionItem>();
		Map<String, Expression> right_alias2expression = new LinkedHashMap<String, Expression>();
		for (SelectItem rSelectItem : right_seis) {
			SelectExpressionItem rsei = (SelectExpressionItem) rSelectItem;
			right_alias2expression.put(rsei.getAlias(), rsei.getExpression());
			right_alias2sei.put(rsei.getAlias(), rsei);
		}
		Multimap<String,Column> newColGroups = ArrayListMultimap.create();

		for (String right_alias : right_alias2expression.keySet()) {
			if (alias2sei.containsKey(right_alias)) {
				// duplicate variable, add equals expression
				Expression ljExpression = FilterUtil
						.uncast(right_alias2expression.get(right_alias));
				Expression expression = FilterUtil.uncast(alias2expression
						.get(right_alias));

				if (ljExpression instanceof StringValue
						&& expression instanceof StringValue
						&& ((StringValue) ljExpression).getNotExcapedValue()
								.equals(((StringValue) expression)
										.getNotExcapedValue())) {
					// equals, no need to add anything
				} else if (!(ljExpression instanceof StringValue)
						&& expression instanceof StringValue) {
					EqualsTo eq = new EqualsTo();
					eq.setLeftExpression(alias2expression.get(right_alias));
					eq.setRightExpression(filterUtil.cast(subsell.getAlias(),
							right_alias, dataTypeHelper.getStringCastType()));
					joinon.add(eq);

				} else {
					EqualsTo eq = new EqualsTo();
					eq.setLeftExpression(alias2expression.get(right_alias));
					eq.setRightExpression(MappingUtils.createCol(
							subsell.getAlias(), right_alias));
					joinon.add(eq);
				}

			} else {
				// new variable, add to the select items list
				SelectExpressionItem sei = new SelectExpressionItem();
				sei.setAlias(right_alias);

				Column columnProjection = MappingUtils.createCol(
						subsell.getAlias(), right_alias);
				sei.setExpression(columnProjection);
				newSeis.add(sei);
				newColGroups.put(ColumnHelper.colnameBelongsToVar(right_alias),columnProjection);

			}
		}

		this.plainSelect.getSelectItems().addAll(newSeis);

		
		

		
		for(String var: newColGroups.keySet()){
			List<Expression> expressions  = (List) newColGroups.get(var);
			SubSelectTermCreator sstc = new SubSelectTermCreator(dataTypeHelper, expressions);
			colstring2Col.put(sstc.toString(), sstc);
			colstring2var.put(sstc.toString(), var);
		}
		
		
		
		
		

	

		subselects.put(subsell, right);
		
		
		if(optional){
			optFromItem2joincondition.putAll(subsell, joinon);
			addOptFromItem(subsell);
			
		}else{
			fromItem2joincondition.putAll(subsell,joinon);
			
			addFromItem(subsell);
			
		}
		
		
		}
	}

	private Map<String, String> varEqualsUriMap = new HashMap<String, String>();

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

				Expression sqlEx = filterUtil.getSQLExpression(expr,
						colstring2var, colstring2Col);
				if (sqlEx != null) {
					addSQLFilter(sqlEx);
				} else {
					log.warn("Unmappable filter condition encountered: "
							+ expr.toString());
				}

			}
		}
	}

	private Map<String, Expression> filters = new HashMap<String, Expression>();

	private void addSQLFilter(Expression sqlEx) {

		if (!filters.containsKey(sqlEx.toString())) {
			filters.put(sqlEx.toString(), sqlEx);
			plainSelect.setWhere(conjunctFilters(new HashSet<Expression>(
					filters.values())));

		}
	}

	private Expression conjunctFilters(Collection<Expression> exps) {
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
	 * extracts the from item of the expressions and adds them to the select
	 * 
	 * @param expression
	 */
	private void addMappingstoQuery(TermCreator tc) {

		
		for(EqualsTo eq : tc.getFromJoins()){
			fromItem2joincondition.put(((Column) eq.getLeftExpression()).getTable(), eq);
			fromItem2joincondition.put(((Column) eq.getRightExpression()).getTable(), eq);
		}
		
		for (FromItem fi : tc.getFromItems()) {
			addFromItem(fi);
		}
		
		
	
		


	}

	/**
	 * simply adds the from item to the query. no join on conditions given
	 * 
	 * @param fi
	 */
	private Map<String, FromItem> fromItems = new LinkedHashMap<String, FromItem>();
	
	private Map<String, FromItem> optFromItems = new LinkedHashMap<String, FromItem>();
	
	private Multimap<FromItem, EqualsTo> fromItem2joincondition = LinkedListMultimap.create();

	private Multimap<FromItem, EqualsTo> optFromItem2joincondition = LinkedListMultimap.create();

	private void addFromItem(FromItem fi) {
		if (!fromItems.containsKey(fi.getAlias())) {
			fromItems.put(fi.getAlias(), fi);
			putFromItems();
		}
	}
	
	private void addOptFromItem(FromItem fi) {
		if (!optFromItems.containsKey(fi.getAlias())) {
			optFromItems.put(fi.getAlias(), fi);
			putFromItems();
		}
	}

	

	/**
	 * this method pushes the fromItems with their join conditions from this
	 * object into the plainselect, thus creating the actual join structure.
	 */
	private void putFromItems() {

		// we start with purging the previously created stuff
		plainSelect.setFromItem(null);
		plainSelect.setJoins(new ArrayList<FromItem>());

		Iterator<FromItem> fis = fromItems.values().iterator();
		FromItem mainfrom = fis.next();
		plainSelect.setFromItem(mainfrom);
		
		
		Set<EqualsTo> mainFromJoinCond = new HashSet<EqualsTo>(fromItem2joincondition.get(plainSelect.getFromItem()));
//		if (fromItem2joincondition.containsKey(plainSelect.getFromItem())) {
//			for (Expression ex : fromItem2joincondition.get(plainSelect.getFromItem())) {
//				addSQLFilter(ex);
//			}
//		}

		while (fis.hasNext()) {
			FromItem fi = fis.next();
			//get the condition, that maps this FromItem with the one in the select
			
			Set<EqualsTo> additionalFiJoinConds = new HashSet<EqualsTo>(fromItem2joincondition.get(fi));
			additionalFiJoinConds.retainAll(mainFromJoinCond);
			
			Set<EqualsTo> leftovers = new HashSet<EqualsTo>(fromItem2joincondition.get(fi));
			leftovers.removeAll(additionalFiJoinConds);
			mainFromJoinCond.addAll(leftovers);
			
			Join join = new Join();
			join.setRightItem(fi);

			if (additionalFiJoinConds.size()>0) {
				
				List<Expression> simplified = new ArrayList<Expression>();
				
				for (EqualsTo equalsTo : additionalFiJoinConds) {
					simplified.add(filterUtil.shortCutFilter(equalsTo));
				}

				join.setOnExpression(this
						.conjunctFilters(new ArrayList<Expression>(
								simplified)));
			} else {
				join.setSimple(true);
			}
			plainSelect.getJoins().add(join);
		}
		// adding the optionals here
		
		for(FromItem ofi: optFromItems.values()){
			Collection<EqualsTo> joincond = optFromItem2joincondition.get(ofi);
			
			Join ojoin = new Join();
			ojoin.setLeft(true);
			ojoin.setOnExpression(conjunctFilters(new ArrayList<Expression>(joincond)));
			ojoin.setRightItem(ofi);
			plainSelect.getJoins().add(ojoin);
		}
		

	}

	

	public PlainSelect getPlainSelect() {

		return plainSelect;
	}

	@Override
	public SelectBody getSelectBody() {

		return plainSelect;
	}

	public BiMap<String, String> getColstring2Var() {
		return colstring2var;
	}

	public Map<String, TermCreator> getColstring2Col() {
		return colstring2Col;
	}

	@Override
	public Set<String> getVarsMentioned() {

		return new HashSet<String>(this.getColstring2Var().values());
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

	@Override
	public List<SelectExpressionItem> getSelectExpressionItems() {
		return plainSelect.getSelectItems();
	}
	
	
	
	
	

}
