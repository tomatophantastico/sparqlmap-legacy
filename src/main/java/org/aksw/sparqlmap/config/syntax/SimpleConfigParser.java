package org.aksw.sparqlmap.config.syntax;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.Union;

import org.aksw.sparqlmap.config.syntax.r2rml.ColumnTermCreator;
import org.aksw.sparqlmap.config.syntax.r2rml.ConstantResourceCreator;
import org.aksw.sparqlmap.config.syntax.r2rml.ConstantValueColumn;
import org.aksw.sparqlmap.config.syntax.r2rml.TermCreator;
import org.aksw.sparqlmap.db.SQLAccessFacade;
import org.aksw.sparqlmap.mapper.subquerymapper.algebra.ColumnHelper;
import org.aksw.sparqlmap.mapper.subquerymapper.algebra.DataTypeHelper;
import org.aksw.sparqlmap.mapper.subquerymapper.algebra.FilterUtil;
import org.aksw.sparqlmap.mapper.subquerymapper.algebra.ImplementationException;
import org.apache.log4j.Logger;

import com.Ostermiller.util.CSVParser;
import com.hp.hpl.jena.vocabulary.RDF;

public class SimpleConfigParser {

	private static Logger log = Logger.getLogger(SimpleConfigParser.class);

	private R2RConfiguration config;

	private SQLAccessFacade sqlfacade;

	public void setSqlfacade(SQLAccessFacade sqlfacade) {
		this.sqlfacade = sqlfacade;
	}

	public R2RConfiguration parse(Reader in) throws IOException {
		config = new R2RConfiguration();

		// first we cleanse the input file from all the comments
		List<String> confElements = convertToSingleLineElements(in);

		// then we parse these elements
		for (String confElement : confElements) {
			parseElement(confElement);
		}
		// and voila, we got the config

		// we add the templates of the mappings into the id column definitions
		for (Mapping mapping : config.getMappingConfiguration().getMappings()) {
			mapping.getIdColumn().setUriTemplate(mapping.getLdtemplate());
		}

		// we now link the templates of resource creating coldefs to the
		// underlying columns
		
		for (Mapping mapping : config.getMappingConfiguration().getMappings()) {
			for(ColumDefinition coldef: mapping.getColDefinitions()){
				if(!getResourceSegements(coldef).isEmpty()){
					ColumnTermCreator ctc = new ColumnTermCreator(config.getDbConn().getDataTypeHelper(), getResourceExpressions(coldef),getFromItems(coldef),getJoinConditions(coldef));
					coldef.setTermCreator(ctc);
				} else
				//we further set the literalExpression for the non-resource cols as the column
				{
					ColumnTermCreator ctc = new ColumnTermCreator(config.getDbConn().getDataTypeHelper(),coldef.getSqldataType(), getLiteralExpressions(coldef),getFromItems(coldef),getJoinConditions(coldef));
					coldef.setTermCreator(ctc);
				}		
			}
			
			//process the type of statement
			
			ColumDefinition typecol = new ColumDefinition();
			typecol.setProperty(RDF.type.getURI());
			TermCreator typeTc = new ConstantResourceCreator(config.getDbConn().getDataTypeHelper(), mapping.getTypeOf().getURI());
			typecol.setTermCreator(typeTc);
			typecol.setMapp(mapping);
			mapping.addColDefinition(typecol);
			
		}
		config.getMappingConfiguration().fillMaps();

		return config;

	}

	private List<String> getResourceSegements(ColumDefinition coldef) {
		List<String> segs = new ArrayList<String>();
		Mapping mapOfCol = coldef.getMapp();
		if (coldef instanceof ConstantValueColumn) {
			segs.add(((ConstantValueColumn) coldef).getValue());
		} else if (mapOfCol.getIdColumn().equals(coldef)) {
			// use the template of the mapping
			segs.addAll(splitTemplate(mapOfCol.getIdColumn().getUriTemplate()));

		} else if (coldef.getJoinsAlias() != null) {
			// use segments of the referenced table
			Mapping joinWithMapp = config.getMappingConfiguration().getMappingForAlias(
					coldef.getJoinsAlias());
			ColumDefinition joinWithIdColumDefinition = joinWithMapp.getIdColumn();
			//we check if the colums  are constructable from the foreign key we got here.
			//this is the case, if the template of the referenced table is contains only its id cols 
						
			List<String> joinWithSegs = splitTemplate(joinWithIdColumDefinition.getUriTemplate());
			
			
			if(joinWithSegs.get(1).toString().equals(joinWithIdColumDefinition.getColumn().getTable().getAlias() +"." + joinWithIdColumDefinition.getColumn().getColumnName())){
				//they are, we can replace this segment with this column
				joinWithSegs.set(1, coldef.getColumn().getTable().getAlias() +"." + coldef.getColumn().getColumnName());
			}
			
			segs.addAll(joinWithSegs);

		} else if (coldef.getUriTemplate() != null) {
			if (!coldef.getUriTemplate().contains("{")) {
				coldef.setUriTemplate( coldef.getUriTemplate()+"{" + coldef.getMapp().getFromPart().getAlias() + "."
						+ coldef.getColname() + "}");
			}
			// this column itself produces an iri
			segs.addAll(splitTemplate(coldef.getUriTemplate()));
		}
		return segs;
	}

	private List<String> splitTemplate(String template) {
	
		
		String[] ldsplits = template.split("\\{|\\}");
		return Arrays.asList(ldsplits);
	

	}

	public List<Expression> getResourceExpressions(ColumDefinition coldef) {
		
		DataTypeHelper dth = config.getDbConn().getDataTypeHelper();
		List<String> resourceSegments = getResourceSegements(coldef);
		List<Expression> expressions = ColumnHelper.getBaseExpressions(1, resourceSegments.size(), ColumnHelper.COL_SQL_TYPE_RESOURCE,config.getDbConn().getDataTypeHelper(),null,null,null);
		for (int i = 0; i < resourceSegments.size(); i++) {
			if (i % 2 == 0) {
				// we have a string
				expressions.add(FilterUtil.cast(new net.sf.jsqlparser.expression.StringValue(
						"'" + resourceSegments.get(i) + "'"),dth.getStringCastType()));
			} else {
				// string denominates a column
				String[] colname = resourceSegments.get(i).split("\\.");
				if (colname.length != 2) {
					throw new ImplementationException(
							resourceSegments.get(i)
									+ " was split in more than 2 segments. Either schema is present (not supported atm ) or sth else");
				}

				if (colname[0].equals(coldef.getMapp().getName())) {
					expressions.add(FilterUtil.cast(coldef.getMapp()
							.getColumnDefinition(colname[1]).getColumn(),dth.getStringCastType()));
				} else {
					// need to get resource creation column from
					ColumDefinition rCol = config.getMappingConfiguration().getColumnForName(colname[0],
							colname[1]);
					expressions.add(FilterUtil.cast(rCol.colum,dth.getStringCastType()));
				}
			}
		}
		return expressions;
	}
	
	public List<Expression> getLiteralExpressions(ColumDefinition coldef){
		List<Expression> literalExpressions = ColumnHelper.getBaseExpressions(2, 0, coldef.getSqldataType(),config.getDbConn().getDataTypeHelper(),coldef.getDatatype(), coldef.getLanguage(),coldef.getLanguageColumn());
		
		
		Expression castedcol = FilterUtil.cast(coldef.getColumn(), config.getDbConn().getDataTypeHelper().getCastTypeString(coldef.getSqldataType()));
		literalExpressions.add(castedcol);
		
		//if not a string, cast to it
//		if(!config.getDbConn().getDataTypeHelper().getCastTypeString(coldef.getSqldataType()).equals(config.getDbConn().getDataTypeHelper().getStringCastType())){
//			Expression stringcol = FilterUtil.cast(coldef.getColumn(), config.getDbConn().getDataTypeHelper().getStringCastType());
//			literalExpressions.add(stringcol);
//			
//		}
		
		
		return literalExpressions;
	}
	
	public List<FromItem> getFromItems(ColumDefinition coldef){
		List<FromItem> fis = new ArrayList<FromItem>();
		for(Expression expr: getResourceExpressions(coldef)){
			expr = FilterUtil.uncast(expr);
			if (expr instanceof Column) {
				Column col = (Column) expr;
				fis.add(col.getTable());
			}
		}
		return fis;
	}
	
	/**
	 * create the join conditions between the fromitems that construct a resource
	 * For now we assume, that it is a joind on the ids of the related tables.
	 * @param coldef
	 * @return
	 */
	public List<EqualsTo> getJoinConditions(ColumDefinition coldef){
		List<EqualsTo> eqs = new ArrayList<EqualsTo>();
		List<FromItem> fis = getFromItems(coldef);
		
		
		for(FromItem fi : fis){
			if(!fi.equals(coldef.getMapp().getFromPart())){
				EqualsTo eq = new EqualsTo();
				Column templateFromItemIdcol = config.getMappingConfiguration().getMappingForAlias(fi.getAlias()).getMappingIdCol();
				eq.setRightExpression(templateFromItemIdcol);
				eq.setLeftExpression(coldef.getColumn());
				eqs.add(eq);
			}
		}
		
		
		
		
//		if(fis.size()>1){
//			Iterator<FromItem> fiIter = fis.iterator();
//			FromItem firstFi = fiIter.next();
//			Column first = config.getMappingConfiguration().getMappingForAlias(firstFi.getAlias()).getMappingIdCol();
//			while(fiIter.hasNext()){
//				FromItem nextFi = fiIter.next();
//				if(!nextFi.toString().equals(firstFi.toString())){
//					EqualsTo eq = new EqualsTo();
//					Column next = config.getMappingConfiguration().getMappingForAlias(nextFi.getAlias()).getMappingIdCol();
//					eq.setRightExpression(next);
//					eq.setLeftExpression(first);
//					eqs.add(eq);
//				}
//			}	
//		}
		return eqs;		
	}
	

	private List<String> convertToSingleLineElements(Reader in)
			throws IOException {
		List<String> confElements = new ArrayList<String>();

		BufferedReader bin = new BufferedReader(in);
		String line;
		StringBuffer confElementString = new StringBuffer();
		while ((line = bin.readLine()) != null) {
			line.trim();
			// we only deal with non-empty, non-comment lines
			if (!line.isEmpty() && !line.startsWith("//")) {

				// remove all line breaks
				line = line.replace("\r\n", " ").replace("\n", " ");
				// we add the string to the buffer
				confElementString.append(line);

				// if now the line is ends with a semicolon, it is considered
				// finished
				if (line.endsWith(";")) {
					confElements.add(confElementString.toString());
					confElementString = new StringBuffer();
				}
			}
		}
		return confElements;
	}

	private void parseElement(String element) throws IOException {
		Scanner scanner = new Scanner(element);
		String start = scanner.useDelimiter("\\(").next();

		String content = element.substring(element.indexOf("(") + 1,
				element.indexOf(")"));

		if (start.equals(ParsingConstants.CONNECTION)) {
			parseConnection(content);
		} else if (start.equals(ParsingConstants.NAMESPACE)) {
			parseNamespace(content);
		} else if (start.equals(ParsingConstants.MAPPING)) {
			parseMapping(content);
		} else if (start.equals(ParsingConstants.R2RBASE)) {
			parseBase(content);
		} else if (start.equals(ParsingConstants.R2RSCHEMABASE)) {
			parseSchemaBase(content);

		} else

		{
			log.warn("Unknown Config Element");
		}

		scanner.close();
	}

	private void parseSchemaBase(String content) {
		config.getNameSpaceConfig().setSchemaBase(splitAndClean(content)[0]);

	}

	private void parseBase(String content) {
		config.getNameSpaceConfig().setInstanceBase(splitAndClean(content)[0]);

	}

	private String[] splitAndClean(String string) {
		return CSVParser.parse(string)[0];

	}

	private void parseMapping(String element) {
		Mapping map = new Mapping();
		String[] elementSplit = splitAndClean(element);

		String viewOrTablePart = elementSplit[0];

		List<SelectExpressionItem> selectExpressionItems = new ArrayList<SelectExpressionItem>();
		Map<String, Integer> selectExpressionItemDataType = new HashMap<String, Integer>();

		// detect if table or query by checking if it consits only out of 1
		// word.
		FromItem fromItem = null;

		if (viewOrTablePart.trim().contains("\\s")) {

			// create the from Item by extracting the select Part
			Statement viewStatement = null;
			try {
				viewStatement = new CCJSqlParserManager()
						.parse(new StringReader(viewOrTablePart));
			} catch (JSQLParserException e) {
				log.error("Error parsing a view statment in the config", e);
			}
			SubSelect viewSubselect = new SubSelect();
			viewSubselect.setSelectBody(ParsingUtils
					.extractSelectBody(viewStatement));
			fromItem = viewSubselect;

			// we most got a view definition
			List<SelectItem> selectItems = ParsingUtils
					.extractSelectItems(viewSubselect.getSelectBody());

			// we have to check if the List not only contains a "*"
			boolean containsAsterisk = false;
			for (SelectItem selectItem : selectItems) {
				if (selectItem instanceof SelectExpressionItem) {
					selectExpressionItems
							.add((SelectExpressionItem) selectItem);
				} else {
					containsAsterisk = true;
					break;
				}

			}
			if (containsAsterisk) {
				// we have to resolve the asterisks
				selectExpressionItems = sqlfacade
						.getSelectItemsForView(viewStatement);
			}

			// check the data types of the queries
			selectExpressionItemDataType = sqlfacade
					.getDataTypeForView(viewStatement);

		} else {
			// it seems that a table is mentioned in the view part, so we
			// retrieve the description of such this table
			String[] schema_table = viewOrTablePart.split(".");
			Table table;
			if (schema_table.length > 1) {
				table = new Table(schema_table[0], schema_table[1]);
			} else {
				table = new Table();
				table.setName(viewOrTablePart);
			}

			selectExpressionItems = sqlfacade.getSelectItemsForTable(table);
			// check the data types of the queries
			selectExpressionItemDataType = sqlfacade.getDataTypeForTable(table);

			fromItem = table;
			fromItem.setAlias(table.getName());

		}
		map.setName(fromItem.getAlias());
		map.setFromPart(fromItem);
		map.setLdtemplate(config.getNameSpaceConfig().getInstanceBase()
				+ elementSplit[1]);
		map.setTypeOf(config.getNameSpaceConfig().resolveToResource(
				elementSplit[2]));
		
		
		
		
		
		
		map.setId(elementSplit[3]);

		// this variable defines at which column in the config file the
		// definition of columns starts
		int colStart = 4;

		for (int i = colStart; i < elementSplit.length; i++) {
			// check if the column is defined
			String prop = null;

			// check the column is a relation
			Column column = ((Column) selectExpressionItems.get(i - colStart)
					.getExpression());
			Integer dataType = selectExpressionItemDataType.get(column
					.getColumnName());
			
			//we first check for language tags defined with @. These are always at the rear.
			
			String lang = null;
			Column langColumn = null;
			
			if(elementSplit[i].contains("@")){
				
				String langdef =elementSplit[i].split("@")[1]; 
				//check if it is a column
				if(langdef.startsWith("->")){
					langColumn = new Column(column.getTable(), langdef.substring(2));
				}else if(langdef.length()==2){ //should be iso code
					lang = langdef;
				}else{
					throw new RuntimeException("Error in config, check language def (@)");
				}
				elementSplit[i] = elementSplit[i].split("@")[0];
			}
			
			String literalType = null;
			
			if(elementSplit[i].contains("^^")){
				//add the datatype
				String typeDefString = elementSplit[i].split("\\^\\^")[1]; 
				literalType = this.config.getNameSpaceConfig().resolveToResource(typeDefString).getURI();
				
				
			}
			
			
			
			
			if (!elementSplit[i].contains("->")) {
				if (elementSplit[i].equals("NULL")) {
					prop = config.getNameSpaceConfig().resolveToProperty(
							"http://UNMAPPED_PROPERTY");
				} else {
					prop = config.getNameSpaceConfig().resolveToProperty(
							elementSplit[i]);
				}

				ColumDefinition coldef = new ColumDefinition();
				coldef.setMapp(map);
				coldef.setProperty(prop);
				coldef.setColname(column.getColumnName());
				coldef.setSqldataType(dataType);
				coldef.setColum(column);
				coldef.setLanguage(lang);
				coldef.setLanguageColumn(langColumn);
				coldef.setDatatype(literalType);
				map.addColDefinition(coldef);

			} else {

				String[] relation = elementSplit[i].split("->");
				prop = config.getNameSpaceConfig().resolveToProperty(
						relation[0]);
				String joinswith = null;
				String uritemplate = null;
				if (relation.length > 1) {
					if (relation[1].startsWith("<")) {
						uritemplate = relation[1].substring(1,
								relation[1].length() - 1);
					} else {
						joinswith = relation[1];
					}
				}

				ColumDefinition coldef = new ColumDefinition();
				coldef.setMapp(map);
				coldef.setProperty(prop);
				coldef.setColname(column.getColumnName());
				coldef.setSqldataType(dataType);
				coldef.setColum(column);
				coldef.setJoinsWith(joinswith);
				coldef.setLanguage(lang);
				coldef.setLanguageColumn(langColumn);
				coldef.setDatatype(literalType);
				if(uritemplate!=null){
					coldef.setUriTemplate(uritemplate);
				}
				
				map.addColDefinition(coldef);
			}

			// }

		}
		
		
						
		

		config.getMappingConfiguration().add(map);

	}

	private void parseNamespace(String element) throws MalformedURLException {

		// split into prefix and ns
		String[] pn = splitAndClean(element);
		config.getNameSpaceConfig().putNamespaceMapping(pn[0], pn[1]);

	}

	private void parseConnection(String config) {

		String[] dbSplits = splitAndClean(config);

		DBConnectionConfiguration conn = new DBConnectionConfiguration(
				dbSplits[0], dbSplits[1], dbSplits[2]);
		this.config.setDbConn(conn);

		// we also create us a connection instance. For throwaway
		this.sqlfacade = new SQLAccessFacade(conn);

	}
	
	public static List<SelectItem> extractSelectItems(SelectBody selectBody){
		final List<SelectItem> selectItems = new ArrayList<SelectItem>();
		selectBody.accept(new SelectVisitor() {

			@Override
			public void visit(Union arg0) {
				// TODO Auto-generated method stub

			}

			@Override
			public void visit(PlainSelect plainSelect) {
				selectItems.addAll(plainSelect.getSelectItems());

			}
		});

		return selectItems;
		
	}

}
