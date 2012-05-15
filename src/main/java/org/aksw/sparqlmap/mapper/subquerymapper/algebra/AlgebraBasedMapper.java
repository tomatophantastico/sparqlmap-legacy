package org.aksw.sparqlmap.mapper.subquerymapper.algebra;

import java.util.Iterator;

import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.WithItem;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;
import net.sf.jsqlparser.util.deparser.SelectDeParser;

import org.aksw.sparqlmap.beautifier.SparqlBeautifier;
import org.aksw.sparqlmap.config.syntax.MappingConfiguration;
import org.aksw.sparqlmap.config.syntax.R2RConfiguration;
import org.aksw.sparqlmap.db.SQLAccessFacade;
import org.aksw.sparqlmap.mapper.Mapper;
import org.aksw.sparqlmap.mapper.subquerymapper.algebra.finder.MappingFilterFinder;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.OpWalker;

public class AlgebraBasedMapper implements Mapper {
	
	static org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(AlgebraBasedMapper.class);

	private MappingConfiguration mappingConf;

	private SQLAccessFacade conn;
	
	private SparqlBeautifier beautifier = new SparqlBeautifier();

	public AlgebraBasedMapper(R2RConfiguration mainConf) {
		this.mappingConf = mainConf.getMappingConfiguration();
	}
	
	public AlgebraBasedMapper(MappingConfiguration mappingConf){
		this.mappingConf = mappingConf;
	}

	/* (non-Javadoc)
	 * @see org.aksw.r2rj.mapper.Mapper#setConn(org.aksw.r2rj.db.Connector)
	 */
	public void setConn(SQLAccessFacade conn) {
		this.conn = conn;
	}

	/* (non-Javadoc)
	 * @see org.aksw.r2rj.mapper.Mapper#setMappingConf(org.aksw.r2rj.config.syntax.MappingConfiguration)
	 */
	public void setMappingConf(MappingConfiguration mappingConf) {
		this.mappingConf = mappingConf;
	}


	public String rewrite(Query sparql) {
		
		Query origQuery = sparql;

		log.debug(origQuery.toString());
		//first we beautify the Query


		Op op = this.beautifier.compileToBeauty(origQuery); // new  AlgebraGenerator().compile(beautified);
		log.debug(op.toString());
		
		MappingFilterFinder mff = new MappingFilterFinder(mappingConf, op);
		

		
		QueryBuilderVisitor builderVisitor = new QueryBuilderVisitor(mappingConf, mff);
		
		
		OpWalker.walk(op, builderVisitor);
		
		
		// prepare deparse select
		StringBuffer out = new StringBuffer();
		Select select = builderVisitor.getSqlQuery();
		SelectDeParser selectDeParser  = mappingConf.getR2rconf().getDbConn().getSelectDeParser();
		
		selectDeParser.setBuffer(out);
		ExpressionDeParser expressionDeParser =  mappingConf.getR2rconf().getDbConn().getExpressionDeParser(selectDeParser, out);
		selectDeParser.setExpressionVisitor(expressionDeParser);
		if (select.getWithItemsList() != null && !select.getWithItemsList().isEmpty()) {
			out.append("WITH ");
			for (Iterator iter = select.getWithItemsList().iterator(); iter.hasNext();) {
				WithItem withItem = (WithItem)iter.next();
				out.append(withItem);
				if (iter.hasNext())
					out.append(",");
				out.append(" ");
			}
		}
		select.getSelectBody().accept(selectDeParser);
		
		
		
		String sqlResult = out.toString();
		log.debug(sqlResult);
		return sqlResult;
		
	

	}

}
