package org.aksw.sparqlmap.config.syntax;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.FromItem;

import org.aksw.sparqlmap.mapper.subquerymapper.algebra.FilterUtil;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.hp.hpl.jena.rdf.model.Resource;

public class Mapping implements Cloneable{
	
	private FromItem fromPart; 
	private String name;
	private Resource typeOf;
	private String idColString;
	private Column idCol;
	private List<ColumDefinition> colDefinitions = new ArrayList<ColumDefinition>();
	private Multimap<String, ColumDefinition> prop2Col = HashMultimap.create();
	private BiMap<String, ColumDefinition> name2col = HashBiMap.create();
	private String ldtemplate;

	
	public FromItem getFromPart() {
		return fromPart;
	}
	protected void setFromPart(FromItem fromPart) {
		this.fromPart = fromPart;
	}
	
	public String getName() {
		return name;
	}
	public List<ColumDefinition> getColDefinitions() {
		return colDefinitions;
	}

	public void addColDefinition(ColumDefinition colDefinition) {
		this.colDefinitions.add(colDefinition);
		
			prop2Col.put(colDefinition.getProperty(), colDefinition);
			name2col.put(colDefinition.getColname(), colDefinition);
		
	}

	
	protected String getLdtemplate() {
		return ldtemplate;
	}

	public void setLdtemplate(String ldtemplate) {
		this.ldtemplate = ldtemplate;
		
		
	}

	

	

	public Resource getTypeOf() {
		return typeOf;
	}

	public void setTypeOf(Resource typeOf) {
		this.typeOf = typeOf;
	}

	
	public Collection<ColumDefinition> getColumDefinition(String prop){
		return prop2Col.get(prop);
	}
	
	public ColumDefinition getColumnDefinition(String name){
		return name2col.get(name);
	}
	
	public ColumDefinition getIdColumn(){
		ColumDefinition id = null;
		for (ColumDefinition coldef : this.colDefinitions) {
			if(coldef.getColname().equals(idColString)){
				id = coldef;
				break;
			}
		}
		return id;
	}
	public void setId(String idColString){
		this.idColString = idColString;
		this.idCol = FilterUtil.createColumn(fromPart.getAlias(), idColString);
		this.idCol.setColumnName(idColString);

	}
	/**
	 * returns the id col of the underlying mapping.
	 * TODO implement multi-col support here.
	 * @return
	 */
	public Column getMappingIdCol() {
		return idCol;
	}
	

	public void setName(String name) {
		this.name = name;
		
	}
	
	
	
//	/**
//	 * clones this mapping and changes the alias of the underlying sql from
//	 * @param suffix
//	 * @return
//	 * @throws CloneNotSupportedException 
//	 */
//	public Mapping clone(final String suffix) {
//		final Mapping copy;
//		try {
//			copy = (Mapping) this.clone();
//		} catch (CloneNotSupportedException e1) {
//			throw new ImplementationException("this should not happen");
//		}
//			
//		this.fromPart.accept(new FromItemVisitor() {
//			
//			@Override
//			public void visit(SubJoin subjoin) {
//				SubJoin fItem = new SubJoin();
//				fItem.setAlias(subjoin.getAlias() + suffix);
//				fItem.setJoin(subjoin.getJoin());
//				fItem.setLeft(subjoin.getLeft());
//				copy.setFromPart(fItem);
//			}
//			
//			@Override
//			public void visit(SubSelect subSelect) {
//				throw new ImplementationException("clone of subselect view not supported yet.");
//				
//			}
//			
//			@Override
//			public void visit(Table tableName) {
//				FromItem fItem = new Table(tableName.getSchemaName(),tableName.getName());
//				fItem.setAlias(tableName.getAlias() + suffix);
//				copy.setFromPart(fItem);
//				
//			}
//		});
//		copy.colDefinitions = new ArrayList<ColumDefinition>();
//		copy.name2col = HashBiMap.create();
//		copy.prop2Col = HashMultimap.create();
//		
//		for(ColumDefinition coldef: this.colDefinitions){
//			ColumDefinition copycol;
//			try {
//				copycol = (ColumDefinition) coldef.clone(suffix);
//				copycol.setMapp(copy);
//			} catch (CloneNotSupportedException e) {
//				throw new ImplementationException("Clonenotsupported should never be thrown");
//			}
//			
//			copy.addColDefinition(copycol);
//		}
//		
//		return copy;
//		
//		
//		
//	}
	
	
	@Override
	public String toString() {
		
		return "Mapping of: " + fromPart.toString();
	}
	
	
	
	@Override
	public Object clone() throws CloneNotSupportedException {
		return super.clone();
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((colDefinitions == null) ? 0 : colDefinitions.hashCode());
		result = prime * result
				+ ((fromPart == null) ? 0 : fromPart.hashCode());
		result = prime * result + ((idColString == null) ? 0 : idColString.hashCode());
		result = prime * result
				+ ((ldtemplate == null) ? 0 : ldtemplate.hashCode());
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		result = prime * result
				+ ((name2col == null) ? 0 : name2col.hashCode());
		result = prime * result
				+ ((prop2Col == null) ? 0 : prop2Col.hashCode());
		result = prime * result + ((typeOf == null) ? 0 : typeOf.hashCode());
		return result;
	}
	
	
	
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Mapping other = (Mapping) obj;
		if (colDefinitions == null) {
			if (other.colDefinitions != null)
				return false;
		} else if (!colDefinitions.equals(other.colDefinitions))
			return false;
		if (fromPart == null) {
			if (other.fromPart != null)
				return false;
		} else if (!fromPart.equals(other.fromPart))
			return false;
		if (idColString == null) {
			if (other.idColString != null)
				return false;
		} else if (!idColString.equals(other.idColString))
			return false;
		if (ldtemplate == null) {
			if (other.ldtemplate != null)
				return false;
		} else if (!ldtemplate.equals(other.ldtemplate))
			return false;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		if (name2col == null) {
			if (other.name2col != null)
				return false;
		} else if (!name2col.equals(other.name2col))
			return false;
		if (prop2Col == null) {
			if (other.prop2Col != null)
				return false;
		} else if (!prop2Col.equals(other.prop2Col))
			return false;
		if (typeOf == null) {
			if (other.typeOf != null)
				return false;
		} else if (!typeOf.equals(other.typeOf))
			return false;
		return true;
	}
	
	
	

	
	
	
	
	

	

}
