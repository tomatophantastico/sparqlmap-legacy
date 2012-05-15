package org.aksw.sparqlmap.config.syntax;

import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;

public class ColumDefinition implements Cloneable{
	
	
	
	private TermCreator termCreator;
	
	private String linkedDataPath;
	
	private Pattern ldPattern;
	
	
	private Integer sqldataType;

	private String uriTemplate;
	
	private Mapping mapp;
	
	private String colname;
	
	private String property; 
	
	private Locale locale;
	
	private String type;
	
	protected Column colum;
	
	private String joinsWith;
	
	private List<Expression> terms;
	
	
	
	/**
	 * a pattern matching the resource pattern
	 * @return
	 */
	public Pattern getLdPattern() {
		return ldPattern;
	}
	
	public void setLdPattern(Pattern ldPattern) {
		this.ldPattern = ldPattern;
	}
	
	/**
	 * the basic structure of the resource creation string. colsnames are replaced
	 * @return
	 */
	public String getLinkedDataPath() {
		return linkedDataPath;
	}
	
	public void setLinkedDataPath(String linkedDataPath) {
		this.linkedDataPath = linkedDataPath;
	}
	
	public List<Expression> getTerms() {
		return terms;
	}
	
	public TermCreator getTermCreator() {
		return termCreator;
	}
	
	protected void setTermCreator(TermCreator termCreator) {
		this.termCreator = termCreator;
	}
	
	
	 
	public void setSqldataType(Integer sqldataType) {
		this.sqldataType = sqldataType;
	}
	
	public void setUriTemplate(String uriTemplate) {
		String ld = uriTemplate;
		if(!ld.contains("{")){
			ld += "{"+mapp.getName() + "." +colname+"}";
		}
		this.uriTemplate = ld;
		
		this.linkedDataPath = ld.replaceAll("\\{.*?\\}", "*");
		
		String pattern = "^\\Q" + ld.replaceAll("\\{.*?\\}", "\\\\E.*?\\\\Q") + "\\E$";  
		this.ldPattern  = Pattern.compile(pattern);		 
	}
	
	public void setJoinsWith(String joinsWith) {
		this.joinsWith = joinsWith;
	}
	
	

	public void setColum(Column colum) {
		this.colum = colum;
	}
	
	public void setMapp(Mapping mapp) {
		this.mapp = mapp;
	}

	
	

	public String getColname() {
		return colname;
	}

	public void setColname(String colname) {
		this.colname = colname;
	}

	public Locale getLocale() {
		return locale;
	}

	public void setLocale(Locale locale) {
		this.locale = locale;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}
	
	public String getProperty() {
		return property;
	}
	
	public void setProperty(String property) {
		this.property = property;
	}
	
	public Mapping getMapp() {
		return mapp;
	}
	protected Column getColumn(){
		return colum;
	}
	
	

	
	
	
	
	public Integer getSqldataType() {
		return sqldataType;
	}
	
	
	/**
	 * the exact construction pattern of the uri
	 * @return
	 */
	public String getUriTemplate() {
		return uriTemplate;
	}
	
	
	public String getJoinsAlias() {
		return joinsWith;
	}
	
	
	
	
	public boolean isIdColumn(){
		boolean isId = false;
		
		if(mapp.getIdColumn().colname.equals(colname)){
			isId= true;
			}
		
		return isId;
	}
	
	
	
	protected boolean isResource(){
		if(terms == null || terms.size()==0){
			return false;
		}else{
			return true;
		}
	}
	


	
	
	@Override
	public String toString() {
		
		if(termCreator ==null){
			return mapp.getName() + "."+ colname;
		}
		
		return termCreator.toString();
		
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((colname == null) ? 0 : colname.hashCode());
		result = prime * result + ((colum == null) ? 0 : colum.hashCode());
		result = prime * result
				+ ((joinsWith == null) ? 0 : joinsWith.hashCode());
		result = prime * result
				+ ((linkedDataPath == null) ? 0 : linkedDataPath.hashCode());
		result = prime * result + ((locale == null) ? 0 : locale.hashCode());
		result = prime * result
				+ ((property == null) ? 0 : property.hashCode());
		result = prime * result
				+ ((terms == null) ? 0 : terms.hashCode());
		result = prime * result
				+ ((sqldataType == null) ? 0 : sqldataType.hashCode());
		result = prime * result + ((type == null) ? 0 : type.hashCode());
		result = prime * result
				+ ((uriTemplate == null) ? 0 : uriTemplate.hashCode());
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
		ColumDefinition other = (ColumDefinition) obj;
		if (colname == null) {
			if (other.colname != null)
				return false;
		} else if (!colname.equals(other.colname))
			return false;
		if (colum == null) {
			if (other.colum != null)
				return false;
		} else if (!colum.equals(other.colum))
			return false;
		if (joinsWith == null) {
			if (other.joinsWith != null)
				return false;
		} else if (!joinsWith.equals(other.joinsWith))
			return false;
		if (linkedDataPath == null) {
			if (other.linkedDataPath != null)
				return false;
		} else if (!linkedDataPath.equals(other.linkedDataPath))
			return false;
		if (locale == null) {
			if (other.locale != null)
				return false;
		} else if (!locale.equals(other.locale))
			return false;
		if (property == null) {
			if (other.property != null)
				return false;
		} else if (!property.equals(other.property))
			return false;
		if (terms == null) {
			if (other.terms != null)
				return false;
		} else if (!terms.equals(other.terms))
			return false;
		if (sqldataType == null) {
			if (other.sqldataType != null)
				return false;
		} else if (!sqldataType.equals(other.sqldataType))
			return false;
		if (type == null) {
			if (other.type != null)
				return false;
		} else if (!type.equals(other.type))
			return false;
		if (uriTemplate == null) {
			if (other.uriTemplate != null)
				return false;
		} else if (!uriTemplate.equals(other.uriTemplate))
			return false;
		return true;
	}

	

	
	



}
