package org.aksw.sparqlmap.config.syntax;

public class R2RConfiguration {

	private String endpoint;

	private MappingConfiguration mappingConfiguration = new MappingConfiguration(this);

	private NameSpaceConfig nameSpaceConfig = new NameSpaceConfig();

	private DBConnectionConfiguration dbConn;
	
	private boolean useSolutionModifierPushing = true;
	
	public boolean isUseSolutionModifierPushing() {
		return useSolutionModifierPushing;
	}
	
	public void setUseSolutionModifierPushing(boolean useSolutionModifierPushing) {
		this.useSolutionModifierPushing = useSolutionModifierPushing;
	}

	public String getEndpoint() {
		return endpoint;
	}

	public void setEndpoint(String endpoint) {
		this.endpoint = endpoint;
	}

	public DBConnectionConfiguration getDbConn() {
		return dbConn;
	}

	public void setDbConn(DBConnectionConfiguration dbConn) {
		this.dbConn = dbConn;
	}

	public MappingConfiguration getMappingConfiguration() {
		return mappingConfiguration;
	}

	public void setMappingConfiguration(
			MappingConfiguration mappingConfiguration) {
		this.mappingConfiguration = mappingConfiguration;
	}

	public NameSpaceConfig getNameSpaceConfig() {
		return nameSpaceConfig;
	}

	public void setNameSpaceConfig(NameSpaceConfig nsconvert) {
		this.nameSpaceConfig = nsconvert;
	}

}
