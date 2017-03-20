package stormBench.stormBench.utils;


import java.io.IOException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * @author Roland
 *
 */
public class XmlTopologyConfigParser {
	
	/*Launch file parameters*/
	private String filename;
	private final DocumentBuilderFactory factory;
	private final DocumentBuilder builder;
	private final Document document;
	
	/*Storm execution parameters*/
	private String topologyId;
	private String sgHost;
	private String sgPort;
	private String nbTasks;
	private String interNbExecutors;
	private String sinkNbExecutors;
	private String interCpuConstraint;
	private String sinkCpuConstraint;
	private String interMemConstraint;
	private String sinkMemConstraint;
	private String dbHost;
	private String windowSize;
	private String windowStep;
	
	public XmlTopologyConfigParser(String filename) throws ParserConfigurationException, SAXException, IOException{
		this.filename = filename;
		this.factory = DocumentBuilderFactory.newInstance();
		this.builder = factory.newDocumentBuilder();
		this.document = builder.parse(this.getFilename());
	}
	
	/**
	 * 
	 * @return the path of the current xml file
	 */
	public String getFilename() {
		return this.filename;
	}

	/**
	 * 
	 * @param filename the new path of the xml file to parse
	 */
	public void setFilename(String filename) {
		this.filename = filename;
	}

	/**
	 * 
	 * @return the Document (according to W3C norm) corresponding to the current xml file
	 */
	public Document getDocument() {
		return this.document;
	}
	
	/**
	 * @return the topologyId
	 */
	public String getTopId() {
		return topologyId;
	}

	/**
	 * @param topologyId the topologyId to set
	 */
	public void setTopId(String topId) {
		this.topologyId = topId;
	}

	/**
	 * @return the sgPort
	 */
	public String getSgPort() {
		return sgPort;
	}

	/**
	 * @param sgPort the sgPort to set
	 */
	public void setSgPort(String sgPort) {
		this.sgPort = sgPort;
	}

	/**
	 * @return the sgHost
	 */
	public String getSgHost() {
		return sgHost;
	}

	/**
	 * @param sgHost the sgHost to set
	 */
	public void setSgHost(String sgHost) {
		this.sgHost = sgHost;
	}

	/**
	 * @return the nbTasks
	 */
	public String getNbTasks() {
		return nbTasks;
	}

	/**
	 * @param nbTasks the nbTasks to set
	 */
	public void setNbTasks(String nbTasks) {
		this.nbTasks = nbTasks;
	}

	/**
	 * @return the interNbExecutors
	 */
	public String getInterNbExecutors() {
		return interNbExecutors;
	}

	/**
	 * @param nbExecutors the nbExecutors to set
	 */
	public void setInterNbExecutors(String nbExecutors) {
		this.interNbExecutors = nbExecutors;
	}

	/**
	 * @return the sinkNbExecutors
	 */
	public String getSinkNbExecutors() {
		return sinkNbExecutors;
	}

	/**
	 * @param nbExecutors the nbExecutors to set
	 */
	public void setSinkNbExecutors(String nbExecutors) {
		this.sinkNbExecutors = nbExecutors;
	}
	
	/**
	 * @return the interCpuConstraint
	 */
	public String getInterCpuConstraint() {
		return interCpuConstraint;
	}

	/**
	 * @param interCpuConstraint the interCpuConstraint to set
	 */
	public void setInterCpuConstraint(String interCpuConstraint) {
		this.interCpuConstraint = interCpuConstraint;
	}

	/**
	 * @return the sinkCpuConstraint
	 */
	public String getSinkCpuConstraint() {
		return sinkCpuConstraint;
	}

	/**
	 * @param sinkCpuConstraint the sinkCpuConstraint to set
	 */
	public void setSinkCpuConstraint(String sinkCpuConstraint) {
		this.sinkCpuConstraint = sinkCpuConstraint;
	}

	/**
	 * @return the interMemConstraint
	 */
	public String getInterMemConstraint() {
		return interMemConstraint;
	}

	/**
	 * @param interMemConstraint the interMemConstraint to set
	 */
	public void setInterMemConstraint(String interMemConstraint) {
		this.interMemConstraint = interMemConstraint;
	}

	/**
	 * @return the sinkMemConstraint
	 */
	public String getSinkMemConstraint() {
		return sinkMemConstraint;
	}

	/**
	 * @param sinkMemConstraint the sinkMemConstraint to set
	 */
	public void setSinkMemConstraint(String sinkMemConstraint) {
		this.sinkMemConstraint = sinkMemConstraint;
	}

	public String getStateHost(){
		return this.dbHost;
	}
	
	public void setDbHost(String dbHost){
		this.dbHost = dbHost;
	}

	/**
	 * @return the windowSize
	 */
	public String getWindowSize() {
		return windowSize;
	}

	/**
	 * @param windowSize the windowSize to set
	 */
	public void setWindowSize(String windowSize) {
		this.windowSize = windowSize;
	}

	/**
	 * @return the windowStep
	 */
	public String getWindowStep() {
		return windowStep;
	}

	/**
	 * @param windowStep the windowStep to set
	 */
	public void setWindowStep(String windowStep) {
		this.windowStep = windowStep;
	}
	
	public void initParameters() {
		Document doc = this.getDocument();
		final Element parameters = (Element) doc.getElementsByTagName(TopologyConfigNodeNames.PARAMETERS.toString()).item(0);
		final NodeList name = parameters.getElementsByTagName(TopologyConfigNodeNames.TOPID.toString());
		this.setTopId(name.item(0).getTextContent());
		final NodeList sgport = parameters.getElementsByTagName(TopologyConfigNodeNames.SGPORT.toString());
		this.setSgPort(sgport.item(0).getTextContent());
		final NodeList sghost = parameters.getElementsByTagName(TopologyConfigNodeNames.SGHOST.toString());
		this.setSgHost(sghost.item(0).getTextContent());
		final NodeList nbtasks = parameters.getElementsByTagName(TopologyConfigNodeNames.NBTASKS.toString());
		this.setNbTasks(nbtasks.item(0).getTextContent());
		final NodeList interNbexecutors = parameters.getElementsByTagName(TopologyConfigNodeNames.INTERNBEXECS.toString());
		this.setInterNbExecutors(interNbexecutors.item(0).getTextContent());
		final NodeList sinkNbexecutors = parameters.getElementsByTagName(TopologyConfigNodeNames.SINKNBEXECS.toString());
		this.setSinkNbExecutors(sinkNbexecutors.item(0).getTextContent());
		final NodeList interCpuConstraint = parameters.getElementsByTagName(TopologyConfigNodeNames.INTERCPU.toString());
		this.setInterCpuConstraint(interCpuConstraint.item(0).getTextContent());
		final NodeList sinkCpuConstraint = parameters.getElementsByTagName(TopologyConfigNodeNames.SINKCPU.toString());
		this.setSinkCpuConstraint(sinkCpuConstraint.item(0).getTextContent());
		final NodeList interMemConstraint = parameters.getElementsByTagName(TopologyConfigNodeNames.INTERMEM.toString());
		this.setInterMemConstraint(interMemConstraint.item(0).getTextContent());
		final NodeList sinkMemConstraint = parameters.getElementsByTagName(TopologyConfigNodeNames.SINKMEM.toString());
		this.setSinkMemConstraint(sinkMemConstraint.item(0).getTextContent());
		final NodeList dbhost = parameters.getElementsByTagName(TopologyConfigNodeNames.STATEHOST.toString());
		this.setDbHost(dbhost.item(0).getTextContent());
		final NodeList size = parameters.getElementsByTagName(TopologyConfigNodeNames.SIZE.toString());
		this.setWindowSize(size.item(0).getTextContent());
		final NodeList step = parameters.getElementsByTagName(TopologyConfigNodeNames.STEP.toString());
		this.setWindowStep(step.item(0).getTextContent());
	}
}