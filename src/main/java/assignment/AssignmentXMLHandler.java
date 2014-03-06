package assignment;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeSet;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;
import javax.xml.XMLConstants;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import utils.CustomSamplersException;

/**
 * This static utility class handles the Assignment XML configuration file parsing.
 * */
public class AssignmentXMLHandler {

	/**
	 * This utility function is used by the parse function to accumulate new entries
	 * that are read from the configuration XML file.
	 * */
	private static void addAssignment(Integer threadId, SortedSet<String> payloadSet,
			HashMap<Integer, SortedSet<String> > assignMap) {
		if (!assignMap.containsKey(threadId)) {
			// Create as new.
			assignMap.put(threadId, new TreeSet<String>(payloadSet));
		} else {
			// Accumulate.
			assignMap.get(threadId).addAll(payloadSet);
		}
	}

	/**
	 * This is the parser function of the class, that parses the file, it got as a parameter.
	 * First of all, it validates it against the assignment.xsd file that is stored in the assignment
	 * package. If the validation passed, the parsing results in the assignment map.
	 * */
	public static HashMap<Integer, SortedSet<String> > parse(String assignmentFilePath)
			throws CustomSamplersException {
		HashMap<Integer, SortedSet<String> > result = new HashMap<Integer, SortedSet<String>>();
		Document doc = null;
		try {
			File asFile = new File(assignmentFilePath);
			DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
			dbFactory.setNamespaceAware(true);
			DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
			doc = dBuilder.parse(asFile);
			InputStream xsdIS = Thread.currentThread()
					.getContextClassLoader().getResourceAsStream("assignment/assignment.xsd");
			Schema schema = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI)
					.newSchema(new StreamSource(xsdIS));
			Validator validator = schema.newValidator();
			validator.validate(new DOMSource(doc));
		} catch (SAXException e) {
			throw new CustomSamplersException("Validation exception occured!", e);
		} catch (IOException e) {
			throw new CustomSamplersException("IOException occured!", e);
		} catch (ParserConfigurationException e) {
			throw new CustomSamplersException("ParserConfigurationException occured!", e);
		}

		try {
			/** First, it parses the {@literal <}payload{@literal >} DOM elements. */
			doc.getDocumentElement().normalize();
			NodeList nList = doc.getElementsByTagName("payload");
			for (int temp = 0; temp < nList.getLength(); temp++) {
				Node nNode = nList.item(temp);
				if (nNode.getNodeType() == Node.ELEMENT_NODE) {
					Element eElement = (Element) nNode;
					SortedSet<String> plSet = new TreeSet<String>();
					plSet.add(eElement.getAttribute("name"));
					NodeList nl = eElement.getElementsByTagName("threadIDList");
					if (nl.getLength() != 0) {
						for (int i = 0; i < nl.getLength(); ++i) {
							String[] idsStr = nl.item(i).getTextContent().trim().split("\\s+");
							for (int j = 0; j < idsStr.length; ++j) {
								addAssignment(Integer.valueOf(idsStr[j]), plSet, result);
							}
						}
					}
					nl = eElement.getElementsByTagName("threadIDRange");
					if (nl.getLength() != 0) {
						for (int i = 0; i < nl.getLength(); ++i) {
							Element idR = (Element) nl.item(i);
							int from = Integer.valueOf(idR.getAttribute("from"));
							int to = Integer.valueOf(idR.getAttribute("to"));
							for (int j = from; j <= to; ++j) addAssignment(j, plSet, result);
						}
					}
				}
			}

			/** Finally, it parses the {@literal <}threads{@literal >} DOM elements. */
			nList = doc.getElementsByTagName("threads");
			for (int temp = 0; temp < nList.getLength(); temp++) {
				Element nEl = (Element) nList.item(temp);
				SortedSet<String> pls = new TreeSet<String>();
				NodeList plsNL = nEl.getElementsByTagName("payloadList");
				for (int i = 0; i < plsNL.getLength(); ++i) {
					pls.addAll(Arrays.asList(plsNL.item(i).getTextContent().trim().split("\\s+")));
				}
				Node idListAtt = nEl.getAttributeNode("idList");
				Node idRangeAtt = nEl.getAttributeNode("idRange");
				if (idListAtt != null) {
					String[] idsStr = idListAtt.getTextContent().trim().split("\\s+");
					for (int i = 0; i < idsStr.length; ++i) {
						addAssignment(Integer.valueOf(idsStr[i]), pls, result);
					}
				}
				if (idRangeAtt != null) {
					String[] range = idRangeAtt.getTextContent().split("-");
					int from = Integer.valueOf(range[0]);
					int to = Integer.valueOf(range[1]);
					for (int i = from; i <= to; ++i) {
						addAssignment(i, pls, result);
					}
				}
				if (idListAtt == null && idRangeAtt == null) {
					System.out.println(" -> Neither idList or idRange was found! Is this intentional?");
				}
			}

		} catch (Exception e) {
			throw new CustomSamplersException("Could not parse the input Assignment XML file!", e);
		}
		return result;
	}

	/**
	 * This is the builder function of the class, that creates an XML file with the name as it got
	 * as a parameter. The content of the output XML file is based on the assignment.xsd schema, and
	 * contains the entries of the assignmentMap.
	 * 
	 * @param  outputAssignFilePath  the file to build
	 * @param  assignmentMap  the target content of the file
	 * */
	public static void build(String outputAssignFilePath,
			HashMap<Integer, SortedSet<String>> assignmentMap) 
					throws CustomSamplersException {
		DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
		try {
			DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
			Document doc = docBuilder.newDocument();
			Element rootElement = doc.createElement("assignment");
			doc.appendChild(rootElement);
			for (Entry<Integer, SortedSet<String>> entry : assignmentMap.entrySet()) {
				Element threads = doc.createElement("threads");
				rootElement.appendChild(threads);
				threads.setAttribute("idList", entry.getKey().toString());
				Element payloads = doc.createElement("payloadList");
				payloads.appendChild(doc.createTextNode(entry.getValue().first()));
				threads.appendChild(payloads);
			}

			// write the content into xml file
			TransformerFactory transformerFactory = TransformerFactory.newInstance();
			Transformer transformer = transformerFactory.newTransformer();
			transformer.setOutputProperty(OutputKeys.INDENT, "yes");
			transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");
			DOMSource source = new DOMSource(doc);
			StreamResult result = new StreamResult(new File(outputAssignFilePath));
			transformer.transform(source, result);

			System.out.println("File saved!");
		} catch (ParserConfigurationException e) {
			throw new CustomSamplersException("Could not create the output Assignment XML file!", e);
		} catch (TransformerConfigurationException e) {
			throw new CustomSamplersException("Could not create the output Assignment XML file!", e);
		} catch (TransformerException e) {
			throw new CustomSamplersException("Could not create the output Assignment XML file!", e);
		}
	}

}
