package com.graph.poc.db;

import java.util.Arrays;

import javax.annotation.PostConstruct;

import org.apache.log4j.Logger;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.graph.poc.dto.EdgeIndexVO;
import com.graph.poc.dto.EdgeVO;
import com.graph.poc.dto.PropertyIndexVO;
import com.graph.poc.dto.PropertyVO;
import com.graph.poc.dto.Schema;
import com.thinkaurelius.titan.core.Cardinality;
import com.thinkaurelius.titan.core.EdgeLabel;
import com.thinkaurelius.titan.core.Multiplicity;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.VertexLabel;
import com.thinkaurelius.titan.core.schema.PropertyKeyMaker;
import com.thinkaurelius.titan.core.schema.RelationTypeIndex;
import com.thinkaurelius.titan.core.schema.TitanGraphIndex;
import com.thinkaurelius.titan.core.schema.TitanManagement;

@Component
public class GraphSchemaCreator {
	public static final String INDEX_BACK_END_NAME = "search";
	@Autowired
	private GraphDBInitializer graphDBConfigurator;
	TitanGraph graphDb;
	TitanManagement mgmt;
	@Autowired
	private Schema schema;
	private static final Logger logger = Logger.getLogger(GraphSchemaCreator.class);

	@PostConstruct
	public void init() {
		logger.info("Init");
		graphDb = (TitanGraph) graphDBConfigurator.getGraph();
//		graphDb.tx().onReadWrite(Transaction.READ_WRITE_BEHAVIOR.MANUAL);
		this.mgmt = graphDb.openManagement();
		create(schema);
		this.mgmt.commit();
	}

	private void create(Schema schema) {
		schema.getLabels().forEach(label -> createVertexLabel(label));
		schema.getProperties().forEach(property -> createProperty(property));
		schema.getEdges().forEach(edge -> createEdge(edge));
		schema.getEdgeIndexes().forEach(edgeIndex -> createEdgeIndex(edgeIndex));
		schema.getPropertyIndexes().forEach(index -> createPropertyIndex(index));
	}

	private Object createPropertyIndex(PropertyIndexVO index) {
		TitanGraphIndex resultIndex = null;
		try {
			TitanManagement.IndexBuilder nameIndexBuilder = mgmt.buildIndex(index.getName(), Vertex.class);
			if (index.isUnique()) {
				nameIndexBuilder = nameIndexBuilder.unique();
			}
			for (String key : index.getPropertyKeys()) {
				PropertyKey pKey = getOrCreatePropertyKey(key,null);
				nameIndexBuilder = nameIndexBuilder.addKey(pKey);

			}
			if (index.isComposite()) {
				resultIndex = nameIndexBuilder.buildCompositeIndex();
			} else {
				resultIndex = nameIndexBuilder.buildMixedIndex(INDEX_BACK_END_NAME);
			}
			logger.info("Index created with name :" + resultIndex.name() + ",backingIndex:"
					+ resultIndex.getBackingIndex() + ",indexed-element :" + resultIndex.getIndexedElement() + ",unique:"
					+ resultIndex.isUnique() + ",keys:" + Arrays.asList(resultIndex.getFieldKeys()));
			logger.info("Index :"+resultIndex);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultIndex;
	}

	private PropertyKey getPropertyKey(String key) {
		PropertyKey pKey = mgmt.getPropertyKey(key);
		return pKey;
	}

	private Object createEdgeIndex(EdgeIndexVO edgeIndex) {
		RelationTypeIndex relationTypeIndex = null;
		EdgeLabel edgeLable = null;
		PropertyKey pKey = null;
		try {
			edgeLable = mgmt.getEdgeLabel(edgeIndex.getEdgeLabel());
			pKey = mgmt.getPropertyKey(edgeIndex.getPropertyKey());
			Direction dir = edgeIndex.getDirection() != null ? edgeIndex.getDirection() : Direction.OUT;
			relationTypeIndex = mgmt.buildEdgeIndex(edgeLable, edgeIndex.getName(), dir, pKey);
			logger.info("Edge Index  with name:" + relationTypeIndex.name() + ",sort-keys:"
					+ Arrays.asList(relationTypeIndex.getSortKey()) + ",sort-order:" + relationTypeIndex.getSortOrder()
					+ ",direction:" + relationTypeIndex.getDirection() + ",type:" + relationTypeIndex.getType());
		} catch (IllegalArgumentException e) {
			logger.error(e.getMessage());

			// retrieve already create index
			relationTypeIndex = mgmt.getRelationIndex(edgeLable, edgeIndex.getName());
			if (relationTypeIndex != null) {
				logger.info("Edge Index with name:" + edgeIndex.getName() + " already created ");
				logger.info("Edge Index with name:" + relationTypeIndex.name() + "retrieved, sort-keys "
						+ Arrays.asList(relationTypeIndex.getSortKey()) + ", type" + relationTypeIndex.getType());
			}
		}
		return relationTypeIndex;
	}

	private void createEdge(EdgeVO edge) {
		createEdgeLabel(edge.getLabel(), edge.getMultiplicity());
	}

	private Object createProperty(PropertyVO property) {
		PropertyKey propertyKey = mgmt.getPropertyKey(property.getName());
		if (propertyKey == null) {
			PropertyKeyMaker propertyKeyMaker = mgmt.makePropertyKey(property.getName())
					.dataType(com.graph.poc.util.DataType.getDataTypeClass(property.getDataType()));

			switch (property.getCardinality()) {
			case SINGLE:
				propertyKeyMaker = propertyKeyMaker.cardinality(Cardinality.SINGLE);
				break;
			case LIST:
				propertyKeyMaker = propertyKeyMaker.cardinality(Cardinality.LIST);
				break;
			case SET:
				propertyKeyMaker = propertyKeyMaker.cardinality(Cardinality.SET);
				break;
			default:
				propertyKeyMaker = propertyKeyMaker.cardinality(Cardinality.SINGLE);
				break;
			}

			propertyKey = propertyKeyMaker.make();
			logger.info("Property Key created for field <<" + propertyKey.name() + " >> Data Type : <<"
					+ propertyKey.dataType() + ">> Id : <<" + propertyKey.id() + ">> Cardinality : <<"
					+ propertyKey.cardinality() + ">> Label : <<" + propertyKey.label());

		}
		return propertyKey;
	}

	public void createVertexLabel(String label) {
		VertexLabel vertexLabel = mgmt.getVertexLabel(label);
		if (vertexLabel == null) {
			vertexLabel = mgmt.makeVertexLabel(label).make();
			logger.info("Vertex label created - " + vertexLabel.name());
		}

	}

	public void createEdgeLabel(String label, Multiplicity multiplicity) {
		EdgeLabel edgeLabel = mgmt.getEdgeLabel(label);
		if (edgeLabel == null) {
			if (multiplicity != null) {
				edgeLabel = mgmt.makeEdgeLabel(label).multiplicity(multiplicity).make();
			} else {
				edgeLabel = mgmt.makeEdgeLabel(label).make();
			}
			logger.info("Edge label created name:" + edgeLabel.name() + ",label:" + edgeLabel.label() + ",multiplicity:"
					+ edgeLabel.multiplicity() + ",description:" + edgeLabel.toString());
		}

	}

	public Iterable<TitanGraphIndex> getIndexes() {
		return graphDb.openManagement().getGraphIndexes(Vertex.class);
	}

	public PropertyKey getOrCreatePropertyKey(final String propertyKeyName,final Class<?> propertyType) {

		PropertyKey propertyKey = getPropertyKey(propertyKeyName);
		if (propertyKey == null) {
			Class<?> pType= (propertyType==null)?String.class:propertyType;
			propertyKey = mgmt.makePropertyKey(propertyKeyName).dataType(pType).make();
			logger.info("Property Key:" + propertyKey + "-" + propertyKey.label());
		}
		return propertyKey;
	}

	public void setSchema(Schema schema) {
		this.schema = schema;
	}

}
