package com.graph.poc.dao;

import java.util.List;
import java.util.Map;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.graph.poc.dto.Node;
import com.graph.poc.dto.Relationship;

public interface VertexDao {

	Vertex getVertexByUri(String uri);

	Vertex createVertex(Node node);

	List<Vertex> getVerticesFor(Map<String, Object> properties);

	Vertex updateVertex(Node node);

	Vertex getVertexByUriNLabel(String uri, String label);

	Edge createEdge(Vertex fromVertex, Vertex toVertex, Relationship relationShip);

	void deleteEdge(Vertex fromVertex, Vertex toVertex, Relationship relationShip);

	List<Edge> getEdges(Vertex fromVertex, Vertex toVertex);

	List<Edge> getEdges(Vertex fromVertex, String label);

	Edge getEdge(Vertex fromVertex, Vertex toVertex, String label);

	Edge updateEdge(Vertex fromVertex, Vertex toVertex, Relationship relationShip);

}
