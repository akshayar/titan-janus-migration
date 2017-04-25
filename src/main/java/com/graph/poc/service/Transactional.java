package com.graph.poc.service;

public interface Transactional<T> {
	T run();
}

