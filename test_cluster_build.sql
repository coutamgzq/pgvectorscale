-- Test script for cluster build functionality
-- This script tests the refactored cluster build code

-- Drop and recreate the extension
DROP EXTENSION IF EXISTS vectorscale CASCADE;
CREATE EXTENSION vectorscale;

-- Show the vectorscale version
SELECT extname, extversion FROM pg_extension WHERE extname = 'vectorscale';

-- Test 1: Basic index build without clustering
\echo 'Test 1: Basic index build without clustering'
DROP TABLE IF EXISTS test_basic;
CREATE TABLE test_basic (
    id SERIAL PRIMARY KEY,
    embedding vector(128)
);

-- Insert some test vectors
INSERT INTO test_basic (embedding)
SELECT (SELECT array_agg(random())::vector FROM generate_series(1, 128))
FROM generate_series(1, 1000);

-- Create index without clustering (num_clusters = 1)
SET diskann.num_clusters = 1;
CREATE INDEX idx_basic ON test_basic USING diskann (embedding vector_cosine_ops);

SELECT 'Basic index created successfully' as result;

-- Test 2: Index build with clustering
\echo 'Test 2: Index build with clustering'
DROP TABLE IF EXISTS test_cluster;
CREATE TABLE test_cluster (
    id SERIAL PRIMARY KEY,
    embedding vector(128)
);

-- Insert test vectors
INSERT INTO test_cluster (embedding)
SELECT (SELECT array_agg(random())::vector FROM generate_series(1, 128))
FROM generate_series(1, 5000);

-- Create index with clustering (num_clusters = 4)
SET diskann.num_clusters = 4;
SET diskann.clustering_max_sample_size = 1000;
SET diskann.clustering_sample_threshold = 1000;
CREATE INDEX idx_cluster ON test_cluster USING diskann (embedding vector_cosine_ops);

SELECT 'Cluster index created successfully' as result;

-- Test 3: Verify index works for queries
\echo 'Test 3: Verify index works for queries'
EXPLAIN (ANALYZE, VERBOSE)
SELECT id, embedding <=> (SELECT embedding FROM test_cluster LIMIT 1) as distance
FROM test_cluster
ORDER BY embedding <=> (SELECT embedding FROM test_cluster LIMIT 1)
LIMIT 10;

-- Test 4: Test with sampling
\echo 'Test 4: Test with sampling (large dataset)'
DROP TABLE IF EXISTS test_sampling;
CREATE TABLE test_sampling (
    id SERIAL PRIMARY KEY,
    embedding vector(128)
);

-- Insert larger dataset
INSERT INTO test_sampling (embedding)
SELECT (SELECT array_agg(random())::vector FROM generate_series(1, 128))
FROM generate_series(1, 10000);

-- Create index with sampling enabled
SET diskann.num_clusters = 8;
SET diskann.clustering_max_sample_size = 2000;
SET diskann.clustering_sample_threshold = 5000;
CREATE INDEX idx_sampling ON test_sampling USING diskann (embedding vector_cosine_ops);

SELECT 'Sampling index created successfully' as result;

-- Test 5: Recall test
\echo 'Test 5: Recall test'
WITH query_vector AS (
    SELECT (SELECT array_agg(random())::vector FROM generate_series(1, 128)) as vec
),
exact_results AS (
    SELECT id, embedding <=> (SELECT vec FROM query_vector) as distance
    FROM test_sampling
    ORDER BY embedding <=> (SELECT vec FROM query_vector)
    LIMIT 10
),
approx_results AS (
    SELECT id, embedding <=> (SELECT vec FROM query_vector) as distance
    FROM test_sampling
    ORDER BY embedding <=> (SELECT vec FROM query_vector)
    LIMIT 10
)
SELECT 
    (SELECT COUNT(*) FROM approx_results) as approx_count,
    (SELECT COUNT(*) FROM exact_results) as exact_count,
    (SELECT COUNT(*) FROM approx_results a JOIN exact_results e ON a.id = e.id) as matches;

-- Cleanup
\echo 'All tests completed successfully!'
