-- Recall test for cluster build functionality
-- This script tests the recall rate of the refactored cluster build code

\set ON_ERROR_STOP on

-- Create test database objects
DROP TABLE IF EXISTS test_recall CASCADE;
CREATE TABLE test_recall (
    id SERIAL PRIMARY KEY,
    embedding vector(128)
);

-- Insert test vectors with some structure (clusters in data)
INSERT INTO test_recall (embedding)
SELECT 
    CASE 
        WHEN i % 4 = 0 THEN (SELECT array_agg(0.9 + random() * 0.1)::vector FROM generate_series(1, 64)) || 
                           (SELECT array_agg(random() * 0.1)::vector FROM generate_series(1, 64))
        WHEN i % 4 = 1 THEN (SELECT array_agg(random() * 0.1)::vector FROM generate_series(1, 64)) || 
                           (SELECT array_agg(0.9 + random() * 0.1)::vector FROM generate_series(1, 64))
        WHEN i % 4 = 2 THEN (SELECT array_agg(0.9 + random() * 0.1)::vector FROM generate_series(1, 32)) || 
                           (SELECT array_agg(random() * 0.1)::vector FROM generate_series(1, 32)) ||
                           (SELECT array_agg(0.9 + random() * 0.1)::vector FROM generate_series(1, 32)) ||
                           (SELECT array_agg(random() * 0.1)::vector FROM generate_series(1, 32))
        ELSE (SELECT array_agg(random() * 0.1)::vector FROM generate_series(1, 128))
    END
FROM generate_series(1, 5000) AS i;

-- Test with different cluster configurations
\echo 'Test 1: No clustering (num_clusters = 0 or default)'
SET diskann.num_clusters = 0;
DROP INDEX IF EXISTS idx_recall_1;
CREATE INDEX idx_recall_1 ON test_recall USING diskann (embedding vector_cosine_ops);

\echo 'Test 2: With clustering (num_clusters = 4)'
SET diskann.num_clusters = 4;
SET diskann.clustering_max_sample_size = 1000;
SET diskann.clustering_sample_threshold = 1000;
DROP INDEX IF EXISTS idx_recall_4;
CREATE INDEX idx_recall_4 ON test_recall USING diskann (embedding vector_cosine_ops);

\echo 'Test 3: With clustering (num_clusters = 8)'
SET diskann.num_clusters = 8;
SET diskann.clustering_max_sample_size = 2000;
SET diskann.clustering_sample_threshold = 2000;
DROP INDEX IF EXISTS idx_recall_8;
CREATE INDEX idx_recall_8 ON test_recall USING diskann (embedding vector_cosine_ops);

-- Recall test function
CREATE OR REPLACE FUNCTION test_recall(num_queries int, k int)
RETURNS TABLE (
    avg_recall numeric,
    min_recall int,
    max_recall int,
    total_queries int
) AS $$
DECLARE
    query_vec vector;
    exact_ids int[];
    approx_ids int[];
    recall_sum numeric := 0;
    min_r int := k;
    max_r int := 0;
    matches int;
    i int;
BEGIN
    FOR i IN 1..num_queries LOOP
        -- Generate random query vector
        query_vec := (SELECT array_agg(random())::vector FROM generate_series(1, 128));
        
        -- Get exact results (using seq scan)
        SELECT array_agg(id) INTO exact_ids
        FROM (
            SELECT id
            FROM test_recall
            ORDER BY embedding <=> query_vec
            LIMIT k
        ) sub;
        
        -- Get approximate results using index
        SET LOCAL enable_seqscan = off;
        SELECT array_agg(id) INTO approx_ids
        FROM (
            SELECT id
            FROM test_recall
            ORDER BY embedding <=> query_vec
            LIMIT k
        ) sub;
        RESET enable_seqscan;
        
        -- Count matches
        SELECT COUNT(*) INTO matches
        FROM unnest(exact_ids) e(id)
        JOIN unnest(approx_ids) a(id) ON e.id = a.id;
        
        recall_sum := recall_sum + matches;
        min_r := LEAST(min_r, matches);
        max_r := GREATEST(max_r, matches);
    END LOOP;
    
    RETURN QUERY SELECT 
        round(recall_sum / (num_queries * k) * 100, 2),
        min_r,
        max_r,
        num_queries;
END;
$$ LANGUAGE plpgsql;

-- Run recall tests
\echo 'Running recall tests...'
\echo ''
\echo 'Index without clustering (idx_recall_1):'
SET enable_indexscan = on;
SET enable_seqscan = off;
SELECT * FROM test_recall(100, 10);

\echo ''
\echo 'Index with 4 clusters (idx_recall_4):'
DROP INDEX idx_recall_1;
SELECT * FROM test_recall(100, 10);

\echo ''
\echo 'Index with 8 clusters (idx_recall_8):'
DROP INDEX idx_recall_4;
SELECT * FROM test_recall(100, 10);

-- Cleanup
DROP FUNCTION test_recall(int, int);
DROP TABLE test_recall CASCADE;

\echo ''
\echo 'All recall tests completed!'
