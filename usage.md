1. 编译插件并安装插件
cd /home/zhangqiang/code/postgres/contrib/pgvectorscale/pgvectorscale
cargo pgrx init --pg17=$(which pg_config)
-- 下面是 release 版本，没有详细调式信息
RUSTFLAGS="-C target-feature=+avx2,+fma" cargo pgrx install --features "pg17,build_parallel" --release
-- 下面是 debug 版本，有详细调式信息
RUSTFLAGS="-C target-feature=+avx2,+fma" cargo pgrx install --features "pg17,build_parallel"

2. 重启实例
cd /home/zhangqiang/code/postgres
./build.sh restart

3. 连接实例
cd /home/zhangqiang/code/postgres
./build.sh connect 31104

4. 实例数据目录
/home/zhangqiang/code/postgres/TestDir/mydb

5. 实例日志目录
/home/zhangqiang/code/postgres/TestDir/mydb/logfile

6. 初始化实例
cd /home/zhangqiang/code/postgres
./build.sh init

7. vectorscale 插件依赖 pgvector，因此创建插件之前需要先创建 pgvector 插件
CREATE EXTENSION IF NOT EXISTS vector;

8. 登陆实例密码
Wp9(testh214#54%

9. 使用 psql 来运行指定 sql 文件
cd /home/zhangqiang/code/postgres && ./install/bin/psql -h localhost -p 31104 -U $USER -d postgres -c "CREATE EXTENSION IF NOT EXISTS vector;"
psql -h localhost -p 31104 -U postgres -d mydb -f /home/zhangqiang/code/postgres/contrib/pgvectorscale/usage.sql

10. 对应的 sql 测试
在 contrib/pgvectorscale/*.sql

11. vectorbench 测试
 a. 进入 vectorbench 目录
  cd /data/zhangqiang/code/VectorDBBench
 b. bench 环境初始化
  source .env
  source /data/zhangqiang/code/vectordbbench_install_dependency/python3.11/bin/ann-env/bin/activate

 c. 运行测试
 -- 重新导入数据测试，将 /home/zhangqiang/code/VectorDBBench/vectordb_bench/config-files/pgvectorscale_diskann_config.yml 中的 drop_old 与 load 设置为 True
 vectordbbench pgvectorscalediskann --config-file pgvectorscale_diskann_config.yml --num-concurrency=20
 
 -- 不用导入数据测试，将 /home/zhangqiang/code/VectorDBBench/vectordb_bench/config-files/pgvectorscale_diskann_config.yml 中的 drop_old 与 load 设置为 False
 vectordbbench pgvectorscalediskann --config-file pgvectorscale_diskann_config.yml --num-concurrency=20

 d. 查看 index 大小
 select * from pg_indexes_size('pgvectorscale_index');

12. 手动创建索引
drop index if exists pgvectorscale_index;

-- 没有开启 cluster 时，创建索引
-- 可以使用 \timing on 打开 sql 语句执行时间
drop index if exists pgvectorscale_index;
set diskann.parallel_flush_interval=0.1;
set diskann.force_parallel_workers=8;
SET diskann.num_clusters = 1;
SET diskann.build_parallel = off;
CREATE INDEX IF NOT EXISTS  "pgvectorscale_index"  ON public. "pg_vectorscale_collection" 
USING  "diskann"  (embedding  "vector_cosine_ops" )
WITH ( "storage_layout" = "memory_optimized", "num_neighbors" = "50", "search_list_size" = "120", "max_alpha" = "1.2", "num_dimensions" = "0", "num_bits_per_dimension" = "2" );

-- 开启 cluster 创建索引
-- 可以使用 \timing on 打开 sql 语句执行时间
drop index if exists pgvectorscale_index;
set diskann.parallel_flush_interval=0.1;
SET diskann.num_clusters = 8;
SET diskann.clustering_max_sample_size = 10000;
SET diskann.clustering_sample_threshold = 10000;
SET diskann.max_workers = 8;
SET diskann.build_parallel = on;
CREATE INDEX IF NOT EXISTS  "pgvectorscale_index"  ON public. "pg_vectorscale_collection" 
USING  "diskann"  (embedding  "vector_cosine_ops" )
WITH ( "storage_layout" = "memory_optimized", "num_neighbors" = "50", "search_list_size" = "120", "max_alpha" = "1.2", "num_dimensions" = "0", "num_bits_per_dimension" = "2" );