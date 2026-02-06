1. 编译插件
RUSTFLAGS="-C target-feature=+avx2,+fma" cargo pgrx install --features "pg17,build_parallel" --release

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