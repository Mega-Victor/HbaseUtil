该Hbase可以直接打成jar包

```
java -jar *.jar para1 para2
```

里面包括Hbase的增删改查函数, 编辑配置文件改变插入数据的表名,源数据路径, rowkey, 数据列数等配置. jar包会直接读取源数据文件, 按照制定好的rowkey和columnFamily column 插入数据