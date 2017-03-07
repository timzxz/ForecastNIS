
package CommonTools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static CommonTools.ParamLoader.param;

public class HbaseUtil {
    private static Configuration config = null;
    private Connection connection = null;
    private String tableName = null;
    private String familyName = null;
    private List<String> columns = null;
    private List<String> fields = null;

    static {
        config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", param.getProperty("hbase.zookeeper.quorum"));
        config.set("hbase.zookeeper.property.clientPort",
                param.getProperty("hbase.zookeeper.property.clientPort"));
        config.set("hbase.master", param.getProperty("hbase.master"));
        config.set("hbase.root.dir", param.getProperty("hbase.root.dir"));
    }

    public HbaseUtil() {
        setConnection();
    }

    public HbaseUtil(String table) {
        setConnection();
        this.setTableName(table);
    }

    public HbaseUtil table(String t) {
        this.setTableName(t);
        return this;
    }

    public HbaseUtil family(String f) {
        this.setFamilyName(f);
        return this;
    }

    public HbaseUtil columns(List<String> f) {
        this.setColumns(f);
        return this;
    }

    public HbaseUtil(String table, String family) {
        setConnection();
        this.setFamilyName(family);
        this.setTableName(table);
    }

    public HbaseUtil(String table, String family, List<String> columns) {
        setConnection();
        this.setFamilyName(family);
        this.setTableName(table);
        this.setColumns(columns);
    }

    private void setConnection() {
        try {
            this.connection = ConnectionFactory.createConnection(config);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public void setFamilyName(String f) {
        this.familyName = f;
    }

    public void setTableName(String t) {
        this.tableName = t;
    }

    public void setColumns(String... cs) {
        this.columns = new ArrayList<String>();
        for (String c : cs)
            this.columns.add(c);
//        System.out.println("set columns");
    }

    public void setColumns(List<String> cs) {
        this.columns = new ArrayList<String>();
        for (String c : cs)
            this.columns.add(c);
//        System.out.println("set columns");
    }

    public void setFields(List<String> fs) {
        this.fields = new ArrayList<String>();
        for (String f : fs)
            this.fields.add(f);
//        System.out.println("set fields");
    }

    public void close() {
        try {
            if (null != connection)
                this.connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static byte[] Trans(String raw) {
        return Bytes.toBytes(raw);
    }

    public static String Trans(byte[] raw) {
        return Bytes.toString(raw);
    }

    private String generateRowKey() {
        if (null != this.fields) {
            String rowKey = "";
            for (String field : this.fields) {
                rowKey += field;
                rowKey += "_";
            }
            rowKey += UUID.randomUUID().toString().substring(24);
            return rowKey;
        } else {
            System.out.println("Need To Set Fields !!!");
            return null;
        }
    }

    public static String generateRowKey(List<String> fields) {
        String rowKey = "";
        for (String field : fields) {
            rowKey += field;
            rowKey += "_";
        }
        rowKey += UUID.randomUUID().toString().substring(24);
        return rowKey;
    }

    public void replaceInsert(
            String tableName,
            String family,
            String rowKey,
            List<String> column,
            List<String> value) {
        try {
            Table table = this.connection.getTable(TableName.valueOf(tableName));
            Put put = new Put(Trans(rowKey));
            int columnSize = column.size();
            for (int i = 0; i < columnSize; i++)
                put.addColumn(Trans(family), Trans(column.get(i)), Trans(value.get(i)));
            table.put(put);
            table.close();
//            System.out.println("Insert Cell "+rowKey);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void replaceInsert(
            String tableName,
            String family,
            List<String> rowKeys,
            List<String> column,
            List<List<String>> values) {
        try {
            Table table = this.connection.getTable(TableName.valueOf(tableName));
            List<Put> putList = new ArrayList<Put>();
            int columnSize = column.size();
            int i = 0;
            for (List<String> value : values) {
                Put put = new Put(Trans(rowKeys.get(i++)));
                for (int j = 0; j < columnSize; ++j) {
                    put.addColumn(
                            Trans(family),
                            Trans(column.get(j)),
                            Trans(value.get(j)));
                }
                putList.add(put);
            }
            table.put(putList);
            table.close();
//            System.out.println("insert success");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void replaceInsert(String rowKey, List<String> value) {
        if (null != this.tableName && null != this.familyName && null != this.columns)
            try {
                Table table = this.connection.getTable(TableName.valueOf(this.tableName));
                Put put = new Put(Trans(rowKey));
                int columnSize = this.columns.size();
                for (int i = 0; i < columnSize; i++)
                    put.addColumn(
                            Trans(this.familyName),
                            Trans(this.columns.get(i)),
                            Trans(value.get(i)));
                table.put(put);
                table.close();
//                System.out.println("Insert Cell");
            } catch (Exception e) {
                e.printStackTrace();
            }
        else {
            System.out.println("Need To Set tableName, familyName, columns !!!");
        }
    }

    public void replaceInsert(List<String> rowKeys, ArrayList<ArrayList<String>> values) {
        if (null != this.tableName && null != this.familyName && null != this.columns)
            try {
                Table table = this.connection.getTable(TableName.valueOf(this.tableName));
                List<Put> putList = new ArrayList<Put>();
                int columnSize = this.columns.size();
                int i = 0;
                for (List<String> value : values) {
                    Put put = new Put(Trans(rowKeys.get(i++)));
                    for (int j = 0; j < columnSize; ++j) {
                        put.addColumn(
                                Trans(this.familyName),
                                Trans(this.columns.get(j)),
                                Trans(value.get(j)));
                    }
                    putList.add(put);
                }
                table.put(putList);
                table.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        else {
            System.out.println("Need To Set tableName, familyName, columns !!!");
        }
    }

    public void createTable(String table, List<String> familys) {
        try {
            Admin admin = connection.getAdmin();
            if (admin.tableExists(TableName.valueOf(table))) {
                System.out.println("FATAL ERROR: TABLE EXIST!");
            } else {
                HTableDescriptor dscrpt = new HTableDescriptor(TableName.valueOf(table));
                for (String f : familys)
                    dscrpt.addFamily(new HColumnDescriptor(f));
                admin.createTable(dscrpt);
                System.out.println("Create Table: " + table);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

//    用于列名（Qualifier）过滤。
//    Scan scan = new Scan();
//    QualifierFilter filter =
//            new QualifierFilter(
//                    CompareOp.EQUAL, new BinaryComparator(
//                    Bytes.toBytes("my-column"))); // 列名为 my-column
//     scan.setFilter(filter);


    public Result query(String tableName, String rowKey) {
        try {
            Get get = new Get(Trans(rowKey));
            Table table = this.connection.getTable(TableName.valueOf(tableName));// 获取表
            Result result = table.get(get);
            table.close();
            return result;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public ResultScanner query(
            String tableName,
            String startRowKey,
            String stopRowKey) {
        try {
            Table table = this.connection.getTable(TableName.valueOf(tableName));// 获取表
            Scan scan = new Scan();
            scan.setStartRow(Trans(startRowKey));
            scan.setStopRow(Trans(stopRowKey));
            ResultScanner rs = table.getScanner(scan);
            table.close();
            return rs;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public ResultScanner query(
            String tableName,
            String startRowKey,
            String stopRowKey,
            String selectFml,
            String... selectCols) {
        try {
            Table table = this.connection.getTable(TableName.valueOf(tableName));// 获取表
            Scan scan = new Scan();
            scan.setStartRow(Trans(startRowKey));
            scan.setStopRow(Trans(stopRowKey));
            for (String col : selectCols)
                scan.addColumn(Trans(selectFml), Trans(col));
            ResultScanner rs = table.getScanner(scan);
            table.close();
            return rs;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * query with columns filter ! ! ! !
     */
    public ResultScanner query(
            String tableName,
            String startRowKey,
            String stopRowKey,
            String selectFml,
            List<String> selectCols) {
        try {
            Table table = this.connection.getTable(TableName.valueOf(tableName));// 获取表
            Scan scan = new Scan();
            scan.setStartRow(Trans(startRowKey));
            scan.setStopRow(Trans(stopRowKey));
            for (String col : selectCols)
                scan.addColumn(Trans(selectFml), Trans(col));
            ResultScanner rs = table.getScanner(scan);
            table.close();
            return rs;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public Result[] query(
            String tableName,
            List<String> rowKeys) {
        try {
            Table table = this.connection.getTable(TableName.valueOf(tableName));// 获取表
            List<Get> gets = new ArrayList<Get>();
            for (String rowKey : rowKeys)
                gets.add(new Get(Trans(rowKey)));
            Result[] results = table.get(gets);
            table.close();
            return results;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void print(Result result) {
        List<Cell> cells = result.listCells();
        for (Cell cell : cells) {
            System.out.println("rowKey: " + Trans(CellUtil.cloneRow(cell)));
            System.out.println("family:" + Trans(CellUtil.cloneFamily(cell)));
            System.out.println("qualifier:" + Trans(CellUtil.cloneQualifier(cell)));
            System.out.println("value:" + Trans(CellUtil.cloneValue(cell)));
            System.out.println("Timestamp:" + cell.getTimestamp());
            System.out.println("-------------------------------------------");
        }
    }

    public static void print(ResultScanner rs) {
        for (Result result : rs)
            print(result);
    }

    public static void print(Result[] results) {
        for (Result result : results)
            print(result);
    }

    /**
     * rowkey的例子，需要自己定义！！！！！！
     */
    private String makeRowKey(String... fields) {
        String ret = "en?";
        for (String field : fields) {
            ret += field;
            ret += "_";
        }
        ret = ret.substring(0, ret.length() - 1);
        ret += UUID.randomUUID().toString().substring(24);
        return ret;
    }

    /**
     * 列的设置，需要自己定义！！！！！！
     */
    public static List<String> makeColumns() {
        List<String> columns = new ArrayList<String>();
        columns.add("username");
        columns.add("repostnum");
        columns.add("commentnum");
        columns.add("likenum");
        columns.add("posttime");
        columns.add("content");
        return columns;
    }

    /**
     * 一行值的设置，需要自己定义！！！！！！
     */
    public static List<String> makeValues(List raw) {
        List<String> values = new ArrayList<String>();
        values.add(String.valueOf(raw.get(1)));
        values.add(String.valueOf(raw.get(3)));
        values.add(String.valueOf(raw.get(4)));
        values.add(String.valueOf(raw.get(5)));
        values.add("-1");
        values.add(String.valueOf(raw.get(7)));
        return values;
    }

    public static void main(String[] args) {
        HbaseUtil hbase = new HbaseUtil()
                .table("tabletest")
                .family("f1");

        hbase.replaceInsert("timzxz", "TwitterDAO", rowkeys, columns, values);
        print(hbase.query(
                "tabletest",
                "ba101772207^",
                "ba101772207`"));
        print(hbase.query("hahaha", "123456", "123465"));
        hbase.close();/* 用完记得close */
    }

}