package CommonTools;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static CommonTools.HbaseUtil.Trans;

public class HQuery {
    /**
     * TODO
     */

    public static void main(String[] args) {

        HQuery hQuery = new HQuery();
//                .Select()
//                .From()
//                .Where();
        hQuery.RawQuery();
//        ArrayList<ArrayList<String>> results = hQuery.Query();
//        for (ArrayList<String> line : results) {
//            System.out.println("<RowKey> : " + line.get(0));
//            for (int i = 1; i < line.size(); ++i)
//                System.out.println("<col: " + hQuery.columns.get(i - 1) + "> : " + line.get(i));
//        }
//        hQuery.close();
//        hQuery1.close();
    }

    public void weaktieQuerySample() {
        /**
         * the first step
         */
        String uid = "123ssfdf4567sf8as90";

        String timestamp = String.valueOf("3456193652346");
        HQuery UserToOrigin = new HQuery()
                .Select("senword16", "senword15")
                .From("rr")//From("tt")
                .Where("uzz = " + uid, "senword17 < " + senword18ts);
        ArrayList<ArrayList<String>> origins = UserToOrigin.QueryWithRowkey();
        for (ArrayList<String> origin : origins) {
//此处的for循环可以采用worker机制取代

            /**
             * the second step
             */
            HQuery OriginToRepost = new HQuery()
                    .Select("wawa")
                    .From("wawa1")//From("wawa2")
                    .Where("hen = " + origin.get(0));
            ArrayList<ArrayList<String>> reposts = OriginToRepost.Query();
            for (ArrayList<String> repost : reposts) {
                /**
                 * 根据toid统计转发次数...
                 */
            }
            OriginToRepost.close();
        }
        UserToOrigin.close();
    }

    public void featureQuerySample() {

        String ttt = "171734642483246";

        String reposttimestamp = "438257042304";

        HQuery OriginToRepost = new HQuery()
                .Select("senword11", "senword12", "senword13")
                .From("sen")//From("sen0")
                .Where("sent = " + ttt, "senword14 < " + senword14ts);

        ArrayList<ArrayList<String>> reposts = OriginToRepost.Query();
        for (ArrayList<String> repost : reposts) {
            /**
             * 根据uid、toid判断是否弱连接...
             */

        }
        OriginToRepost.close();
    }

    /**
     * 小批量（返回数据不多）查询时推荐使用，结果不包含rowkey
     *
     * @return
     */
    public ArrayList<ArrayList<String>> Query() {
        if (-1 != stage) {
            ArrayList<ArrayList<String>> ret = new ArrayList<ArrayList<String>>();
            ResultScanner rawresults = RawQuery();
            for (Result result : rawresults) {
                ArrayList<String> line = new ArrayList<String>();
                List<Cell> cells = result.listCells();
                if (0 < cells.size())
                    for (Cell cell : cells)
                        line.add(Trans(CellUtil.cloneValue(cell)));
                ret.add(line);
            }
            if (null != resultScanner)
                resultScanner.close();
            if (null != hbaseUtil)
                hbaseUtil.close();
            return ret;
        }
        return null;
    }

    /**
     * 小批量（返回数据不多）查询时推荐使用，结果包含rowkey，在每行的第一个位置
     *
     * @return
     */
    public ArrayList<ArrayList<String>> QueryWithRowkey() {
        if (-1 != stage) {
            ArrayList<ArrayList<String>> ret = new ArrayList<ArrayList<String>>();
            ResultScanner rawresults = RawQuery();
            for (Result result : rawresults) {
                ArrayList<String> line = new ArrayList<String>();
                List<Cell> cells = result.listCells();
                if (0 < cells.size()) {
                    line.add(Trans(CellUtil.cloneRow(cells.get(0))));
                    for (Cell cell : cells)
                        line.add(Trans(CellUtil.cloneValue(cell)));
                }
                ret.add(line);
            }
            if (null != resultScanner)
                resultScanner.close();
            if (null != hbaseUtil)
                hbaseUtil.close();
            return ret;
        }
        return null;
    }

    /**
     * 大批量（返回数据多）查询时使用。一定要记得关闭【hbaseUtil】和【resultScanner】! ! !
     * （RawQuery结果包含rowkey，可以选择使用）
     *
     * @return
     */
    public ResultScanner RawQuery() {
        if (-1 != stage) {
            String startrk = "", stoprk = "";
            char type = Prefix.charAt(1);
            String commrk = "";
            switch (type) {
                case 'a': //ORIGIN
                    commrk = Prefix + DELIMITER + columns.get("uid").Value;
                    if (columns.get("senword1").Condition &&
                            (">").equals(columns.get("senword2").Operator)) {
                        startrk = commrk + DELIMITER + columns.get("senword3").Value;
                        stoprk = commrk + DELIMITER + ":";
                    } else {
                        startrk = commrk + "^";
                        stoprk = commrk + "`";
                    }
                    break;
                case 'c': //REPOST
                    commrk = Prefix + DELIMITER + columns.get("senword4").Value;
                    if (columns.get("senword5").Condition &&
                            ("<").equals(columns.get("senword6").Operator)) {
                        startrk = commrk + DELIMITER + "/";
                        stoprk = commrk + DELIMITER +
                                String.valueOf(
                                        Long.parseLong(
                                                columns.get("senword7").Value) + 1);
                    } else {
                        startrk = commrk + "^";
                        stoprk = commrk + "`";
                    }
                    break;
            }
            if (!(startrk.equals("") || stoprk.equals(""))) {
//                this.hbaseUtil = new HbaseUtil(TableTest);
                List<String> cols = new ArrayList<String>();
                for (String colname : columns.keySet())
                    if (columns.get(colname).Select)
                        cols.add(colname);
                System.out.println("START ROWKEY: " + startrk +
                        "\n STOP ROWKEY: " + stoprk);
                return null;
//                return this.hbaseUtil.query(TableTest, startrk, stoprk, "message", cols);
            }
        }
        /**
         * resultScanner使用后，记得
         *
         * hQuery.resultScanner.close();
         * hQuery.hbaseUtil.close();
         *
         */
        return null;
    }


    class Col {
        boolean Select;
        boolean Distinct;
        boolean Condition;
        String Operator;
        String Value;
        boolean Count;

        public Col(boolean slct, boolean dstc, boolean cdt, boolean cnt) {
            this.Select = slct;
            this.Distinct = dstc;
            this.Condition = cdt;
            this.Count = cnt;
        }

        public void setCondition(String op, String val) {
            this.Condition = true;
            this.Operator = op;
            this.Value = val;
        }

        public void setCount() {
            this.Count = true;
        }

    }

    private static final String TableForecast = "tabletest";
    private static final String DELIMITER = "_";
    private int stage;
    public ResultScanner resultScanner = null;
    public Result result = null;
    public Result[] results = null;
    private String Prefix = null;
    public Map<String, Col> columns = null;//<ColName, ColProperty>
    private HbaseUtil hbaseUtil = null;

    public void close() {
        if (null != resultScanner)
            resultScanner.close();
        if (null != hbaseUtil)
            hbaseUtil.close();
    }

    public HQuery Select(String... cols) {
        int current = 1;
        if (this.stage < current) {
            this.stage = current;
            columns = new HashMap<String, Col>();
            for (String s : cols) {
                String[] param = s.split(" ");
                switch (param.length) {
                    case 0:
                        SyntaxError("SELECT");
                        break;
                    case 1:
                        columns.put(s, new Col(true, false, false, false));
                        break;
                    case 2:
                        if ("distinct" == param[0] || "DISTINCT" == param[0])
                            columns.put(param[1], new Col(true, true, false, false));
                        if ("count" == param[0] || "COUNT" == param[0])
                            columns.put(param[1], new Col(true, false, false, true));
                        break;
                    case 3:
                        columns.put(param[2], new Col(true, true, false, true));
                        break;
                }
            }
        } else SyntaxError("SELECT");
        return this;
    }

    public HQuery From(String type) {
        int current = 3;
        if (this.stage < current) {
            this.stage = current;
            this.Prefix = type;
        } else SyntaxError("FROM");
        return this;
    }

    public HQuery Where(String... conditions) {
        int current = 4;
        if (this.stage < current) {
            this.stage = current;
            for (String cond : conditions) {
                String[] param = cond.split(" ");
                if (3 == param.length) {
                    boolean select = false;
                    for (String colname : columns.keySet())
                        if (param[0] == colname) {
                            Col tmp = columns.get(colname);
                            tmp.setCondition(param[1], param[2]);
                            columns.put(colname, tmp);
                            select = true;
                            break;
                        }
                    if (!select) {
                        Col tmp = new Col(false, false, true, false);
                        tmp.setCondition(param[1], param[2]);
                        columns.put(param[0], tmp);
                    }
                } else SyntaxError("WHERE");
            }
        } else SyntaxError("WHERE");
        return this;
    }

    //TODO
//    public HQuery GroupBy(String... cols) {
//        int current = 5;
//        if (this.stage < current) {
//            this.stage = current;
//
//        } else SyntaxError();
//        return this;
//    }
//
//    public HQuery OrderBy(String... cols) {
//        int current = 6;
//        if (this.stage < current) {
//            this.stage = current;
//
//        } else SyntaxError();
//        return this;
//    }
//
//    public HQuery Having(String conditions) {
//        int current = 7;
//        if (this.stage < current) {
//            this.stage = current;
//
//        } else SyntaxError();
//        return this;
//    }
//
//    public HQuery Limit(int limit) {
//        int current = 8;
//        if (this.stage < current) {
//            this.stage = current;
//
//        } else SyntaxError();
//        return this;
//    }

    private void SyntaxError() {
        this.stage = -1;
        System.out.println("Syntax ERROR ! ! ! !");
    }

    private void SyntaxError(String caller) {
        SyntaxError();
        System.out.println("ERROR IN " + caller + " ! ! ! !");
    }


}
