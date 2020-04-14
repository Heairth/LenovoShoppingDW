package com.foo.udf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.json.JSONException;
import org.json.JSONObject;

public class BaseFieldUDF extends UDF {
    public String evaluate(String line, String jsonkeysString) {
// 0 准备一个 sb
        StringBuilder sb = new StringBuilder();
// 1 切割 jsonkeys mid uid vc vn l sr os ar md
        String[] jsonkeys = jsonkeysString.split(",");
// 2 处理 line 服务器时间 | json
        String[] logContents = line.split("\\|");
// 3 合法性校验
        if (logContents.length != 2 || StringUtils.isBlank(logContents[1])) {
            return "";
        }
// 4 开始处理 json
        try {
            JSONObject jsonObject = new JSONObject(logContents[1]);
// 获取 cm 里面的对象
            JSONObject base = jsonObject.getJSONObject("cm");
// 循环遍历取值
            for (int i = 0; i < jsonkeys.length; i++) {
                String filedName = jsonkeys[i].trim();
                if (base.has(filedName)) {
                    sb.append(base.getString(filedName)).append("\t");
                } else {
                    sb.append("\t");
                }
            }
            sb.append(jsonObject.getString("et")).append("\t");
            sb.append(logContents[0]).append("\t");
        } catch (JSONException e) {
            e.printStackTrace();
        }
        return sb.toString();
    }
    public static void main(String[] args) {
        String line = "1585238416060|{\"cm\":{\"ln\":\"-46.7\",\"sv\":\"V2.1.6\",\"os\":\"8.1.6\",\"g\":\"8OOV6KL1@gmail.com\",\"mid\":\"0\",\"nw\":\"4G\",\"l\":\"es\",\"vc\":\"17\",\"hw\":\"1080*1920\",\"ar\":\"MX\",\"uid\":\"0\",\"t\":\"1585181409752\",\"la\":\"-42.6\",\"md\":\"Huawei-2\",\"vn\":\"1.2.0\",\"ba\":\"Huawei\",\"sr\":\"B\"},\"ap\":\"app\",\"et\":[{\"ett\":\"1585214623137\",\"en\":\"newsdetail\",\"kv\":{\"entry\":\"2\",\"goodsid\":\"0\",\"news_staytime\":\"16\",\"loading_time\":\"6\",\"action\":\"1\",\"showtype\":\"3\",\"category\":\"13\",\"type1\":\"\"}},{\"ett\":\"1585236038964\",\"en\":\"loading\",\"kv\":{\"extend2\":\"\",\"loading_time\":\"40\",\"action\":\"3\",\"extend1\":\"\",\"type\":\"1\",\"type1\":\"\",\"loading_way\":\"2\"}},{\"ett\":\"1585237759381\",\"en\":\"ad\",\"kv\":{\"entry\":\"1\",\"show_style\":\"3\",\"action\":\"2\",\"detail\":\"\",\"source\":\"4\",\"behavior\":\"1\",\"content\":\"2\",\"newstype\":\"7\"}},{\"ett\":\"1585213358811\",\"en\":\"active_foreground\",\"kv\":{\"access\":\"\",\"push_id\":\"3\"}},{\"ett\":\"1585220363209\",\"en\":\"active_background\",\"kv\":{\"active_source\":\"2\"}},{\"ett\":\"1585190427083\",\"en\":\"praise\",\"kv\":{\"target_id\":6,\"id\":5,\"type\":1,\"add_time\":\"1585199208691\",\"userid\":9}}]}";
        String x = new BaseFieldUDF().evaluate(line,
                "mid,uid,vc,v`n,l,sr,os,ar,md,ba,sv,g,hw,nw,ln,la,t");
        System.out.println(x);
    }
}

