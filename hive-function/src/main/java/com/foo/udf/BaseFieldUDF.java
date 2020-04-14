package com.foo.udf;


import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * 解析基础字段表 一对一
 */
public class BaseFieldUDF extends UDF {
    public String evaluate(String line,String jsonkeysString) {
//        准备一个sb
        StringBuilder sb = new StringBuilder();

//        1585223282522|{"cm":{"ln":"-96.7","sv":"V2.6.6","os":"8.1.9","g":"797Y8P0Y@gmail.com","mid":"0","nw":"3G","l":"pt","vc":"6","hw":"640*1136","ar":"MX","uid":"0","t":"1585130950979","la":"22.2","md":"HTC-15","vn":"1.0.2","ba":"HTC","sr":"H"},"ap":"app","et":[{"ett":"1585207964362","en":"notification","kv":{"ap_time":"1585158380927","action":"1","type":"3","content":""}},{"ett":"1585192474494","en":"active_foreground","kv":{"access":"","push_id":"3"}},{"ett":"1585152835025","en":"favorites","kv":{"course_id":5,"id":0,"add_time":"1585141848917","userid":5}}]}       2020-03-30
//      1.  切割
        String[] jsonKeys = jsonkeysString.split(",");
//      2.  处理line  服务器时间|json
        String[] logContents = line.split("\\|");
//      3.  合法性校验
        if(logContents.length != 2 || StringUtils.isBlank(logContents[1])){
            return "";
        }
//        开始处理json
        try {

            JSONObject jsonObject = new JSONObject(logContents[1]);
//        获取cm里面的对象
            JSONObject base = jsonObject.getJSONObject("cm");

//        循环遍历取值
            for (int i = 0; i < jsonKeys.length; i++) {
                String fileName = jsonKeys[i].trim();

                if(base.has(fileName)){
                    sb.append(base.getString(fileName)).append("\t");
                }else{
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

    public static void main(String[] args) throws JSONException {
        String line = "1585223282522|{\"cm\":{\"ln\":\"-96.7\",\"sv\":\"V2.6.6\",\"os\":\"8.1.9\",\"g\":\"797Y8P0Y@gmail.com\",\"mid\":\"0\",\"nw\":\"3G\",\"l\":\"pt\",\"vc\":\"6\",\"hw\":\"640*1136\",\"ar\":\"MX\",\"uid\":\"0\",\"t\":\"1585130950979\",\"la\":\"22.2\",\"md\":\"HTC-15\",\"vn\":\"1.0.2\",\"ba\":\"HTC\",\"sr\":\"H\"},\"ap\":\"app\",\"et\":[{\"ett\":\"1585207964362\",\"en\":\"notification\",\"kv\":{\"ap_time\":\"1585158380927\",\"action\":\"1\",\"type\":\"3\",\"content\":\"\"}},{\"ett\":\"1585192474494\",\"en\":\"active_foreground\",\"kv\":{\"access\":\"\",\"push_id\":\"3\"}},{\"ett\":\"1585152835025\",\"en\":\"favorites\",\"kv\":{\"course_id\":5,\"id\":0,\"add_time\":\"1585141848917\",\"userid\":5}}]}";
        String x = new BaseFieldUDF().evaluate(line,"mid,uid,vc,vn,l,sr,os,ar,md,ba,sv,g,hw,nw,ln,la,t");
        System.out.println(x);
    }

}
