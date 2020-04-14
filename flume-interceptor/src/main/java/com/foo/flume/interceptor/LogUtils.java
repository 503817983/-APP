package com.foo.flume.interceptor;

import org.apache.commons.lang.math.NumberUtils;

/**
 * 这是一个flume的日志过滤工具类
 */
public class LogUtils {
    public static boolean validateStart(String log) {
        if(log==null){

            return  false;
        }
        //校验json文件
        if(!log.trim().startsWith("{")||!log.trim().endsWith("}")){

            return false;
        }
        return true;
    }

    public static boolean validateEvent(String log) {
        //可以看一下我们的数据
        //服务器时间|json
//        1584956819302|{"cm":{"ln":"-61.0","sv":"V2.1.6","os":"8.1.3","g":"3208D1MO@gmail.com","mid":"4","nw":"3G","l":"pt","vc":"0","hw":"640*960","ar":"MX","uid":"4","t":
//            "1584864789831","la":"-26.4","md":"HTC-1","vn":"1.1.5","ba":"HTC","sr":"L"},"ap":"app","et":[{"ett":"1584915680064","en":"display","kv":{"goodsid":"0","action":"1"
//                ,"extend1":"2","place":"1","category":"33"}},{"ett":"1584867301794","en":"newsdetail","kv":{"entry":"2","goodsid":"1","news_staytime":"24","loading_time":"0","acti
//            on":"2","showtype":"2","category":"50","type1":"542"}},{"ett":"1584878046542","en":"notification","kv":{"ap_time":"1584873825127","action":"1","type":"1","content"
//:""}},{"ett":"1584888749819","en":"active_background","kv":{"active_source":"1"}},{"ett":"1584865839574","en":"comment","kv":{"p_comment_id":3,"addtime":"158485918
//            9027","praise_count":903,"other_id":1,"comment_id":6,"reply_count":16,"userid":2,"content":"氯球冰绥屡吉"}},{"ett":"1584905284201","en":"praise","kv":{"target_id":
//            4,"id":7,"type":2,"add_time":"1584882243602","userid":5}}]}

        //切割

        String[] logContents = log.split("\\|");
        //校验
        if(logContents.length!=2){
            return false;
        }
        //校验服务器时间
        if(logContents[0].length()!=13 || !NumberUtils.isDigits(logContents[0])){
            return false;
        }


        return true;
    }
}
