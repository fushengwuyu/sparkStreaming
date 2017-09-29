package MR;

/**
 * Created by hadoop on 2017/6/21.
 * product_no lac_id moment start_time user_id county_id staytime city_id
 * 13429100031 22554 8 2013-03-11 08:55:19.151754088 571 571 282 571
 * 13429100082 22540 8 2013-03-11 08:58:20.152622488 571 571 270 571
 *
 * 字段解释：
 * product_no：用户手机号；
 * lac_id：用户所在基站；
 * start_time：用户在此基站的开始时间；
 * staytime：用户在此基站的逗留时间。
 * 需求:根据lac_id 和start_time 知道用户当时的位置，根据staytime 知道用户各个基站的逗留时长。根据轨迹合并连续基站的staytime
 * 最终得到每一个用户按时间排序在每一个基站驻留时长
 * 期望输出举例：
 * 13429100082 22540 8 2013-03-11 08:58:20.152622488 571 571 270 571
 * 13429100082 22691 8 2013-03-11 08:56:37.149593624 571 571 390 571
 *
 * 分析：
 *  这是一个简单的排序题， 可以考虑自定义sort类，它实现WritableCompairable接口，重载具体的compair方法，同时
 *  将sort类作为map的key的类型。框架自动调用它实现特殊排序的功能。
 */
public class TimeCount {
}
