# SQL_GENERATOR
lxf_value_flow = {'table':' report_tb.bigolive_lxf_value_flow_orc ','catalogue':'Beijing_day','ps':'','day':'day','to_uid':'to_uid','from_uid':'from_uid','gift_id':"reverse(split(reverse(order_id),'_')[0])",'value':"ticket_change",'value_type':"ticket_type",'from_countrycode':"from_countrycode","to_countrycode":"to_countrycode"}
default_gift_flow = {'table':' default.user_vgift_changelog_orc ','catalogue':['Beijing_day','Beijing_time','local_time'],'ps':'','day':'day','to_uid':'uid','from_uid':"reverse(split(reverse(op_id),'_')[2])",'gift_id':"vgift_typeid",'value':"vgift-change",'timestamp':"ts/1000000",'hour':"hour",'value_type':"type"}
default_ticket_flow = {'table':' default.user_ticket_changelog_orc ','catalogue':['Beijing_day','Beijing_time','local_time'],'ps':"and appid = 60 and ticket_change >0",'day':'day','to_uid':'uid','from_uid':"reverse(split(reverse(op_id),'_')[2])",'gift_id':"reverse(split(reverse(op_id),'_')[0])",'value':"ticket_change/100",'timestamp':"ts/1000000",'hour':"hour",'value_type':"type"}
default_vmonoey_flow = {'table':' default.user_vmoney_changelog_orc ','catalogue':['Beijing_day','Beijing_time','local_time'],'ps':" and appid = 60 ",'day':'day','to_uid':"reverse(split(reverse(op_id),'_')[1])",'from_uid':"uid",'gift_id':"reverse(split(reverse(op_id),'_')[0])",'value':"vm_change/100",'timestamp':"ts/1000000",'hour':"hour",'value_type':"type"}
shl_vmonoey_change = {'table':' report_tb.bigolive_shl_vmoney_change_orc ','catalogue':'Beijing_day','ps':"",'day':'day','hour':'substr(hour,12,2)','uid':"uid",'value':"vmoney_chang",'value_type':"type",'countrycode':'countrycode'}
shl_ticket_change = {'table':' report_tb.bigolive_shl_ticket_change_orc ','catalogue':'Beijing_day','ps':"",'day':'day','hour':'substr(hour,12,2)','uid':"uid",'value':"ticket_change",'value_type':"type",'countrycode':'countrycode'}
user_info = {'table':' bigolive.app_user_new_orc ','catalogue':'','ps':'and appid = 60 ','registerday':'substr(registertime,1,10)','uid':"uid",'user_name':'user_name','deviceid':'deviceid','bigo_id':'yyuid','gender':'substr(split(data2,"gender")[1],4,1)'}
yingshou_countrycode = {'table':' report_tb.bigolive_shl_fixed_countrycode ','ps':'','catalogue':'','uid':"uid",'yingshou_country':"countrycode"}
user_level = {'table':' report_tb.bigolive_lxf_countrycode_level ','catalogue':'Specific_day','ps':'','day':'day','uid':"uid",'yingshou_country':"countrycode",'user_level':'user_level'}
user_active = {'table':' bigolive.imactive_users_static_tbl_orc ','catalogue':'Beijing_day','ps':'and appid = 60 ','day':'createdate','uid':"uid"}
# 这个需要两个join
family_member = {'table':' report_tb.bigolive_family_member_by_day ','catalogue':'Beijing_day','ps':"and ((from_unixtime(int(qtime/1000)) >date_add(day,1)) or ((from_unixtime(int(qtime/1000))<date_add(day,1)) and (stat = 1))) " ,'day':'day','uid':"uid",'family_id':"family_id",'stat':'stat','quit_time':'from_unixtime(int(qtime/1000))','in_time':'from_unixtime(int(ctime/1000))'}
purchase_dollars = {'table':' report_tb.bigolive_shl_fixed_purchase_report_orc ','catalogue':'Beijing_day','ps':'','day':'day','uid':"uid",'yingshou_country':"countrycode",'purchase_dollars':'purchase_dollars','offline_dollars':'offline_dollars','change_dollars':'change_dollars','agent_dollars':'agent_dollars','store_dollars':'store_dollars','e_dollars':'e_dollars','total_dollars':'purchase_dollars+ offline_dollars+ e_dollars+ agent_dollars'}
online_purchase_channel = {'table':' report_tb.bigolive_shl_purchase_all_temp ','catalogue':'Beijing_day','ps':'','day':'day','uid':"uid",'yingshou_country':"countrycode",'purchase_dollars':'purchase_dollars','purchase_name':'purchase_name','pay_type':'pay_type','hour':'hour','purchase_amount':'purchase_amount','vm_count':'vm_count'}
offline_purchase_channel = {'table':' report_tb.bigolive_lxf_purchase_offline_orc ','catalogue':'Beijing_day','ps':'','day':'day','uid':"uid",'yingshou_country':"countrycode",'purchase_dollars':'purchase_dollars','purchase_amount':'amount','vm_count':'vmoney'}
agents_purchase_channel = {'table':' report_tb.bigolive_lxf_agents_order_orc ','catalogue':'Beijing_day','ps':'','day':'day','uid':"agents_uid",'yingshou_country':"countrycode",'purchase_dollars':'dollars','purchase_amount':'amount','vm_count':'vmoney','agents_country':'agents_country'}
lxf_user_money_history ={'table':'report_tb.bigolive_lxf_money_history_orc','catalogue':'Specific_day','ps':'','day':'day','uid':"uid",'vmoney_balance':'vmoney_balance','yingshou_country':"fixed_countrycode",'send_vmoney':'send_vmoney','get_ticket':'get_ticket','ticket_balance':'ticket_balance'}
user_device = {'table':'live_dw_com.dwd_live_com_dim_snapshot_user_device','catalogue':'Beijing_day','ps':'','day':'day','uid':"uid",'deviceid':'device_id','phone_lang':'phone_lang','device_brand':'lower(vendor)','os':'os','model':'model','country' :'country'}
user_last_login = {'table':'report_tb.bigolive_guxiaoying_uid_lastlogin','catalogue':'','ps':'','lastlogin':'lastlogin','uid':"uid"}
zip_list_flow = [lxf_value_flow,default_gift_flow,default_ticket_flow,default_vmonoey_flow]
zip_list_user = [shl_vmonoey_change,user_info,yingshou_countrycode,user_level,user_active,family_member,purchase_dollars,online_purchase_channel,offline_purchase_channel,
                 agents_purchase_channel,lxf_user_money_history,user_device,user_last_login,shl_ticket_change ]

class event_method(object):
    def __init__(self,event_list,list_num,user_dimension,user_summary_dimension,user_catalogue,user_restrict,user_order_by,user_having_by):
        self.event_list = event_list
        self.list_num = list_num
        self.user_dimension = user_dimension
        self.user_summary_dimension = user_summary_dimension
        self.user_catalogue = user_catalogue
        self.user_restrict = user_restrict
        self.user_order_by = user_order_by
        self.user_having_by = user_having_by

    # def ticket_change_quick (event_list,list_num,user_dimension,user_summary_dimension,user_catalogue,user_restrict,user_order_by,user_having_by):
    #     #
    #     a1 = ''
    #     a2 = ''
    #     # 设置维度
    #     for i in range(len(a)):
    #         if a[i] == '送礼用户':
    #             a1 = str(a1) + event_list[+number]['from_uid'] + " as from_uid"
    #             a2 = str(a2) + event_list[+number]['from_uid']
    #         elif a[i] == '收礼用户':
    #             a1 = str(a1) + zip_list_flow[number]['to_uid'] + " as to_uid"
    #             a2 = str(a2) + zip_list_flow[number]['to_uid']
    #         elif a[i] == '天':
    #             a1 = str(a1) + zip_list_flow[number]['day'] + " as day"
    #             a2 = str(a2) + zip_list_flow[number]['day']
    #         elif a[i] == '礼物id':
    #             a1 = str(a1) + zip_list_flow[number]['gift_id'] + " as gift_id"
    #             a2 = str(a2) + zip_list_flow[number]['gift_id']
    #         elif a[i] == '小时':
    #             a1 = str(a1) + zip_list_flow[number]['hour'] + " as hour"
    #             a2 = str(a2) + zip_list_flow[number]['hour']
    #         elif a[i] == '本地具体时间(精确到分钟)':
    #             a1 = str(a1) + "from_unixtime(cast(" + zip_list_flow[number][
    #                 'timestamp'] + "- ${timezone}*3600 as int)) as local_day"
    #             a2 = str(a2) + "from_unixtime(cast(" + zip_list_flow[number]['timestamp'] + "- ${timezone}*3600 as int))"
    #         elif a[i] == '北京具体时间(精确到分钟)':
    #             a1 = str(a1) + "from_unixtime(floor(" + zip_list_flow[number]['timestamp'] + ")) as specific_time"
    #             a2 = str(a2) + "from_unixtime(floor(" + zip_list_flow[number]['timestamp'] + "))"
    #         if i < len(a) - 1:
    #             a1 = a1 + ','
    #             a2 = a2 + ','
    #         elif i == len(a) - 1:
    #             a1 = a1
    #             a2 = a2
    #     b1 = ''
    #     b2 = ''
    #     # 设置汇总维度
    #     for i2 in range(len(b)):
    #         if b[i2] == '度量值汇总':
    #             b1 = str(b1) + "sum(" + zip_list_flow[number]['value'] + ") as value_sum"
    #             b2 = str(b2) + 'value_sum'
    #         elif b[i2] == 'UV值-to':
    #             b1 = str(b1) + "count(distinct(" + zip_list_flow[number]['to_uid'] + ")) as uv"
    #             b2 = str(b2) + 'uv'
    #         elif b[i2] == 'UV值-from':
    #             b1 = str(b1) + "count(distinct(" + zip_list_flow[number]['from_uid'] + ")) as from_uv"
    #             b2 = str(b2) + 'from_uv'
    #         elif b[i2] == '次数':
    #             b1 = str(b1) + "count(*) as times"
    #             b2 = str(b2) + 'times'
    #         # 特定table才有
    #         if i2 < len(b) - 1:
    #             b1 = b1 + ','
    #             b2 = b2 + ','
    #         elif i2 == len(b) - 1:
    #             b1 = b1
    #             b2 = b2
    #     b3 = b2.split(',')
    #     c1 = ''
    #     # 设置限定条件
    #     for i3 in range(len(c)):
    #         # c = ['需要输入特定的收礼用户id', '需要输入特定的送礼用户id', '需要输入特定的礼物id', '需要指定特定的收礼用户国家码', '需要指定特定的送礼用户国家码']
    #         if c[i3] == '需要输入特定的收礼用户id':
    #             c1 = str(c1) + " and " + zip_list_flow[number]['to_uid'] + " in (${to_uid_list})"
    #         elif c[i3] == '需要输入特定的送礼用户id':
    #             c1 = str(c1) + " and " + zip_list_flow[number]['from_uid'] + " in (${from_uid_list})"
    #         elif c[i3] == '需要输入特定的礼物id':
    #             c1 = str(c1) + " and " + zip_list_flow[number]['gift_id'] + " in (${gift_id_list})"
    #         elif c[i3] == '需要指定特定的送礼用户国家码':
    #             c1 = str(c1) + " and " + zip_list_flow[number]['from_uid'] + " in (${from_countrycode})"
    #         elif c[i3] == '需要指定特定的收礼用户国家码':
    #             c1 = str(c1) + " and to_countrycode in (${to_countrycode})"
    #         elif c[i3] == '需要指定特定type':
    #             # 房间礼物默认1,34,200,305
    #             c1 = str(c1) + " and type in (${value_type})"
    #     print(c1)
    #
    #     # 设置限制条件
    #     e1 = ''
    #     if len(e) == 0:
    #         e1 = ''
    #     else:
    #         if e[0] == '需要按照上述b维度降序的topX的用户数':
    #             e1 = " order by " + b2 + " desc limit ${X}"
    #         elif e[0] == '需要按照上述b维度升序的topX的用户数':
    #             e1 = " order by " + b2 + " asc limit ${X}"
    #         elif e[0] == '需要按照上述第X个维度降序的topX的用户数':
    #             num = e_app[0] - 1
    #             num_data = b3[num]
    #             e1 = " order by " + num_data + " desc limit ${X}"
    #
    #     f1 = ''
    #     if len(f) == 0:
    #         f1 = ''
    #     else:
    #         if f[0] == '度量值汇总':
    #             f1 = " having sum(" + zip_list_flow[number]['value'] + ") >= ${X}"
    #     t1 = ''
    #     if t == '北京时间(精确到天)':
    #         t1 = " where " + zip_list_flow[number]['day'] + " between '${day1}' and '${day2}' "
    #     elif t == '本地时间(精确到分钟)':
    #         t1 = " where " + zip_list_flow[number][
    #             'day'] + " between date_sub('${start_date}',1) and date_add('${end_date}',1) " \
    #                      "and floor(" + zip_list_flow[number][
    #                  'timestamp'] + ") between unix_timestamp(concat('${start_date}',' ${start_date_time}'))+${timezone}*3600 and unix_timestamp(concat('${end_date}',' ${end_date_time}'))+${timezone}*3600  "
    #     elif t == '北京时间(精确到分钟)':
    #         t1 = " where " + zip_list_flow[number]['day'] + " between '${day1}' and '${day2}' " \
    #                                                         "and floor(" + zip_list_flow[number][
    #                  'timestamp'] + ") between unix_timestamp(concat('${day1}',' ${day1time}')) and unix_timestamp(concat('${day2}',' ${day2time}'))"
    #     print(t1)
    #     # 设置最终语句
    #     if len(d) == 0:
    #         result = "select " + a1 + "," + b1 + " from " + zip_list_flow[number]['table'] + t1 + c1 + " " + \
    #                  zip_list_flow[number][
    #                      'ps'] + " group by " + a2 + f1 + e1
    #     else:
    #         d1 = ''
    #         print(d_app)
    #         for i4 in range(len(d)):
    #             if d[i4] == '需要特定时间注册的收礼用户id':
    #                 d1 = str(d1) + d_app[
    #                     0] + "(select uid,split(registertime, ' ')[0] as registerdate from default.app_user_all_orc where split(registertime, ' ')[0] between '${sub_day1}' and '${sub_day2}') as base_2 on base_1.to_uid = base_2.uid"
    #             elif d[i4] == '需要特定时间注册的送礼用户id':
    #                 d1 = str(d1) + d_app[
    #                     0] + "(select uid,split(registertime, ' ')[0] as registerdate from default.app_user_all_orc where split(registertime, ' ')[0] between '${sub_day1}' and '${sub_day2}') as base_3 on base_1.from_uid = base_3.uid"
    #             # 送礼
    #             elif d[i4] == '收礼用户需要特定的家族成员':
    #                 d1 = str(d1) + d_app[
    #                     0] + "(select day,uid, family_id　from report_tb.bigolive_family_member_by_day 　where day between '${sub_day1}' and '${sub_day2}' " \
    #                          "and ((from_unixtime(int(qtime/1000)) >date_add(day,1)) or ((from_unixtime(int(qtime/1000))<date_add(day,1)) and (stat = 1))) " \
    #                          "and country in (${countrycode})  group by day,uid, family_id) as t_family on base_1.from_uid = t_family.uid"
    #             elif d[i4] == '送礼用户需要特定的家族成员 X 按天':
    #                 # 还需要按照to_uid来匹配from_uid,因为是有收礼
    #                 d1 = str(d1) + d_app[
    #                     0] + "(select day,uid, family_id　from report_tb.bigolive_family_member_by_day 　where day between '${sub_day1}' and '${sub_day2}' " \
    #                          "and ((from_unixtime(int(qtime/1000)) >date_add(day,1)) or ((from_unixtime(int(qtime/1000))<date_add(day,1)) and (stat = 1))) " \
    #                          "and country in (${countrycode}) group by day,uid, family_id) as t_family on (base_1.from_uid = t_family.uid and base_1.day = t_family.day)"
    #             elif d[i4] == '需要特定的送礼用户营收国家码':
    #                 d1 = str(d1) + d_app[
    #                     0] + "(select uid,countrycode from report_tb.bigolive_shl_fixed_countrycode where countrycode in (${countrycode}))as t0 on base_1.from_uid = t0.uid"
    #             elif d[i4] == '需要特定的收礼用户营收国家码':
    #                 d1 = str(d1) + d_app[
    #                     0] + "(select uid,countrycode from report_tb.bigolive_shl_fixed_countrycode where countrycode in (${countrycode}))as t0 on base_1.to_uid = t0.uid"
    #             #     d1 = str(d1) + 'day'
    #             elif d[i4] == '需要了解用户的bigoid':
    #                 d1 = str(d1) + d_app[
    #                     1] + "(select uid,nick_name as Username,yyuid as BIGO_ID from bigolive.app_user_new_orc)as t1 on base_1.from_uid = t1.uid"
    #
    #         print(d1)
    #         # result = "select base_1.*, split(registertime, ' ')[0]  from (select "+a1 +','+ b1 +" from "+ zip_list_flow[number]['table']+ " where day between '${day1}' and '${day2}'"+c1+"  group by " + a1 + e1+") as base_1" + d1
    #         result = "select base_1.*,t0.* from (select " + a1 + ',' + b1 + " from " + \
    #                  zip_list_flow[number]['table'] + t1 + c1 + zip_list_flow[number][
    #                      'ps'] + "  group by " + a2 + f1 + ") as base_1" + d1 + e1
    #
    #     print(result)

    def user_feature_quick (event_list,list_num,user_dimension,user_summary_dimension,user_catalogue,user_restrict,user_order_by,user_having_by):
        #
        user_dimension1 = ''
        user_dimension2 = ''
        # 设置维度
        for i in range(len(user_dimension)):
            if user_dimension[i] != 'ALL':
                user_dimension1 = str(user_dimension1) + event_list[list_num][user_dimension[i]]+ ' as ' +user_dimension[i]
                user_dimension2 = str(user_dimension2) + event_list[list_num][user_dimension[i]]
            if i < len(user_dimension) - 1:
                user_dimension1 = user_dimension1 + ','
                user_dimension2 = user_dimension2 + ','
            elif i == len(user_dimension) - 1:
                user_dimension1 = user_dimension1
                user_dimension2 = user_dimension2
        user_summary_dimension1 = ''
        user_summary_dimension2 = ''
        # 设置汇总维度 'sum', 'count_rows', 'count_distinct', 'count']
        for i2 in range(len(user_summary_dimension)):
            if user_summary_dimension[i2] == 'count_rows':
                user_summary_dimension1 = ' count(1) '
            elif user_summary_dimension[i2] != 'None':
                print(user_summary_dimension[i2])
                method = user_summary_dimension[i2].split('&')[1]
                value = user_summary_dimension[i2].split('&')[0]
                if method == 'sum':
                    user_summary_dimension1 = str(user_summary_dimension1) + "sum(" + event_list[list_num][value] + ") as " + method+'_'+value
                    user_summary_dimension2 = str(user_summary_dimension2) + method+'_'+value
                elif method == 'count_distinct':
                    user_summary_dimension1 = str(user_summary_dimension1) + "count(distinct(" + event_list[list_num][value] + ")) as "+ method+'_'+value
                    user_summary_dimension2 = str(user_summary_dimension2) + method+'_'+value
                elif method == 'count':
                    user_summary_dimension1 = str(user_summary_dimension1) + "count(" + event_list[list_num][value] + ") as " + method+'_'+value
                    user_summary_dimension2 = str(user_summary_dimension2) + method+'_'+value
        # # 特定table才有--那就只放在特定的table
        #     elif b[i2] == '博彩钻石消耗与盈利':
        #         b1 = str(b1) + "sum(case when type = '33' and param like '贪吃的bigo活动%' then vm_change end)GreedyBigo_cost," \
        #                        "sum(case when type = '33' and subtype = '5497' then vm_change end)GreedyBigo_win," \
        #                        "sum(case when type = '33' and param like  '13402%' and  vm_change<0  then vm_change end)YummyBigo_cost," \
        #                        "sum(case when type = '33' and param like  '13402%' and vm_change>0  then vm_change  end)YummyBigo_win," \
        #                        "sum(case when type = '33' and param ='15762 fisher bigo' and vm_change<0  then vm_change  end) Fisher_cost," \
        #                        "sum(case when type = '33' and param ='15762 fisher bigo' and vm_change>0  then vm_change  end) Fisher_win," \
        #                        "sum(case when type = '59' and subtype =25866 and  vm_change<0  then vm_change  end) Treasurebigo_cost," \
        #                        "sum(case when type = '59' and subtype =25866 and vm_change>0  then vm_change  end) Treasurebigo_win," \
        #                        "sum(case when type = '59' and subtype =26967 and  vm_change<0  then vm_change  end) Lucky_Dino_cost," \
        #                        "sum(case when type = '59' and subtype =26967 and vm_change>0  then vm_change  end) Lucky_Dino_win"
        #     elif b[i2] == '充值所获钻石':
        #         b1 = str(b1) + "sum(case when type in ('4','6','62','69','70','75') then vm_change end) as cash_vm"
        #     elif b[i2] == '金豆转钻石所获钻石':
        #         b1 = str(b1) + "sum(case when type = '11' then vm_change end ) as zhuan_vm"
        #     elif b[i2] == 'SuperLucky所获钻石':
        #         b1 = str(b1) + "sum(case when type in ('66','68') then vm_change end) as SL_vm"
        #     elif b[i2] == '房间送礼消耗钻石':
        #         b1 = str(b1) + "sum(case when type  in ('3','200','204') then vm_change end) as gift_giving_vm"
            if i2 < len(user_summary_dimension) - 1:
                user_summary_dimension1 = user_summary_dimension1 + ','
                user_summary_dimension2 = user_summary_dimension2 + ','
            elif i2 == len(user_summary_dimension) - 1:
                user_summary_dimension1 = user_summary_dimension1
                user_summary_dimension2 = user_summary_dimension2
        # user_summary_dimension3 = user_summary_dimension2.split(',')
        user_restrict1 = ''
        # 设置限定条件
        print(user_restrict)
        if user_restrict[0] != 'None':
            print("True")
            for i3 in range(len(user_restrict)):
                # c = ['需要输入特定的收礼用户id', '需要输入特定的送礼用户id', '需要输入特定的礼物id', '需要指定特定的收礼用户国家码', '需要指定特定的送礼用户国家码']
                if i3 != 0:
                    user_restrict1 = str(user_restrict1) + " and " +  event_list[list_num][user_restrict[i3]] + " in (${"+user_restrict[i3]+"_list})"
                elif i3 == 0:
                    user_restrict1 = event_list[list_num][user_restrict[i3]] + " in (${"+user_restrict[i3]+"_list})"
                elif user_restrict[i3] == '需要正式的用户(排除游客和机器人)':
                    user_restrict1 = '((cast(uid as bigint) between 0 and 1100000000) or (cast(uid as bigint) between 1500000001 and 1900000000) or (cast(uid as bigint)>3500000000))' \
                         'and (cast(uid as bigint) not between 10000000 and 100000000) and (cast(uid as bigint) not between 1544200001 and 1544710000)'
        else:
            print("False")
            user_restrict1 = ''
        # 设置限制条件,order by 也可以按照一个条件
        user_order_by1 = ''
        if user_order_by != 'None':
            if user_order_by == 'desc':
                user_order_by1 = " order by " + user_summary_dimension2 + " desc limit ${X}"
            elif user_order_by == 'asc':
                user_order_by1 = " order by " + user_summary_dimension2 + " asc limit ${X}"
            # elif e[0] == '需要按照上述第X个维度降序的topX的用户数':
            #     num = e_app[0] - 1
            #     num_data = b3[num]
            #     e1 = " order by " + num_data + " desc limit ${X}"

        user_having_by1 = ''
        if user_having_by != 'None':
            user_having_by1 = " having sum(" + event_list[list_num][user_having_by] + ") >= ${X}"
        print(user_having_by1)
        user_catalogue1 = ''
        if user_catalogue == 'Beijing_day':
            user_catalogue1 = " where " + event_list[list_num]['day'] + " between '${day1}' and '${day2}' "
        elif user_catalogue == 'local_time':
            user_catalogue1 = " where " + event_list[list_num][
                'day'] + " between date_sub('${start_date}',1) and date_add('${end_date}',1) " \
                         "and floor(" + event_list[list_num][
                     'timestamp'] + ") between unix_timestamp(concat('${start_date}',' ${start_date_time}'))+${timezone}*3600 and unix_timestamp(concat('${end_date}',' ${end_date_time}'))+${timezone}*3600  "
        elif user_catalogue == 'Beijing_time':
            user_catalogue1 = " where " + event_list[list_num]['day'] + " between '${day1}' and '${day2}' " \
                                                            "and floor(" + event_list[list_num][
                     'timestamp'] + ") between unix_timestamp(concat('${day1}',' ${day1time}')) and unix_timestamp(concat('${day2}',' ${day2time}'))"
        elif user_catalogue == 'Specific_day':
            user_catalogue1 = " where " + event_list[list_num]['day']  + "= '${day2}'"
        elif user_catalogue == 'None':
            user_catalogue1 = ''
        if user_catalogue != 'None':
            if user_restrict[0] !='None':
                if (user_dimension[0] != 'ALL') and (user_summary_dimension[0] != 'None'):
                    result = "select " + user_dimension1 + "," + user_summary_dimension1 + " from " + event_list[list_num]['table'] + user_catalogue1 + " and " + user_restrict1 + " " + event_list[list_num]['ps'] + " group by " + user_dimension2+user_having_by1 +user_order_by1
                elif (user_dimension[0] != 'ALL') and (user_summary_dimension[0] == 'None'):
                    result = "select " + user_dimension1 + user_summary_dimension1 + " from " + event_list[list_num]['table'] + user_catalogue1 + " and " + user_restrict1 + " " + event_list[list_num]['ps'] + " group by " + user_dimension2+user_having_by1 +user_order_by1
                elif (user_dimension[0] == 'ALL') and (user_summary_dimension[0] != 'None'):
                    result = "select " + user_dimension1 + user_summary_dimension1 + " from " + event_list[list_num][
                        'table'] + user_catalogue1 + " and " + user_restrict1 + " " + event_list[list_num][
                                 'ps'] + user_having_by1 + user_order_by1
                # else:
                #     result = "select " + user_dimension1 + "," + user_summary_dimension1 + " from " + event_list[list_num][
                #     'table'] + user_catalogue1 + " and " + user_restrict1 + " " + event_list[list_num]['ps']+user_having_by1 +user_order_by1
            else:
                if (user_dimension[0] != 'ALL') and (user_summary_dimension[0] != 'None'):
                    result = "select " + user_dimension1 + "," + user_summary_dimension1 + " from " + event_list[list_num]['table'] + user_catalogue1 + " " + event_list[list_num]['ps'] + " group by " + user_dimension2+user_having_by1 +user_order_by1
                elif (user_dimension[0] != 'ALL') and (user_summary_dimension[0] == 'None'):
                    result = "select " + user_dimension1 + user_summary_dimension1 + " from " + event_list[list_num]['table'] + user_catalogue1 + " " + event_list[list_num]['ps'] + " group by " + user_dimension2+user_having_by1 +user_order_by1
                elif (user_dimension[0] == 'ALL') and (user_summary_dimension[0] != 'None'):
                    result = "select " + user_dimension1 + user_summary_dimension1 + " from " + event_list[list_num]['table'] + user_catalogue1 + " " + event_list[list_num]['ps'] +user_having_by1 +user_order_by1
                # else:
                #     result = "select " + user_dimension1 + "," + user_summary_dimension1 + " from " + event_list[list_num][
                #     'table'] + user_catalogue1 + " " + event_list[list_num]['ps']+user_having_by1 +user_order_by1
        else:
            if user_restrict[0] !='None':
                if (user_dimension[0]!= 'ALL') and (user_summary_dimension[0] != 'None'):
                    result = "select " + user_dimension1 + "," + user_summary_dimension1 + " from " + event_list[list_num]['table'] + " where "+ user_restrict1 + " " + event_list[list_num]['ps'] + " group by " + user_dimension2 +user_having_by1 +user_order_by1
                elif (user_dimension[0] != 'ALL') and (user_summary_dimension[0] == 'None'):
                    result = "select " + user_dimension1 + user_summary_dimension1 + " from " + event_list[list_num]['table'] + " where " + user_restrict1 + " " + event_list[list_num]['ps'] + " group by " + user_dimension2+user_having_by1 +user_order_by1
                elif (user_dimension[0] == 'ALL') and (user_summary_dimension[0] != 'None'):
                    result = "select " + user_dimension1 + user_summary_dimension1 + " from " + event_list[list_num]['table'] + " where " + user_restrict1 + " " + event_list[list_num]['ps'] + user_having_by1 +user_order_by1
                # else:
                #     result = "select " + user_dimension1 + "," + user_summary_dimension1 + " from " + event_list[list_num][
                # 'table'] + " where "+ user_restrict1 + " " + event_list[list_num]['ps'] +user_having_by1 +user_order_by1
            else:
                if (user_dimension[0] != 'ALL') and (user_summary_dimension[0] != 'None'):
                    result = "select " + user_dimension1 + "," + user_summary_dimension1 + " from " + event_list[list_num]['table'] + event_list[list_num]['ps'] + " group by " + user_dimension2 +user_having_by1 +user_order_by1
                elif (user_dimension[0] != 'ALL') and (user_summary_dimension[0] == 'None'):
                    result = "select " + user_dimension1 + user_summary_dimension1 + " from " + event_list[list_num]['table']  + event_list[list_num]['ps'] + " group by " + user_dimension2+user_having_by1 +user_order_by1
                elif (user_dimension[0] == 'ALL') and (user_summary_dimension[0] != 'None'):
                    result = "select " + user_dimension1 + user_summary_dimension1 + " from " + event_list[list_num]['table']  + event_list[list_num]['ps'] + user_having_by1 +user_order_by1
                # else:
                #     result = "select " + user_dimension1 + "," + user_summary_dimension1 + " from " + event_list[list_num][
                # 'table'] + event_list[list_num]['ps']+user_having_by1 +user_order_by1
        print(result)
        return result



if __name__ == '__main__':


    event_num = int(input('请输入需要的事件数\n'))
    #每个事件选择的维度
    event_sql = []
    for i in range(event_num):
        result = ''
        print('********事件'+str(i)+'********')
        print('事件分类(下拉列表)--查询用户性质：zip_list_user；查询用户流水情况(钻石,金豆)：zip_list_flow')
        data = ['zip_list_user','zip_list_flow']
        event_list = eval(input('请输入需要的事件分类\n'))
        if event_list == zip_list_user:
            print('所含事件(下拉列表):0.用户钻石情况查询(只支持北京时间) 1.用户属性 2.用户营收国家码 3.用户等级 4.用户活跃日期 5.家族成员名单 6.用户充值美金 '
                  '7.用户线上充值渠道细节查询 8.用户线下充值细节查询 9.代理充值细节查询 10.用户钱包情况&累计消费情况 11.用户设备 12.用户最新登陆时间'
                  '13.用户金豆情况查询(只支持北京时间) ')
        elif event_list == zip_list_flow:
            print('所含事件(下拉列表):0.用户收送礼流向国家用户详情（只支持北京时间) 1.用户礼物数量流向（支持本地时间）；2.用户金豆流水（支出本地时间）3.用户钻石流水(支出本地时间)')
        list_num = int(input('请输入具体的事件所需的排序\n'))
        dimension = list(event_list[list_num].keys())[3:]
        print('该事件有的维度(下拉列表)'+str(dimension))
        # 维度
        user_dimension_init = input('输入你想要的维度，逗号分隔,不需要的话，填ALL\n')
        user_dimension = list(user_dimension_init.split(','))
        #下拉列表应该不会存在不存在值的情况
        # print(user_dimension)
        # 需要groupby的维度
        method = ['sum', 'count_rows', 'count_distinct', 'count']
        print('能够选择的汇总维度：' +str(method) +' .不需要的话，填None')
        user_summary_dimension_init = input('基于下拉列表选择，选择你想要汇总的维度&使用的汇总方法，逗号分隔\n')
        user_summary_dimension = list(user_summary_dimension_init.split(','))
        if user_dimension[0] =='ALL' and user_summary_dimension[0] == 'None':
            print("only one of them can be empty/None")
            break
        # 限定目录-单选
        print('能够选择的时间区间类别：'+ str(list(event_list[list_num].values())[1]))
        user_catalogue = input('需要选择的时间区间\n')
        # 是否需要限定所选范围
        user_restrict_init = input('是否需要根据某个维度限定所选范围,不需要的话，填None\n')
        user_restrict = list(user_restrict_init.split(','))
        print(user_restrict)
        user_order_by = ''
        user_having_by = ''
        # 是否需要order by,默认按照汇总维度降序。看看是降序还是升序
        user_order_by = input('input the order you want,asc or desc\n')
        #
        # # having ，只能默认是大于；其实也还是可以选的
        user_having_by = input('input the which dimension \n')
        # 关于目录方面,有得需要本地时间,有的不需要.--像性质这个,从见到table那一刻就需要锁定需要where;
        # 关于收送礼,是另一个方面--需要问需要北京时间还是本地时间.[必须让他们选择任一个]
        if event_list == zip_list_user:
            result = event_method.user_feature_quick(event_list,list_num,user_dimension,user_summary_dimension,user_catalogue,user_restrict,user_order_by,user_having_by)
        elif event_list == zip_list_flow:
            result = event_method.user_feature_quick(event_list,list_num,user_dimension,user_summary_dimension,user_catalogue,user_restrict,user_order_by,user_having_by)
        event_sql.append(result)
    print(event_sql)
    #实际join的信息
    if event_num >1:
        join_method_list = []
        join_key1_list = []
        join_key2_list = []
        for num_i in range(event_num-1):
             #默认join第一个table
            print('********第一个事件和第' + str(num_i+2) + '个事件join的情况********')
            join_method = input('input the method to join [inner join, left join, outer join]\n')
            # 有可能同时join多个字段。
            #链接字段
            join_key1_0 = input('input the table_1 join key-主键，首选uid，其次是day\n')
            join_key1 = list(join_key1_0.split(','))
            join_key2_0 = input('input the table_2 join key-主键，首选uid，其次是day\n')
            join_key2 =list(join_key2_0.split(','))
            join_method_list.append(join_method)
            join_key1_list.append(join_key1)
            join_key2_list.append(join_key2)
        result_init = ''
        for join_i in range(event_num-1):
            print(join_key1_list[join_i])
            print(len(join_key1_list[join_i]))
            if len(join_key1_list[join_i]) ==1:
                result_init =  str(result_init) + join_method_list[join_i] + ' ('+ event_sql[join_i+1] +') as base_'+ str(join_i+1) + ' on (base_0.'+join_key1_list[join_i][0] +' = base_'+ str(join_i+1)+'.'+join_key2_list[join_i][0] +') '
            elif len(join_key1_list[join_i]) ==2:
                result_init = str(result_init) + join_method_list[join_i] + ' (' + event_sql[join_i + 1] + ') as base_' + str(join_i + 1) + ' on (base_0.' + join_key1_list[
                                  join_i][0] + ' = base_' + str(join_i + 1) + '.' + join_key2_list[join_i][0] + ' and ' +'base_0.' + join_key1_list[
                                  join_i][1] + ' = base_' + str(join_i + 1) + '.' + join_key2_list[join_i][1]+ ') '
                print(result_init)
        final_result = 'select * from (' + event_sql[0] + ') as base_0 ' + result_init
        print(final_result)
    elif event_num == 1:
        final_result = event_sql[0]
        print(final_result)
