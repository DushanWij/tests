from snowflake_utils.analyticsdb_io import AnalyticsDBConnector

user_email = 'dushan.wijesinghe@transferwise.com'


db_io = AnalyticsDBConnector(schema_name='reports')
db_io.use_local_connection('dushan.wijesinghe@transferwise.com')

def df_contact_public():
    df_contact_public_script = """
            SELECT text, ts, REPLY_USERS
FROM SLACK.MESSAGES
where channel_id = 'CLYNPV90R' AND ts LIKE '%2021-06-01 13:39:13%'
   OR ts LIKE '%2021-01-26 11:41:27%'
   OR ts LIKE '%2021-04-13 08:25:04%'
   OR ts LIKE '%2021-07-14 10:35:12%'
   OR ts LIKE '%2021-01-11 11:57:05%'
   OR ts LIKE '%2021-02-03 16:10:29%'
   OR ts LIKE '%2021-02-17 09:01:12%'
   OR ts LIKE '%2021-03-09 10:21:58%'
   OR ts LIKE '%2021-03-03 11:48:30%'
   OR ts LIKE '%2021-03-11 09:09:27%'
   OR ts LIKE '%2021-03-23 08:54:42%'
   OR ts LIKE '%2021-05-11 14:13:52%'
   OR ts LIKE '%2021-05-18 11:01:51%'
   OR ts LIKE '%2021-05-18 10:24:55%'
   OR ts LIKE '%2021-06-30 10:56:09%'
   OR ts LIKE '%2021-07-08 15:08:24%'
   OR ts LIKE '%2021-07-22 13:28:05%'
   OR ts LIKE '%2021-02-16 02:57:41%'
   OR ts LIKE '%2021-02-22 15:04:25%'
   OR ts LIKE '%2021-04-20 12:27:43'
   OR ts LIKE '%2021-04-14 07:26:36%'
   OR ts LIKE '%2021-04-28 08:51:06%'
   OR ts LIKE '%2021-05-20 11:04:17%'
   OR ts LIKE '%2021-05-19 20:41:25%'
   OR ts LIKE '%2021-06-21 18:40:50%'
   OR ts LIKE '%2021-06-29 18:17:44%'
   OR ts LIKE '%2021-07-05 06:54:30%'
   OR ts LIKE '%2021-07-09 15:17:47%'
   OR ts LIKE '%2021-07-15 16:03:18%'
   OR ts LIKE '%2021-01-11 11:15:57%'
   OR ts LIKE '%2021-01-08 11:54:08%'
   OR ts LIKE '%2021-01-08 08:16:04%'
   OR ts LIKE '%2021-01-07 12:06:34%'
   OR ts LIKE '%2021-02-08 15:53:01%'
   OR ts LIKE '%2021-02-08 07:13:23%'
   OR ts LIKE '%2021-02-22 16:20:47%'
   OR ts LIKE '%2021-03-10 10:09:31%'
   OR ts LIKE '%2021-03-23 09:41:50%'
   OR ts LIKE '%2021-04-06 06:50:28%'
   OR ts LIKE '%2021-04-08 07:33:59%'
   OR ts LIKE '%2021-04-07 13:43:18%'
   OR ts LIKE '%2021-04-26 03:20:39%'
   OR ts LIKE '%2021-06-07 11:20:49%'
   OR ts LIKE '%2021-06-05 08:44:40%'
   OR ts LIKE '%2021-06-15 14:16:35%'
   OR ts LIKE '%2021-06-11 14:38:50%'
   OR ts LIKE '%2021-06-16 09:51:24%'
   OR ts LIKE '%2021-06-23 15:26:33%'
   OR ts LIKE '%2021-07-09 09:52:28%'
   OR ts LIKE '%2021-07-20 06:44:21%'
   OR ts LIKE '%2021-07-19 13:54:11%'
   OR ts LIKE '%2021-01-06 16:56:12%'
;
        """
    df_contact = db_io.fetch(df_contact_public_script)
    return df_contact


class processing:
    def cleaning(list1):
        if str(list1) == 'None':
            return 0
        else:
            list2 = list1.split(',')
            list3 = []
            for entry in list2:
                list3.append(
                    str.replace(entry, '\n', '').replace(' ', '').replace('[', '').replace(']', '').replace('"', ''))
            return list3

    def get_list_matches(list1, list2):
        if list2 == 0:
            return []
        matching_values = set(list1).intersection(list2)
        return matching_values


    def get_avg(dictionary, matching_values):
        res = dict.fromkeys(matching_values, ' ')
        result = [dictionary[key] for key in res.keys() & dictionary.keys()]
        if str(result) == '[]':
            return 0
        else:
            return sum(result) / len(result)

    def get_count(matching_values):
        result = matching_values
        if str(result) == '[]':
            return 0
        else:
            return len(result)


list_analysts = ['US6PX35DY', 'UJCEPEUCT', 'U0356HW04', 'U01Q21XE725', 'UA9GL48UQ', 'UG2R0HU7K',
                             'UC06S86DD', 'U0223E4TVSN']

dic_analysts = {'US6PX35DY': 240, 'UJCEPEUCT': 100, 'U0356HW04': 120, 'U01Q21XE725': 00, 'UA9GL48UQ': 45,
                'UG2R0HU7K': 120, 'UC06S86DD': 120, 'U0223E4TVSN': 00}

df_contacts = df_contact_public()
df_contacts['average_time_spent'] = df_contacts['REPLY_USERS'].apply(lambda x: processing.get_avg(dic_analysts, processing.get_list_matches(list_analysts, processing.cleaning(x))))
df_contacts['count_of_analysts'] = df_contacts['REPLY_USERS'].apply(lambda x: processing.get_count(processing.get_list_matches(list_analysts, processing.cleaning(x))))
