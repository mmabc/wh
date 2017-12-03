SELECT ip ,count(*) requestCount from schema1.log where request_time between '2017-01-01.15:00:00' and '2017-01-01.15:59:59' group by ip order by requestCount desc;

SELECT ip ,count(*) requestCount from schema1.log where request_time between '2017-01-01.00:00:00 ' and '2017-01-01.23:59:59' group by ip order by requestCount desc;

# this shows that 192.168.129.191 has 747 records for 2017-01-01.
cat access.log |egrep '192.168.129.191'|sort|grep '2017-01-01'|cut -d '|' -f1 > 192.168.129.191.txt