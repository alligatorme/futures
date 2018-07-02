import requests
import re
#import json
symbol='A0701'
url='http://stock2.finance.sina.com.cn/futures/api/json.php/IndexService.getInnerFuturesDailyKLine?symbol='+symbol
rst=requests.get(url)
#print(type(rst))

def xfer(x):
#	if re.match('\d{4}-\d{2}-\d{2}',x):
	if '-' in x:
		x=x[:4]+x[5:7]+x[-2:]
		return int(x)
	if '.' in x:return float(x)
	return int(x)

for i in rst.json():
	print(list(map(xfer,i)))
#	print(i)
