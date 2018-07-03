import pickle as pkl
import requests,os

def save_var(n,v):
	with open(n,'wb') as fd:
		pkl.dump(v,fd)

def xfer(x):
#	if re.match('\d{4}-\d{2}-\d{2}',x):
	if '-' in x:
		x=x[:4]+x[5:7]+x[-2:]
		return int(x)
	if '.' in x:return float(x)
	return int(x)

def single(symbol):
#	symbol='A0701'
	url='http://stock2.finance.sina.com.cn/futures/api/json.php/IndexService.getInnerFuturesDailyKLine?symbol='+symbol
	try:
		rst=requests.get(url).json()
		if rst:
			print(symbol+' is OK!')
			return [list(map(xfer,i)) for i in rst]
		else:
			print(symbol+' error:')
	except requests.RequestException as e:
		print(symbol+' error:')
		print(e)
		return None

def sina():
	import entity
	info=entity.info()
	drt='/home/raptor/rsvr/sina/'
	for k,v in info.items('Dominant'):
		for nod in v.split(',')[:3]:
			for i in range(19,-1,-1):
				symbol=k+str(i).zfill(2)+nod
				if not symbol in os.listdir(drt):
					rst=single(symbol)
					save_var(drt+symbol,rst)




if __name__=="__main__":
	sina()
