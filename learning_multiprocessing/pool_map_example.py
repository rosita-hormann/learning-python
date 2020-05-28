import multiprocessing

def f(number):
	print('hello', number)

if __name__ == '__main__':
	pool = multiprocessing.Pool() #use all available cores, otherwise specify the number you want as an argument processes=n
	numbers = []
	for i in range(0, 512):
		numbers.append(i)
	pool.map(f, numbers)
	pool.close()
	pool.join()