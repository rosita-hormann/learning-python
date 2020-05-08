"""
Threading:
- A new thread is spawned within the ecisting process.
- Starting a thread is faster than starting a process
- Memory is shared between all threads.
- Nutexes often necessary to control access to shared data.
- One GIL (Global Interpreter Lock) for all threads.
"""

from threading import Thread
import os
import math

def calc():
	for i in range(0, 4000000):
		math.sqrt(i)

threads = []

for i in range(os.cpu_count()):
	print('registering thread %d' % i)
	threads.append(Thread(target=calc))

for thread in threads:
	thread.start()

for thread in threads:
	thread.join()