"""
Multiprocessing:
- A new process is started independent from the first process.
- Starting a process is slower than starting a thread.
- Memory is not shared between all threads.
- Nutexes not necessary (unless threading in the new process)
- One GIL (Global Interpreter Lock) for each process.
"""

from multiprocessing import Process
import os
import math

def calc():
	for i in range(0, 4000000):
		math.sqrt(i)

processes = []

for i in range(os.cpu_count()):
	print('registering process %d' % i)
	processes.append(Process(target=calc))

for process in processes:
	process.start()

for process in processes:
	process.join()
