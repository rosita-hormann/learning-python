from multiprocessing import Process, Queue

def put_something(q, what_to_put):
    q.put(what_to_put)

def get_something(q):
    print(q.get())

if __name__ == '__main__':
    q = Queue()
    array_numbers = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    for n in array_numbers:
        p1 = Process(target=put_something, args=(q,n,))
        p1.start()
        # print q.get()    # prints "[42, None, 'hello']"
        p1.join()

    for n in array_numbers:
        p2 = Process(target= get_something, args=(q,))
        p2.start()
        p2.join()