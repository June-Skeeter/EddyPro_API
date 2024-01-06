from multiprocessing import Pool
import time
import numpy as np

def f(x):
    time.sleep(0.5)
    return x*x

def f2(x,y):
    time.sleep(0.5)
    return x*y

if __name__ == '__main__':
    with Pool(processes=4) as pool:         # start 4 worker processes
        # result = pool.apply_async(f, (10,)) # evaluate "f(10)" asynchronously in a single process
        # print(result.get(timeout=10))        # prints "100" unless your computer is *very* slow

        # print(pool.map(f, range(10)))       # prints "[0, 1, 4,..., 81]"
        print('async')
        result = pool.apply_async(f, (10,))
        print('f:',result.get(timeout=1))
        # result = pool.map_async(f2, [[10,2],[10,3]])
        # print('f2:',result.get(timeout=1))

            
        print('map')
        for i,result in enumerate(pool.map(f, range(10))):
            print('f:', result)
            
        # for i,result in enumerate(pool.map(f2, [[10,2],[10,3]])):
        #     print('f2:', result)

        print('imap')
        for i,result in enumerate(pool.imap(f, range(10))):
            print(result)
            
        print('starmap')
        for i,result in enumerate(pool.starmap(f, zip(range(10)))):
            print(result)

        print('starmap_async')
        result = pool.starmap_async(f2, zip(range(10),range(10)))
        for r in (result.get()):
            print(r)
        for i,result in enumerate(pool.starmap_async(f2, zip(range(10),range(10))).get()):
            print(result)
        # print(next(it))                     # prints "0"
        # print(next(it))                     # prints "1"
        # print(it.next(timeout=1))           # prints "4" unless your computer is *very* slow

        # result = pool.apply_async(time.sleep, (10,))
        # print(result.get(timeout=1))        # raises multiprocessing.TimeoutError