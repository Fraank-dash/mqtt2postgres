# taken from https://stackoverflow.com/questions/11515944/how-to-use-multiprocessing-queue-in-python

from multiprocessing import Process, Queue
import time
import sys


def reader_proc(queue):
    """Read from the queue; this spawns as a separate Process"""
    while True:
        msg = queue.get()  # Read from the queue and do nothing
        if msg == "DONE":
            break


def writer(count, num_of_reader_procs, queue):
    """Write integers into the queue.  A reader_proc() will read them from the queue"""
    for ii in range(0, count):
        queue.put(ii)  # Put 'count' numbers into queue

    ### Tell all readers to stop...
    for ii in range(0, num_of_reader_procs):
        queue.put("DONE")


def start_reader_procs(qq, num_of_reader_procs):
    """Start the reader processes and return all in a list to the caller"""
    all_reader_procs = list()
    for ii in range(0, num_of_reader_procs):
        ### reader_p() reads from qq as a separate process...
        ###    you can spawn as many reader_p() as you like
        ###    however, there is usually a point of diminishing returns
        reader_p = Process(target=reader_proc, args=((qq),))
        reader_p.daemon = True
        reader_p.start()  # Launch reader_p() as another proc

        all_reader_procs.append(reader_p)

    return all_reader_procs


if __name__ == "__main__":
    num_of_reader_procs = 2
    qq = Queue()  # writer() writes to qq from _this_ process
    for count in [10**4, 10**5, 10**6]:
        assert 0 < num_of_reader_procs < 4
        all_reader_procs = start_reader_procs(qq, num_of_reader_procs)

        writer(count, len(all_reader_procs), qq)  # Queue stuff to all reader_p()
        print("All reader processes are pulling numbers from the queue...")

        _start = time.time()
        for idx, a_reader_proc in enumerate(all_reader_procs):
            print("    Waiting for reader_p.join() index %s" % idx)
            a_reader_proc.join()  # Wait for a_reader_proc() to finish

            print("        reader_p() idx:%s is done" % idx)

        print(
            "Sending {0} integers through Queue() took {1} seconds".format(
                count, (time.time() - _start)
            )
        )
        print("")