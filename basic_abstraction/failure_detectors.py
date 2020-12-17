import threading
import time

start = time.time()

class PerfectFailureDetector:
    heartbeat_request = ("pfd", "request")
    heartbeat_reply = ("pfd", "reply")
    
    def __init__(self, link, crash_callback, timeout_time=0.1):
        self.peers = set()
        self.detected = set()
        self.alive = set()
        self.lock = threading.Lock()
        self.crash_callback = crash_callback
        self.timeout_time = timeout_time
        self.link = link
        self.link.add_callback(self.receive)
        self.timeout_thread = None

    def start_heartbeat(self):
        self.timeout_thread = PerfectFailureDetector.TimeoutThread(self.timeout_callback, self.timeout_time)
        self.timeout_thread.start()

    def add_peers(self, *peers):
        with self.lock:
            self.peers.update(peers)
            self.alive.update(peers)

    def receive(self, source_number, raw_message):
        if raw_message == self.heartbeat_request:
            self.link.send(source_number, self.heartbeat_reply)
        elif raw_message == self.heartbeat_reply and source_number not in self.alive:
            with self.lock:
                self.alive.add(source_number)

    def timeout_callback(self):
        with self.lock:
            # Copy because otherwise weird OS shenanigans happens
            old_alive = self.alive
            self.alive = set()
            for peer in self.peers - self.detected:
                if peer not in old_alive:
                    self.detected.add(peer)
                    self.crash_callback(peer)
                self.link.send(peer, self.heartbeat_request)

    def kill(self):
        self.timeout_thread.kill()

    class TimeoutThread(threading.Thread):
        def __init__(self, callback, timeout_time):
            super().__init__()
            self.callback = callback
            self.timeout_time = timeout_time
            self.alive = True

        def run(self):
            time.sleep(self.timeout_time)
            while self.alive:
                time.sleep(0.001)
                self.callback()
                time.sleep(self.timeout_time)

        def kill(self):
            self.alive = False

if __name__ == "__main__":
    from basic_abstraction.link import FairLossLink
    timescale = 0.01

    class Test:
        def __init__(self, process_number):
            self.link = FairLossLink(process_number)
            self.pfd = PerfectFailureDetector(self.link, self.callback, timeout_time=timescale)

        def callback(self, process_number):
            print("{} {}: Process {} has crashed".format(time.time() - start, self.link.process_number, process_number))

        def kill(self):
            self.pfd.kill()
            self.link.kill()

    test0 = Test(0)
    test0.pfd.add_peers(1, 2)
    test1 = Test(1)
    test1.pfd.add_peers(0, 2)
    test2 = Test(2)
    test2.pfd.add_peers(1, 0)

    test0.pfd.start_heartbeat()
    test1.pfd.start_heartbeat()
    test2.pfd.start_heartbeat()

    time.sleep(5*timescale)

    test3 = Test(3)
    test3.pfd.add_peers(1, 2, 0)
    test3.pfd.start_heartbeat()
    test0.pfd.add_peers(3)
    test1.pfd.add_peers(3)
    test2.pfd.add_peers(3)

    time.sleep(5*timescale)
    test2.kill()
    time.sleep(10*timescale)
    test1.kill()

    time.sleep(20*timescale)

    test0.kill()
    test3.kill()
