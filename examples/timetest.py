import time
now = time.time()
now_mono = time.monotonic()
now_int = now.as_integer_ratio()
print (now,now_int)
print (now_mono, now_mono/3600)
print(time.ctime())
print(time.process_time())