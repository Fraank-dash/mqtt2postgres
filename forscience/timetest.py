# In order to return the current time as "datetime" this workaround is kind of necessary
# this is the test for it
import time
now = time.time()
now_mono = time.monotonic()
now_int = now.as_integer_ratio()
print (now,now_int)
print (now_mono, now_mono/3600)
print(time.ctime())
print(time.process_time())