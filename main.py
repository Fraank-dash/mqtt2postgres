from source import broker2db
# this should do the trick...

try: 
    x = broker2db.broker(config_file_name=r'config/config.yaml')
except Exception as ex:
    print(ex)
try:
    x.run()
except Exception as ex:
    print(ex)