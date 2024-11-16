from source import broker2db

try: 
    x = broker2db.broker(config_file_name=r'examples/test_config.yaml') 
    
except Exception as ex:
    print(ex)
try:
    x.run()
except Exception as ex:
    print(ex)