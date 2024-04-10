import yaml
#import psycopg2
import sqlalchemy as sa
import pandas as pd

def load_config_file(file_name:str):
    """
    """
    file = open(file=file_name,mode='r')
    conf = yaml.safe_load(file)
    return conf


def create_engine_from_conf(conf:dict):
    """
    """
    engine = sa.create_engine('{db_language}+{db_adapter}://{user}:{pw}@{ip}:{port}/{db_name}'.
                            format(db_language=conf['db_server']['database']['db_language'],
                                    db_adapter=conf['db_server']['database']['db_adapter'],
                                    user=conf['db_server']['connection']['user'],
                                    pw=conf['db_server']['connection']['pw'],
                                    ip=conf['db_server']['ip'],
                                    port=conf['db_server']['port'], 
                                    db_name=conf['db_server']['database']['db_name']
                                    )
                             ) 
    return engine

if __name__ == '__main__':
    file_name=r'config/config.yaml'
    conf = load_config_file(file_name=file_name)

    for num,topic in enumerate(conf['mqtt_server']['topics']):
        print(num,topic)
    
    engine = create_engine_from_conf(conf=conf)

    connection = engine.connect() 

    result = connection.execute(sa.text(f"SELECT * FROM {conf['db_server']['database']['db_table']}"))
    
    connection.close()
    df = pd.DataFrame(result.all())
    print(df.to_string())
    