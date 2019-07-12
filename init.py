from ultron.config import config_setting

config_setting.set_queue(qtype='redis', host='10.15.5.164', port=6379, pwd='', db=1)
config_setting.update()
