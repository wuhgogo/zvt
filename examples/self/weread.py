# -*- coding: utf-8 -*-
import logging
import time
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timedelta

from zvt import init_log, zvt_config
from zvt.informer.informer import WechatInformer

logger = logging.getLogger(__name__)

sched = BackgroundScheduler()

vid = zvt_config['weread_vid']
template_id = zvt_config['weread_template_id']


def saturday(day=None):
    """
    get saturday
    x as weekday
    -x-2 when x<5
    5-x when x>=5
    """
    if day:
        today = datetime.strptime(str(day), "%Y%m%d")
    else:
        today = datetime.now()

    weekday = today.weekday()
    delta = -weekday - 2
    if weekday >= 5:
        delta = 5 - weekday
    return datetime.strftime(today + timedelta(delta), "%Y%m%d")


def team_up():
    team_up_term = saturday()
    return f'https://open.weixin.qq.com/connect/oauth2/authorize?appid=wx8ffef4695bc01c1b&' \
           f'redirect_uri=https%3A%2F%2Fweread.qq.com%2Fwrpage%2Fhuodong%2Fabtest%2Fzudui' \
           f'%3FcollageId%3D{vid}_{team_up_term}%26tag%3D%26shareVid%3D{vid}' \
           f'%26wrRefCgi%3D&response_type=code&scope=snsapi_base&' \
           f'state=ok_userinfo&connect_redirect=1#wechat_redirect'


def team_up_template(message, url):
    return {
        "template_id": template_id,
        "url": url,
        "data": {
            "message": {
                "value": message,
                "color": "#173177"
            },
            "url": {
                "value": "https://weread.qnmlgb.tech/",
                "color": "#173177"
            },
            "time": {
                "value": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "color": "#173177"
            }
        }
    }


@sched.scheduled_job('cron', hour=0, minute=5, day_of_week='5')
def team_up_job():
    while True:
        error_count = 0
        wechat_informer = WechatInformer()
        task = 'weread team up'

        try:
            url = team_up()
            title = msg = f'{task} finished'
            logger.info(msg)
            body = team_up_template(msg, url)
            wechat_informer.send_template_message(body)
            del body['template_id']
            del body['touser']
            wechat_informer.send_sc_message(title, body)
            break
        except Exception as e:
            msg = f'{e}'
            logger.exception(msg)
            time.sleep(60 * 3)
            error_count = error_count + 1
            if error_count == 3:
                wechat_informer.send_template_message(team_up_template(msg, ''))
                break


if __name__ == '__main__':
    init_log('weread.log')

    team_up_job()

    sched.start()

    sched._thread.join()
