#!/home/longen/.pyenv/shims/python
# -*- coding:utf-8 -*-
import os
import re
import json
import time
import zipfile
import logging
import traceback

from io import BytesIO
from requests import get
from logging import handlers
from datetime import datetime
from bottle import route, HTTPResponse, run, request, template

from roc_oct import get_task_ids

project_path = os.path.dirname(__file__)
shops = json.loads(get("http://192.168.200.94:10000/store_tasks.json").text)
logger = logging.getLogger("export")

file_handler = handlers.RotatingFileHandler(
    os.path.join(project_path, "export.log"), maxBytes=1024 * 1024 * 10, backupCount=5)

logger.addHandler(file_handler)
logger.setLevel(logging.DEBUG)


def get_shop(shops_store=[time.time(), json.loads(get("http://192.168.200.94:10000/store_tasks.json").text)]):
    last_time, shops = shops_store
    if time.time() - last_time > 3600:
        shops_store[0] = time.time()
        shops_store[1] = json.loads(get("http://192.168.200.94:10000/store_tasks.json").text)
    return shops_store[1]


@route('/')
def index():
    return template(os.path.join(project_path, "search.html"), date=None)


@route("/search")
def search():
    try:
        now = datetime.now()
        shops = get_shop()
        date = request.GET.get("date")
        if date:
            if re.match("\d{8}", date):
                task_ids = get_task_ids(date)
                logger.debug("Get oct task ids at %s. " % date)
                shops = [shop for shop in shops if str(shop["store_task_id"]) in task_ids]
            else:
                return template(os.path.join(project_path, "search.html"), date=date)
        crawl_tasks = dict()
        unregist_tasks = list()

        for shop in shops:
            explore_tasks = [tasks for tasks in shop["explore_tasks"] if tasks["explore_task_id"]]
            if explore_tasks:
                for explore_task in explore_tasks:
                    taxon = shop["taxon"] if shop["taxon"].count(explore_task["taxon"]) else "|".join(
                        [shop["taxon"], explore_task["taxon"]])
                    try:
                        days = str((now - datetime.strptime(explore_task["last_exec_at"][:10], "%Y-%m-%d")).days)
                    except (ValueError, TypeError):
                        days = "0"
                    crawl_tasks[explore_task["explore_task_id"]] = [shop["store_name"], str(shop["store_task_id"]),
                                                                    str(explore_task["explore_task_id"]),
                                                                    shop["source_site_name"],
                                                                    taxon, shop["gender"], shop["brand"],
                                                                    explore_task["last_exec_at"] or "", days]
            else:
                unregist_tasks.append(shop)
        logger.debug("Compare oct task and roc task finished. ")
        crawl_tasks_str = ""
        unregist_tasks_str = ""

        crawl_tasks_str += "店铺,店铺任务id,ROC任务ID,来源网站名称,分类名称,性别名称,品牌名称,最后执行时间,最后执行时间差/天\n"
        for crawl_task in crawl_tasks.values():
            crawl_tasks_str += (",".join(crawl_task)) + "\n"

        unregist_tasks_str += "编号,来源网站,品牌,性别,分类,店铺/任务id\n"
        for index, unregist_task in enumerate(unregist_tasks):
            unregist_tasks_str += "%s,%s,%s,%s,%s,%s/%s\n" % (index + 1, unregist_task["source_site_name"],
                                                              unregist_task["brand"], unregist_task["gender"],
                                                              unregist_task["taxon"],
                                                              unregist_task["store_name"],
                                                              unregist_task["store_task_id"])

        zip_file = BytesIO()
        zf = zipfile.ZipFile(zip_file, "w")
        zf.writestr("crawl_tasks.csv", crawl_tasks_str.encode("gbk"))
        zf.writestr("unregist_tasks.csv", unregist_tasks_str.encode("gbk"))
        zf.close()
        zip_file.seek(0)
        body = zip_file.read()
        zip_file.close()
        logger.debug("Create zip file finished. ")
        headers = dict()
        headers['Content-Type'] = 'application/zip'
        headers['Content-Length'] = len(body)
        headers['Date'] = time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime())
        headers["Accept-Ranges"] = "bytes"
        headers['Content-Disposition'] = 'attachment; filename="%s.zip"' % time.strftime("%Y%m%d%H%M%S")
        return HTTPResponse(body, **headers)
    except Exception as e:
        logger.error("Error: %s" % traceback.format_exc())
        raise e


if __name__ == "__main__":
    run(host=os.environ.get("HOST", "127.0.0.1"), port=os.environ.get("PORT", 8888))
