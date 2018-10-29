#!/home/longen/.pyenv/shims/python
# -*- coding:utf-8 -*-
import os
import re
import bs4
import json
import time
import zipfile
import logging
import traceback

import requests
from requests import get, post
from io import BytesIO
from logging import handlers
from datetime import datetime, timedelta
from urllib.parse import urlencode, urljoin

from bottle import route, HTTPResponse, run, request, template

project_path = os.path.dirname(__file__)
logger = logging.getLogger("export")

file_handler = handlers.RotatingFileHandler(
    os.path.join(project_path, "export.log"), maxBytes=1024 * 1024 * 10, backupCount=5)

logger.addHandler(file_handler)
logger.setLevel(logging.DEBUG)

OCT_HOST = "octopus.app.jinanlongen.com"

oct_params = dict(tuple(line.strip().split(":", 1)) for line in """utf8:✓
upload_date:计划上新时间
planned_upload_step_end_at:
store_id:
status:请选择
cycle_grade:请选择
store_task_id:""".split("\n"))

def login_octopus():
    url_login = 'http://{}/login'.format(OCT_HOST)
    user_name = 'admin'
    password = 'meiguogou5.com'

    resp = get(url_login, timeout = 120)
    doc = bs4.BeautifulSoup(resp.text, 'lxml')
    cookies = resp.cookies
    csrf_token = doc.select('meta[name=csrf-token]')[0].attrs['content']

    for i in range(0, 3):
        login_data = { 'session[name]': user_name, 'session[password]': password, 'authenticity_token': csrf_token }
        resp = post(url_login, login_data, cookies = cookies, allow_redirects = False)
        if resp.status_code == 302: break
        print('failed try: %d'%i)
        time.sleep(2)
    else:
        cookies = None

    return cookies

def get_task_ids(date):
    cookies = login_octopus()
    if cookies == None: raise Exception('cannot login octopus')

    oct_params["planned_upload_step_end_at"] = "%s-%s-%s" % (
        date[:4], date[4:6], date[6:])
    first_url = "%s?%s" % (
        "http://{}/store_tasks/tb_new_arrival_search".format(OCT_HOST),
        urlencode(oct_params))
    links = []
    task_ids = []
    links.append(first_url)
    while links:
        url = links.pop()
        for i in range(1, 4):
            try:
                resp = get(url, cookies = cookies, timeout = (i * 60))
                break
            except requests.exceptions.ReadTimeout as e:
                if i > 3:
                    raise e
                print('failed to get: {}. retry {} after 60 seconds'.format(url, i))
                time.sleep(60)
                print('retrying ...')

        soup = bs4.BeautifulSoup(resp.text, "lxml")
        tbody = soup.find(id="body")

        explore_task_links = tbody.select("tr td a")
        for _link in explore_task_links:
            text = _link.text
            if text.isdigit():
                task_ids.append(text)
        next = soup.select(".next a")
        if next:
            links.append(urljoin(url, next.pop().get("href")))
    return task_ids


def get_shop(shops_store=[0, []]):
    last_time, _ = shops_store
    if time.time() - last_time > 3600:
        try:
            resp = get("http://192.168.200.94:10000/store_tasks.json")
            if resp.status_code == 200:
                shops_store[1] = json.loads(resp.text)
                shops_store[0] = time.time()
            else:
                logger.error("Roc_oct rails server returns %s: %s. " %
                         (resp.status_code, resp.text))
        except Exception:
            logger.error(
                "Error in roc_oct rails server: " +
                "".join(traceback.format_exc()))
            raise
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
                shops = [shop for shop in shops
                         if str(shop["store_task_id"]) in task_ids]
            else:
                return template(os.path.join(
                    project_path, "search.html"), date=date)
        crawl_tasks = dict()
        unregist_tasks = list()

        for shop in shops:
            explore_tasks = [tasks for tasks in shop["explore_tasks"]
                             if tasks["explore_task_id"]]
            if explore_tasks:
                for explore_task in explore_tasks:
                    if shop["taxon"].count(explore_task["taxon"]):
                        taxon = shop["taxon"]
                    else:
                        taxon = "|".join([shop["taxon"], explore_task["taxon"]])
                    try:
                        last_exec_at = datetime.strptime(
                            explore_task["last_exec_at"][:19],
                            "%Y-%m-%dT%H:%M:%S") + timedelta(hours=8)
                        days = str((now - last_exec_at).days)
                        last_exec_at = last_exec_at.strftime(
                            "%Y-%m-%d %H:%M:%S")
                    except (ValueError, TypeError):
                        days = "0"
                        last_exec_at = "尚未执行"
                    crawl_tasks[explore_task["explore_task_id"]] = \
                        [shop["store_name"], str(shop["store_task_id"]),
                        str(explore_task["explore_task_id"]),
                        shop["source_site_name"],
                        taxon, shop["gender"], shop["brand"],
                        last_exec_at, days]
            else:
                unregist_tasks.append(shop)
        logger.debug("Compare oct task and roc task finished. ")
        crawl_tasks_str = ""
        unregist_tasks_str = ""

        crawl_tasks_str += \
            "店铺,店铺任务ID,ROC任务ID,来源网站名称,分类名称," \
            "性别名称,品牌名称,最后执行时间,最后执行时间差/天\n"
        for crawl_task in crawl_tasks.values():
            crawl_tasks_str += (",".join(crawl_task)) + "\n"

        unregist_tasks_str += "编号,来源网站,品牌,性别,分类,店铺/任务ID\n"
        for index, unregist_task in enumerate(unregist_tasks):
            unregist_tasks_str += "%s,%s,%s,%s,%s,%s/%s\n" % (
                index + 1, unregist_task["source_site_name"],
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
        headers['Date'] = time.strftime(
            "%a, %d %b %Y %H:%M:%S GMT", time.gmtime())
        headers["Accept-Ranges"] = "bytes"
        headers['Content-Disposition'] = 'attachment; filename="%s.zip"' % \
                                         time.strftime("%Y%m%d%H%M%S")
        return HTTPResponse(body, **headers)
    except Exception as e:
        logger.error("Error: %s" % traceback.format_exc())
        raise e


if __name__ == "__main__":
    run(host=os.environ.get("HOST", "127.0.0.1"),
        port=os.environ.get("PORT", 8888))
