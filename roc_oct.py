#!/home/longen/.pyenv/shims/python
# -*- coding:utf-8 -*-
import os
import json
import bs4
import logging
import datetime
from argparse import ArgumentParser
from urllib.parse import urlencode, urljoin
from requests import get
from logging import handlers

from utils import retry_wrapper

project_path = os.path.dirname(__file__)

logger = logging.getLogger("roc_oct")

file_handler = handlers.RotatingFileHandler(
    os.path.join(project_path, "roc_oct.log"), maxBytes=1024*1024*10, backupCount=5)

logger.addHandler(file_handler)

logger.setLevel(logging.DEBUG)


get = retry_wrapper(5, error_handler=lambda *args, **kwargs: logger.debug("error in requests.get: %s"%str(args)))(get)

oct_adr = "http://octopus.app.jinanlongen.com/store_tasks/tb_new_arrival_search"

oct_params = dict(tuple(line.strip().split(":", 1)) for line in """utf8:✓
upload_date:计划上新时间
planned_upload_step_end_at:
store_id:
status:请选择
cycle_grade:请选择
store_task_id:""".split("\n"))

oct_headers = {
    "Cookie": "_session_id=7856fd5bed1784fe4f5ef96a448f7a34"
}

roc_adr = "http://roc.app.jinanlongen.com/explore_tasks"

roc_params = dict(tuple(line.strip().split(":", 1)) for line in """utf8:✓
q[id]:
q[source_site_id]:1
q[gender_id]:2
q[brand_id]:1
q[taxon_id]:50
q[created_at_start]:
q[created_at_end]:
q[last_exec_at_start]:
q[last_exec_at_end]:""".split("\n"))

roc_headers = {
    "Cookie": "_roc_session=T0ExVDVjemZIcFFCL3dSdDJuTWhnaDNZcndadG10Q252WVowTWk2cElrakhGeXdXaE1aWEpWWkJnamdpUUFFR0VaZW1mcjF5VzlDU2RtY0JCbGlBWGdGTXJJcVphd0tydk5kV1p4QUZkUGJaNWlwN2s2TEdwcGN1RkxDNW11ek56SUxQNlk3SUJudXlveU93QUVoNjE5RzdKelF6YVFqSFRiZmVtREJibXdzZm9HbklOMVk0cGtUNkJ4ak9kdDAxLS1ZaEtzSFpHWGFYYTBmRFp4dmFlTDdBPT0%3D--54b6a1901cd0270998b86fe3b5a4164572d4bfaa"
}

oct_total_adr = "http://192.168.200.94:10000/store_tasks.json"


taxon_adr = "http://roc.app.jinanlongen.com/taxons/%s/children.json"


def get_task_ids(date):
    oct_params["planned_upload_step_end_at"] = "%s-%s-%s"%(date[:4], date[4:6], date[6:])
    first_url = "%s?%s"%(oct_adr, urlencode(oct_params))
    links = []
    task_ids = []
    links.append(first_url)
    while links:
        url = links.pop()
        resp = get(url, headers=oct_headers)
        soup = bs4.BeautifulSoup(resp.text, "lxml")
        tbody = soup.find(id="body")
        a = tbody.select("tr td a")
        for _a in a:
            text = _a.text
            if text.isdigit():
                task_ids.append(text)
        next = soup.select(".next a")
        if next:
            links.append(urljoin(url, next.pop().get("href")))
    return task_ids


def get_jd_task_ids(date=""):
    return json.loads(get("http://jdnew.net.jinanlongen.com/gettaskontime.ashx?startDate=%s&endDate=%s"%(date, date)).text)


def get_total_tasks():
    return json.loads(get(url=oct_total_adr).text)


def get_gender_brand_site_taxon():
    resp = get(roc_adr, headers=roc_headers)
    soup = bs4.BeautifulSoup(resp.text, "lxml")
    taxons, genders, sites, brands = dict(), dict(), dict(), dict()
    for option in soup.select('select[class="taxon-select input-sm search1"] option'):
        value = option.get("value")
        if value == "0":
            continue
        taxons[option.text] = (int(value), dict())
    for option in soup.select('#q_gender_id option'):
        value = option.get("value")
        if not value:
            continue
        genders[option.text] = int(value)
    for option in soup.select('#q_source_site_id option'):
        value = option.get("value")
        if not value:
            continue
        sites[option.text] = int(value)
    for option in soup.select('#q_brand_id option'):
        value = option.get("value")
        if not value:
            continue
        brands[option.text] = int(value)
    return taxons, genders, sites, brands


def get_crawl_tasks(taxons, genders, sites, brands, **kwargs):
    if not kwargs.get("taxon", ""):
        return []

    ts = kwargs.pop("taxon", "").split("|")
    source_site_name = kwargs.pop("source_site_name", "")
    brand = kwargs.pop("brand", "")
    gender = kwargs.pop("gender", "")
    taxon_code = "0"
    children = taxons
    while True:
        try:
            taxon_code, children = children[ts.pop(0)]
            if not children:
                resp = get(taxon_adr%taxon_code, headers=roc_headers)
                if resp.status_code == 200:
                    for child in json.loads(resp.text):
                       children[child["name"]] = child["id"], dict()
                else:
                    break
        except (KeyError, IndexError):
            break

    roc_params["q[source_site_id]"] = source_site_name and sites[source_site_name]
    roc_params["q[gender_id]"] = gender and genders[gender]
    roc_params["q[brand_id]"] = brand and brands[brand]
    roc_params["q[taxon_id]"] = taxon_code
    resp = get(url="%s?%s"%(roc_adr, urlencode(roc_params)), headers=roc_headers)
    soup = bs4.BeautifulSoup(resp.text, "lxml")
    trs = soup.select("tbody tr")
    crawl_tasks = []

    for tr in trs:
        tds = tr.select("td")
        crawl_tasks.append((tds[1].text, tds[2].text, tds[3].text, tds[4].text, tds[5].text, tds[9].text))
    logger.debug("Oct task id: %s, Roc task count: %s. "%(kwargs["store_task_id"], len(crawl_tasks)))
    return crawl_tasks


def start():
    parser = ArgumentParser()
    parser.add_argument("-t", "--total", action="store_true")
    parser.add_argument("-i", "--id", required=True)
    logger.debug("Start to get total tasks. ")
    total_tasks = get_total_tasks()
    args = parser.parse_args()
    if not args.total:
        logger.debug("Start to get task ids of %s. "%args.id)
        task_ids = get_task_ids(args.id)
        tasks = [task for task in total_tasks if str(task["store_task_id"]) in task_ids]
    else:
        tasks = total_tasks
    logger.debug("Start to get taxons, genders, sites, brands. ")
    taxons, genders, sites, brands = get_gender_brand_site_taxon()
    crawl_tasks = dict()
    unregist_tasks = list()
    logger.debug("Enrich crawl tasks and unregist tasks. ")
    for task in tasks:
        find_crawl_tasks = get_crawl_tasks(taxons, genders, sites, brands, **task)
        if find_crawl_tasks:
            for crawl_task in find_crawl_tasks:
                crawl_tasks[crawl_task[0]] = crawl_task
        else:
            logger.debug("Oct task id: %s found crawl task failed. "%task["store_task_id"])
            unregist_tasks.append(task)

    crawl_tasks_file = open("crawl_tasks_%s.csv"%args.id, "w")
    unregist_tasks_file = open("unregist_tasks_%s.csv"%args.id, "w")

    crawl_tasks_file.write("ROC任务ID,来源网站名称,分类名称,性别名称,品牌名称,最后执行时间,最后执行时间差/天\n")
    for crawl_task in crawl_tasks.values():
        crawl_tasks_file.write((",".join(crawl_task)))
        try:
            days = ",%s\n" % (datetime.datetime.now() - datetime.datetime.strptime(crawl_task[-1], "%Y/%m/%d %H:%M:%S")).days
        except ValueError:
            days = ",%s\n" % 0
        crawl_tasks_file.write(days)

    unregist_tasks_file.write("编号,来源网站,品牌,性别,分类,店铺/任务id\n")
    for index, unregist_task in enumerate(unregist_tasks):
        unregist_tasks_file.write(
            "%s,%s,%s,%s,%s,%s/%s\n"%(index+1, unregist_task["source_site_name"],
            unregist_task["brand"], unregist_task["gender"], unregist_task["taxon"],
            unregist_task["store_name"], unregist_task["store_task_id"]))

    crawl_tasks_file.close()
    unregist_tasks_file.close()


if __name__ == "__main__":
    print(get_task_ids("2017-11-29"))