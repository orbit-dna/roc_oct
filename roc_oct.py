# -*- coding:utf-8 -*-
import os
import time
import json
import bs4
import signal
import datetime
from argparse import ArgumentParser
from urllib.parse import urlencode, urljoin
from requests.sessions import Session

from multiprocessing import Queue, Process

tasks = Queue()

session = Session()

from utils import retry_wrapper


def close_handler(*args, **kwargs):
    os.kill(os.getpid(), 9)


def interrupt_handler(*args, **kwargs):
    raise KeyboardInterrupt

get = retry_wrapper(5, exception=(Exception, KeyboardInterrupt), error_handler=lambda *args, **kwargs: print("error in requests.get: %s"%str(args)))(session.get)

oct_adr = "http://octopus.app.jinanlongen.com/store_tasks/tb_new_arrival_search"

oct_params = dict(tuple(line.strip().split(":", 1)) for line in """utf8:✓
upload_date:计划上新时间
planned_upload_step_end_at:
store_id:
status:请选择
cycle_grade:请选择
store_task_id:""".split("\n"))

oct_headers = {
    "Cookie": "_session_id=57decd1935c294f27f973ac6764c8915"
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
    "Cookie": "_roc_session=R0NwejZ1emFCYndKdGZWSU1OaGVtOVZDWEpHVFlZL0VyaUFwU1lRUGJrTmE4U2N4Z2xOZG16NmJHbGZwN1NNZ0Y2V0JZOFZ0RVo0YmtZaldoZXdsMHpjRE1GNmNhUUs0bTFVcVhwMEhQaU5NdENhUTFTTjl0dXVWMlhmZWY5VUNVK0w5UFRucEVlZW10YkVlSnhmblFCYkt6UDhZdXJkVnhKR3BVb05EcTM2aC9DMkFUN0tGNTZMdVhxclpFelQvWU1aQ2NWOWxWbElSKzZpWkJ4Z2MzNXdBR0F0ZUFzYjc0anVOalFIYzZvS2had1JiR3c3eUNhSXJZdDJ5VlZOWlBHdFhVVCtUQ2lMTjZNRkxSUHVERXFkcFFmaVFVTXJtb2NsZUZZTnQ1aXNWOVhBOEFscGlUbVZIZ3Q0Y3R1UUp2WGhSUXF4cldHVDBXNGdUUm9rVkpCZDVQZFRyRFVOem5lWWdNbVlOeUphSGxLVmY2Tll0MCtvSEI5OXB5YjZ5Q2MzVVJrTzYySk9DdVl5WEJZVXhGblp5N0hySVJPVGVhSzRDMk9KUHY4L04rR2FPbHo2NkdZTmd3ZlRkWkZMYTZYWmZjQWdHVVlWVGJCMG9DQW1ud3ZpeXpYRTYyVEhKTHAxOG94eVJkV1Y1eC82cUhBVTVFQ2EzMnAvWFpDV1FGSGZ1Q3R2cHhkR3lKMXR1UHVlZ0NRei92WFdZSTV2blN0S0g5S1pSMEZrPS0tSVFWTTRBMkozU0c5OEdaemszZllrQT09--64524b5012214aef121cdecb2732248dcc03a96a"
}

oct_total_adr = "http://192.168.21.63:10000/store_tasks.json"


taxon_adr = "http://roc.app.jinanlongen.com/taxons/%s/children.json"


def get_task_ids():
    date = (datetime.datetime.now() + datetime.timedelta(days=2)).strftime("%Y-%m-%d")
    oct_params["planned_upload_step_end_at"] = date
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
        taxons[option.text] = int(value)
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

    while True:
        count = 1
        while True:
            taxon = ts[-1*count]
            if taxon:
                if taxon in taxons:
                    taxon_code = taxons[taxon]
                    break
                else:
                    count += 1
            else:
                break

        if taxon == ts[-1]:
            break

        resp = get(taxon_adr%taxon_code, headers=roc_headers)
        if resp.status_code == 200:
            children = json.loads(resp.text)
            for child in children:
                taxons[child["name"]] = child["id"]

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
    print("Oct task id: %s, Roc task count: %s. "%(kwargs["store_task_id"], len(crawl_tasks)))
    return crawl_tasks


def control(pid):
    length = tasks.qsize()
    print("tasks len: %s"%length)
    def close(*args, **kwargs):
        os.kill(pid, signal.SIGTERM)
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, close)
    signal.signal(signal.SIGINT, close)
    while True:
        time.sleep(10)
        current_length = tasks.qsize()
        print("Current length: %s"%current_length)
        if current_length:
            if current_length == length:
                print("Send signal.SIGINT to child. ")
                os.kill(pid, signal.SIGINT)
            else:
                length = current_length
        else:
            break


def listen():
    signal.signal(signal.SIGTERM, close_handler)
    signal.signal(signal.SIGINT, interrupt_handler)


def enrich_tasks():
    parser = ArgumentParser()
    parser.add_argument("-t", "--total", action="store_true")
    print("Start to get total tasks. ")
    total_tasks = get_total_tasks()
    if not parser.parse_args().total:
        print("Start to get task ids the day after tomorrow. ")
        task_ids = get_task_ids()
        ts = [task for task in total_tasks if str(task["store_task_id"]) in task_ids]
    else:
        ts = total_tasks

    for task in ts:
        tasks.put(task)


def run():
    listen()
    print("Start to get taxons, genders, sites, brands. ")
    taxons, genders, sites, brands = get_gender_brand_site_taxon()
    crawl_tasks = dict()
    unregist_tasks = list()
    print("Enrich crawl tasks and unregist tasks. ")
    # have_got = False
    while tasks.qsize():
        task = tasks.get_nowait()
        if task["store_task_id"] in ["1656", 1656]:
            continue
        # if not (have_got or task["store_task_id"] in ["199", 199]):
        #     continue
        # have_got = True
        find_crawl_tasks = get_crawl_tasks(taxons, genders, sites, brands, **task)
        if find_crawl_tasks:
            for crawl_task in find_crawl_tasks:
                crawl_tasks[crawl_task[0]] = crawl_task
        else:
            print("Oct task id: %s found crawl task failed. " % task["store_task_id"])
            unregist_tasks.append(task)

    crawl_tasks_file = open("crawl_tasks_%s.csv" % datetime.datetime.now().strftime("%Y%m%d%H%M%S"), "w")
    unregist_tasks_file = open("unregist_tasks_%s.csv" % datetime.datetime.now().strftime("%Y%m%d%H%M%S"), "w")

    crawl_tasks_file.write("ROC任务ID,来源网站名称,分类名称,性别名称,品牌名称,最后执行时间,最后执行时间差/天\n")
    for crawl_task in crawl_tasks.values():
        crawl_tasks_file.write((",".join(crawl_task)))
        try:
            days = ",%s\n" % (
            datetime.datetime.now() - datetime.datetime.strptime(crawl_task[-1], "%Y/%m/%d %H:%M:%S")).days
        except ValueError:
            days = 0
        crawl_tasks_file.write(days)

    unregist_tasks_file.write("编号,来源网站,品牌,性别,分类,店铺/任务id\n")
    for index, unregist_task in enumerate(unregist_tasks):
        unregist_tasks_file.write(
            "%s,%s,%s,%s,%s,%s/%s\n" % (index + 1, unregist_task["source_site_name"],
                                        unregist_task["brand"], unregist_task["gender"], unregist_task["taxon"],
                                        unregist_task["store_name"], unregist_task["store_task_id"]))

    crawl_tasks_file.close()
    unregist_tasks_file.close()


def start():
    enrich_tasks()
    child = Process(target=run)
    child.start()
    pid = child.pid
    control(pid)





if __name__ == "__main__":
    start()