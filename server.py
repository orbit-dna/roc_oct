#!/home/longen/.pyenv/shims/python
# -*- coding:utf-8 -*-
"""
暂时废弃
"""
import os
import time
from bottle import run, route, request, template, redirect, static_file

project_path = os.path.dirname(__file__)


@route('/')
def index():
    id = request.GET.get("id", "")
    return template(os.path.join(project_path, "index.html"), id=id)


@route('/search')
def search():
    id = request.GET.get("date")
    if not id:
        id = time.strftime("%Y%m%d")
        total = "-t"
    else:
        try:
            time.strptime(id, "%Y%m%d")
        except Exception:
            return redirect('/?id=%s'%id)
        total = ""
    try:
        os.unlink("crawl_tasks_%s.csv"%id)
        os.unlink("unregist_tasks_%s.csv" % id)
        os.unlink("%s.zip" % id)
    except Exception:
        pass
    os.popen("%s -i %s %s"%(os.path.join(project_path, "roc_oct.py"), id, total))
    return {"id": id}


@route("/check")
def check():
    id = request.GET.get("id")
    if os.path.exists("crawl_tasks_%s.csv"%id):
        return {"finished": True}
    else:
        return {"finished": False}


@route("/download")
def download():
    id = request.GET.get("id")
    os.system("zip -j %s.zip crawl_tasks_%s.csv unregist_tasks_%s.csv"%(id, id, id))
    return static_file("%s.zip"%id, ".", download=True)


if __name__ == "__main__":
    run(host=os.environ.get("HOST", "127.0.0.1"), port=8888)