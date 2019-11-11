from dao.fetch_raw_dao import FetchRawDao
from datetime import date
import json
import gzip

class ArchiveJsonFile:
  def __init__(self):
    self.dao = FetchRawDao()

  def archive_bigkinds(self):
    datas = self.dao.queryBigKinds()

    for data in datas:
      data["date"] = data["date"].strftime("%Y-%m-%d")

    self._save_json_to_gz("02_bigkinds_all.json.gz", datas)

  def archive_chosun(self):
    datas = self.dao.queryChosun(year = None, page = None)

    for data in datas:
      data["date"] = data["date"].strftime("%Y-%m-%d")

    self._save_json_to_gz("02_chosun_all.json.gz", datas)

  def archive_all(self):
    print("start all data to json...")
    datas_chosun = self.dao.queryChosun(year = None, page = None)
    datas_bigkinds = self.dao.queryBigKinds(year = None, page = None)

    for data in datas_chosun:
      data["date"] = data["date"].strftime("%Y-%m-%d")

    for data in datas_bigkinds:
      data["date"] = data["date"].strftime("%Y-%m-%d")

    all_datas = {
      "chosun" : datas_chosun,
      "bigkinds" : datas_bigkinds
    }

    self._save_json_to_gz("01_total_all.json.gz", all_datas)

  def archive_bigkinds_by_gov(self):
    print("start bigkinds by gov data to json...")
    datas_bigkinds = self.dao.queryBigKinds(year=None, page=None)
    splited_datas = self._split_by_gov(datas_bigkinds)

    self._save_json_to_gz("03_bigkinds_1998_dj.json.gz", splited_datas[0])
    self._save_json_to_gz("03_bigkinds_2003_cy.json.gz", splited_datas[1])
    self._save_json_to_gz("03_bigkinds_2008_mb.json.gz", splited_datas[2])
    self._save_json_to_gz("03_bigkinds_2013_gh.json.gz", splited_datas[3])
    self._save_json_to_gz("03_bigkinds_2017_cr.json.gz", splited_datas[4])

  def archive_chosun_by_gov(self):
    print("start chosun by gov data to json...")
    datas_chosun = self.dao.queryChosun(year=None, page=None)
    splited_datas = self._split_by_gov(datas_chosun)

    self._save_json_to_gz("03_chosun_1998_dj.json.gz", splited_datas[0])
    self._save_json_to_gz("03_chosun_2003_cy.json.gz", splited_datas[1])
    self._save_json_to_gz("03_chosun_2008_mb.json.gz", splited_datas[2])
    self._save_json_to_gz("03_chosun_2013_gh.json.gz", splited_datas[3])
    self._save_json_to_gz("03_chosun_2017_cr.json.gz", splited_datas[4])

  def _save_json_to_gz(self, filename, data):
    json_strs = json.dumps(data, ensure_ascii=False) + "\n"
    json_bytes = json_strs.encode('utf-8')

    with gzip.GzipFile(filename, mode='w') as fout:
      fout.write(json_bytes)

  def _split_by_gov(self, datas):
    splitted = [
      list(filter(lambda x: x["date"] >= date(1998, 2, 25) and x["date"] < date(2003, 2, 25), datas)),
      list(filter(lambda x: x["date"] >= date(2003, 2, 25) and x["date"] < date(2008, 2, 25), datas)),
      list(filter(lambda x: x["date"] >= date(2008, 2, 25) and x["date"] < date(2013, 2, 25), datas)),
      list(filter(lambda x: x["date"] >= date(2013, 2, 25) and x["date"] < date(2017, 5, 10), datas)),
      list(filter(lambda x: x["date"] >= date(2017, 5, 10) and x["date"] < date(2019, 8, 26), datas))
    ]

    for sp in splitted:
      for data in sp:
        data["date"] = data["date"].strftime("%Y-%m-%d")

    return splitted

if __name__ == "__main__":
  a = ArchiveJsonFile()

  #a.archive_all()
  #a.archive_bigkinds()
  #a.archive_chosun()
  a.archive_bigkinds_by_gov()
  a.archive_chosun_by_gov()