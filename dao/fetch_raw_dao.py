from db.database import DataBase

class FetchRawDao:
  def __init__(self):
    self.db = DataBase()

  def queryBigKinds(self, year = None, page = None):
    query = """
      SELECT 
        press_date AS date, title, content
      FROM bigkind_raw
        WHERE content is not null
    """

    if year != None:
      query = query + f" AND year = '{year}'"

    if page != None:
      query = query + f" AND page = '{page}'"

    return self.db.queryList(query,())


  def queryChosun(self, year = None, page = None):
    query = """
      SELECT 
        press_date AS date, title, content
      FROM chosun_archive 
        WHERE content is not null AND length(content) > 20
    """

    if year != None:
      query = query + f" AND year = '{year}'"

    if page != None:
      query = query + f" AND page = '{page}'"

    return self.db.queryList(query,())