from db.database import DataBase

class ChosunArchiveDao:
  def __init__(self):
    self.db = DataBase()

  def findRaw(self, year, page):
    query = """
      SELECT id, article_id, year, page, title, link,
        press_date
      FROM chosun_archive
      WHERE year = %s
        AND (content is null or length(content) < 20)
        AND is_photo = false
        AND no_content = false
        AND data_error = false
        AND article_id is not null
    """

    if page != None:
      query = query + f" AND page = '{page}'"

    return self.db.queryList(query, (year))
