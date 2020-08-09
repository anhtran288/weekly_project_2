from bs4 import BeautifulSoup
import pandas as pd
import requests
import sqlite3
import re
import time

TIKI_URL = 'https://tiki.vn'

conn = sqlite3.connect('tiki.db')
cur = conn.cursor()

# Create table categories in the database using a function
def create_categories_table():
    query = """
        CREATE TABLE IF NOT EXISTS categories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name VARCHAR(255),
            url TEXT, 
            parent_id INTEGER, 
            create_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """
    try:
        cur.execute(query)
        conn.commit()
    except Exception as err:
        print('ERROR BY CREATE TABLE', err)
        
#create_categories_table()

def create_items_table():
    query = """
        CREATE TABLE IF NOT EXISTS items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cat_id,
            cat TEXT,
            product_sku TEXT,
            title VARCHAR(255),
            price_regular INTEGER,
            final_price INTEGER,
            discount INTEGER,
            url TEXT,
            img TEXT,
            no_review INTEGER,
            tikinow TEXT,
            create_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """
    try:
        cur.execute(query)
        conn.commit()
    except Exception as err:
        print('ERROR BY CREATE TABLE', err)

def create_items_table_tikinow():
    query = """
        CREATE TABLE IF NOT EXISTS items_tikinow (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cat_id,
            cat TEXT,
            product_sku TEXT,
            title VARCHAR(255),
            price_regular INTEGER,
            final_price INTEGER,
            discount INTEGER,
            url TEXT,
            img TEXT,
            no_review INTEGER,
            tikinow TEXT,
            create_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """
    try:
        cur.execute(query)
        conn.commit()
    except Exception as err:
        print('ERROR BY CREATE TABLE', err)

# Get the HTML content get_url()
def get_url(url):
    try:
        response = requests.get(url).text
        soup = BeautifulSoup(response, 'html.parser')
        return soup
    except Exception as err:
        print('ERROR BY REQUEST:', err)
        
#get_url(TIKI_URL)

# Instead of using a function to do CRUD on database
# Create a class Category is preferred
# attributes: name, url, parent_id
# instance method: save_into_db()
class Category:
    def __init__(self, name, url, parent_id=None, cat_id=None):
        self.cat_id = cat_id
        self.name = name
        self.url = url
        self.parent_id = parent_id

    def __repr__(self):
        return f"ID: {self.cat_id}, Name: {self.name}, URL: {self.url}, Parent: {self.parent_id}"

    def save_into_db(self):
        query = """
            INSERT INTO categories (name, url, parent_id)
            VALUES (?, ?, ?);
        """
        val = (self.name, self.url, self.parent_id)
        try:
            cur.execute(query, val)
            self.cat_id = cur.lastrowid
            conn.commit()
        except Exception as err:
            print('ERROR BY INSERT:', err)


class Items:
    def __init__(self, cat_id, product_sku, title, cat, price_regular, final_price, discount, url, img, no_review, tikinow = None):
      self.product_sku = product_sku
      self.title = title
      self.cat = cat
      self.price_regular = price_regular
      self.final_price = final_price
      self.discount = discount
      self.url = url
      self.img = img
      self.no_review = no_review
      self.tikinow = tikinow
      self.cat_id = cat_id
    
    def __repr__(self):
        return f"CATID: {self.cat_id} SKU: {self.product_sku} TITLE: {self.title}, CAT: {self.cat}, FINAL_PRICE: {self.final_price}, DISCOUNT: {self.discount}, URL: {self.url}, IMG: {self.img}, NOREVIEW: {self.no_review}, TIKINOW: {self.tikinow}"

    def save_into_db(self):
        query = """
            INSERT INTO items (cat_id, product_sku, title, cat, price_regular, final_price, discount, url, img, no_review, tikinow)
            VALUES (?, ?, ?, ?, ?,?, ?, ?,?, ?, ?);
        """
        val = (self.cat_id, self.product_sku, self.title, self.cat, self.price_regular, self.final_price, self.discount, self.url, self.img, self.no_review, self.tikinow)
        try:
            cur.execute(query, val)
            #self.cat_id = cur.lastrowid
            conn.commit()
        except Exception as err:
            print('ERROR BY INSERT:', err)

    def save_into_db_tikinow(self):
        query = """
            INSERT INTO items_tikinow (cat_id, product_sku, title, cat, price_regular, final_price, discount, url, img, no_review, tikinow)
            VALUES (?, ?, ?, ?, ?,?, ?, ?,?, ?, ?);
        """
        val = (self.cat_id, self.product_sku, self.title, self.cat, self.price_regular, self.final_price, self.discount, self.url, self.img, self.no_review, 1)
        try:
            cur.execute(query, val)
            #self.cat_id = cur.lastrowid
            conn.commit()
        except Exception as err:
            print('ERROR BY INSERT:', err)

# get_main_categories()
def get_main_categories(save_db=False):
    soup = get_url(TIKI_URL)

    result = []
    for a in soup.find_all('a', {'class': 'MenuItem__MenuLink-sc-181aa19-1 fKvTQu'}):
        name = a.find('span', {'class': 'text'}).text
        url = a['href']
        main_cat = Category(name, url)

        if save_db:
            main_cat.save_into_db()
        result.append(main_cat)
    return result


# get_sub_categories() given a parent category
def get_sub_categories(parent_category, save_db=False):
    parent_url = parent_category.url
    result = []

    try:
        soup = get_url(parent_url)
        div_containers = soup.find_all('div', {'class':'list-group-item is-child'})
        for div in div_containers:
            name = div.a.text

            # replace more than 2 spaces with one space
            name = re.sub('\s{2,}', ' ', name)
            name = re.sub('\n', '', name)

            sub_url = TIKI_URL + div.a['href']
            cat = Category(name, sub_url, parent_category.cat_id)
            if save_db:
                cat.save_into_db()
            result.append(cat)
    except Exception as err:
        print('ERROR BY GET SUB CATEGORIES:', err)
    return result

def get_all_categories(categories):
    if len(categories) == 0:
        return
    for cat in categories:
        sub_categories = get_sub_categories(cat, save_db=True)
        print(sub_categories)
        get_all_categories(sub_categories)

# return object Cat from table categories
def get_cat_from_db(cat): 
  cat_id = cat[0]
  url = cat[1]
  name = cat[2]
  return Category(cat_id=cat_id, url = url, name = name)


# generate page url, argument is list of tuple of rows with no child from db, return list of cat objects with page
def url_gen (last_layers, n):
  url_list = []
  for cat in last_layers:
    cat_object = get_cat_from_db(cat)
    for i in range (1,n+1):
      new_url = cat_object.url + '&page=' + str(i)
      new_url_object = Category(cat_id=cat_object.cat_id, url = new_url, name = cat_object.name)
      #if get_detail(url[0]) == 0:
       # break
      #else:
      url_list.append(new_url_object)
  return url_list

def url_gen_tikinow (last_layers, n):
  url_list = []
  for cat in last_layers:
    cat_object = get_cat_from_db(cat)
    for i in range (1,n+1):
      new_url = cat_object.url + '&page=' + str(i) + '&support_p2h_delivery'
      new_url_object = Category(cat_id=cat_object.cat_id, url = new_url, name = cat_object.name)
      #if get_detail(url[0]) == 0:
       # break
      #else:
      url_list.append(new_url_object)
  return url_list

#get items in page with argument is a categ object with page number
def get_detail (cat_url_object, save_db=False):
  result = []
  regex_price = r'[\.đ]'
  regex_review = r'[()nhận\sxét]'

  page = requests.get(cat_url_object.url)
  soup = BeautifulSoup(page.content, 'html.parser')
  
  items = soup.find_all(class_='product-item')
  # if len(items) == 0:
  #   return
  if len(items) !=0:
    for item in items:
      #print (item['product-sku'])
      cat_id = cat_url_object.cat_id
      title = item.a['title']
      product_sku = item['product-sku']
      cat = item['data-category'].split('/')[-1]
      #print(cat)
      url = 'https://tiki.vn'+ item.a['href']
      #price = item.find('span', class_='final-price').get_text()
      final_price = re.sub(regex_price,'',item.find(class_='final-price').contents[0].strip())
      #print (item.find(class_='final-price').contents)
      if item.find('span', class_='price-regular'):
        price_regular= item.find('span', class_='price-regular').get_text()  
        price_regular= int(re.sub(regex_price,'', price_regular))
      else:
        price_regular = ''

      img= item.a.img['src']
      no_review = item.find('p', class_='review').get_text()
      if no_review == 'Chưa có nhận xét':
        no_review = 0
      else: 
        no_review= re.sub(regex_review,'',no_review)
      discount = item.find ('span', class_='sale-tag').get_text().strip() if item.find ('span', class_='sale-tag') else ''
      
      #def __init__(self, title, cat, price_regular, final_price, discount, url, img, no_review, tikinow = None):

      object_item = Items(cat_id, product_sku, title, cat, price_regular, final_price, discount, url, img, no_review,)
      if save_db:
        object_item.save_into_db()
      result.append(object_item)
      #result.append(d)
    
  #df = pd.DataFrame (data = data, columns=data[0].keys())
  return result

def get_detail_tikinow (cat_url_object, save_db=False):
  result = []
  regex_price = r'[\.đ]'
  regex_review = r'[()nhận\sxét]'

  page = requests.get(cat_url_object.url)
  soup = BeautifulSoup(page.content, 'html.parser')
  
  items = soup.find_all(class_='product-item')
  # if len(items) == 0:
  #   return
  if len(items) !=0:
    for item in items:
      #print (item['product-sku'])
      cat_id = cat_url_object.cat_id
      title = item.a['title']
      product_sku = item['product-sku']
      cat = item['data-category'].split('/')[-1]
      #print(cat)
      url = 'https://tiki.vn'+ item.a['href']
      #price = item.find('span', class_='final-price').get_text()
      final_price = re.sub(regex_price,'',item.find(class_='final-price').contents[0].strip())
      #print (item.find(class_='final-price').contents)
      if item.find('span', class_='price-regular'):
        price_regular= item.find('span', class_='price-regular').get_text()  
        price_regular= int(re.sub(regex_price,'', price_regular))
      else:
        price_regular = ''

      img= item.a.img['src']
      no_review = item.find('p', class_='review').get_text()
      if no_review == 'Chưa có nhận xét':
        no_review = 0
      else: 
        no_review= re.sub(regex_review,'',no_review)
      discount = item.find ('span', class_='sale-tag').get_text().strip() if item.find ('span', class_='sale-tag') else ''
      
      #def __init__(self, title, cat, price_regular, final_price, discount, url, img, no_review, tikinow = None):

      object_item = Items(cat_id, product_sku, title, cat, price_regular, final_price, discount, url, img, no_review, 1)
      if save_db:
        object_item.save_into_db_tikinow()
      result.append(object_item)
      #result.append(d)
    
  #df = pd.DataFrame (data = data, columns=data[0].keys())
  return result


#____________MAINTITKINOW____________#

create_categories_table()
create_items_table_tikinow()
cur.execute('DROP TABLE items_tikinow;')
conn.commit()
create_items_table_tikinow()

main_cat = get_main_categories()
#get_all_categories(main_cat)
#last_layers = cur.execute('''SELECT url FROM categories WHERE id NOT IN (SELECT parent_id FROM categories WHERE parent_id IS NOT NULL);''').fetchall()
last_layers = cur.execute('''SELECT id, url, name FROM categories WHERE id NOT IN (SELECT parent_id FROM categories WHERE parent_id IS NOT NULL);''').fetchall()

url_list = url_gen_tikinow(last_layers,2) #list of category object with url having pages

for url in url_list:
  try:
    get_detail_tikinow(url,save_db=True)
  except Exception as err:
        print('ERROR BY GET DETAIL:', err)
  print (url)
  time.sleep(1)