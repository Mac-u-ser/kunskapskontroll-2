# Import modules
import logging
#!pip install wikipedia-api
import wikipediaapi
import databasesaver as dbs
import requests
import pandas as pd


logger = logging.getLogger(__name__)

logging.basicConfig(
    filename='pipeline.log', 
    format='[%(asctime)s][%(name)s] %(message)s', 
    datefmt='%Y-%m-%d %H:%M:%S', 
    level=logging.INFO)
# some constants
URL = f'https://sv.wikipedia.org/w/api.php'
headers = {'user-agent': 'SchizoBot/0.1 (schizoakustik@schizoakustik.se)'}

# get the data from the previous run
logger.info('Starting data pipeline...')
data = dbs.DatabaseLoader()
df = data.load_data()
# read the category
wiki_wiki = wikipediaapi.Wikipedia('UpplandsRunes" (schizoakustik@schizoakustik.se)', 'sv')
wiki_wiki_html = wikipediaapi.Wikipedia('UpplandsRunes" (schizoakustik@schizoakustik.se)', 'sv', extract_format=wikipediaapi.ExtractFormat.HTML)
cat = wiki_wiki.page("Category:Upplands runinskrifter")


count=0
for c in cat.categorymembers.values():
    count = count + 1
    #limit number of pages for exercise purpose 
    if count > 20 : break
    print(c.title)
    inskrift = c.title
    #if the page is new read and save it
    if not (inskrift in df['signum'].values):
        p_wiki = wiki_wiki.page(inskrift)
        tmp = p_wiki.text
            #extract inskription from the text
        if tmp.find('Translitterering av runraden:') == -1:
            logger.info('no transliteration in ', inskrift)
            lit = ' '
        else:
            #len('Translitterering av runraden:') = 29
            lit = tmp[tmp.find('Translitterering av runraden:')+29:tmp.rfind('Normalisering till runsvenska:')].strip()
        if tmp.find('Normalisering till runsvenska:') == -1:
            logger.info('no normalization in ', inskrift)
            norm = ' '
        else:
            norm = tmp[tmp.find('Normalisering till runsvenska:')+30:tmp.rfind('Översättning till nusvenska:')].strip()
        p_wiki_html = wiki_wiki_html.page(inskrift)
        p_wiki_html = wiki_wiki_html.page(inskrift)
        tmp = p_wiki_html.text
        if tmp.find('<p>Översättning\xa0till nusvenska:\n</p>\n<dl><dd>') == -1:
            logger.info('no translation in ', inskrift)
            trans = ' '
        else:
            trans = tmp[tmp.find('<p>Översättning till nusvenska:</p><dl><dd>')+43:tmp.rfind('</dd></dl>')].strip()
        #get revision ID
        params = {'action': 'parse','page': inskrift,'format': 'json'}
        response = requests.get(URL, headers=headers, params=params)
        page_data = response.json()
        #save the page
        df.loc[len(df)]  = [inskrift, page_data['parse']['revid'],lit, norm, trans, 0]   
    else:
        #if the page is newly edited check the changes
        params = {'action': 'parse','page': inskrift,'format': 'json'}
        response = requests.get(URL, headers=headers, params=params)
        page_data = response.json()
        #check revisionID
        if df.loc[df['signum'] == inskrift, 'revisionID'].item() < page_data['parse']['revid']:
            p_wiki = wiki_wiki.page(inskrift)
            tmp = p_wiki.text
            #extract inskription from the text
            if tmp.find('Translitterering av runraden:') == -1:
                logger.info('no transliteration in ', inskrift)
                lit = ' '
            else:
                #len('Translitterering av runraden:') = 29
                lit = tmp[tmp.find('Translitterering av runraden:')+29:tmp.rfind('Normalisering till runsvenska:')].strip()
            if tmp.find('Normalisering till runsvenska:') == -1:
                logger.info('no normalization in ', inskrift)
                norm = ' '
            else:
                norm = tmp[tmp.find('Normalisering till runsvenska:')+30:tmp.rfind('Översättning till nusvenska:')].strip()
            p_wiki_html = wiki_wiki_html.page(inskrift)
            tmp = p_wiki_html.text
            if tmp.find('<p>Översättning\xa0till nusvenska:\n</p>\n<dl><dd>') == -1:
                logger.info('no translation in ', inskrift)
                trans = ' '
            else:
                trans = tmp[tmp.find('<p>Översättning till nusvenska:</p><dl><dd>')+43:tmp.rfind('</dd></dl>')].strip()
            # we keep track of both wiki revisions and our own editions.
            df.loc[df['signum'] == inskrift] = [inskrift, page_data['parse']['revid'], lit, norm, trans, df.loc[df['signum'] == inskrift,'edition'].item() + 1 ]
        
output = dbs.DatabaseSave(df)
output.save_data()

