import requests
from datetime import datetime
import boto3
import pandas as pd
from bs4 import BeautifulSoup
from lxml import etree

date = datetime.now()

def uploadNewsHtmlS3(url):
    """
    La siguiente función sube un archivo HTML con destino bucket en S3.

    Args:
        url: Parametro que hace referencia a la página web de la cual se desea 
        extraer la información.
    """
    
    s3 = boto3.resource('s3')
    url = url
    newspeaper = url.split(".")[1]
    headers = {"Accept": "application/json"}
    response = requests.request("GET", url, headers=headers)
    localPath = f'/tmp/{newspeaper}_raw.html'
    myfile = open(localPath,'w', encoding='utf-8')
    myfile.write(str(response.text))
    myfile.close()
    s3Path =  f'headlines/raw/periodico={newspeaper}/year={date.year}/month={date.month}/day={date.day}/{newspeaper}.html'
    s3.meta.client.upload_file(localPath, 'parcial-3er-corte', s3Path)
    

def getEltiempoNews():
    """
    La siguiente función procesa el archivo eltiempo.HTML que se encuentra en el direcctorio 
    temporal de Lambda, para ello utiliza los XPATH de la categoria, el título y 
    extrae el texto, url.

    Exceptions: 
        ValueError: Cuando el tamaño de las listas no coincide después de procesar los titulos,
        se llenan los espacios vacios con 0.
    """

    file = open('/tmp/eltiempo.html', encoding="utf-8")
    soup = BeautifulSoup(file, "html.parser")

    titles = []
    categories = []
    urls = []

    for article in soup.find_all("article"):
        try:
            category = article.find(class_='category-published').find('a')
            categories.append(category.get_text())
            title = article.find(class_='title-container').find(class_='title page-link')
            titles.append(title.get_text())
            url = article.find(class_='title-container').find(class_='title page-link')['href']
            urls.append(url)
        except:
            pass

    urls = list(map(lambda x: f'https://www.eltiempo.com{x}', urls))
    columns = {'titles': titles, 'categories':categories, 'urls':urls}
    df = pd.DataFrame(columns)
    df.to_csv('/tmp/eltiempo.csv', index=False, encoding='utf-8', sep=';')


def getPublimetroNews():
    """
    La siguiente función procesa el archivo publimetro.HTML que se encuentra en el direcctorio 
    temporal de Lambda, para ello utiliza los XPATH de la categoria, el título y 
    extrae el texto, url.

    Exceptions: 
        ValueError: Cuando el tamaño de las listas no coincide después de procesar los titulos,
        se llenan los espacios vacios con 0.
    """

    file = open('/tmp/publimetro.html', encoding="utf-8")
    soup = BeautifulSoup(file, "html.parser")

    titles = []
    categories = []
    urls = []

    for article in soup.find_all("article"):
        try:
            category_aux = article.find('span')
            if category_aux == None:
                category = t
            else:
                category = category_aux.get_text()
                t = category
            categories.append(category)
            title_aux = article.find('h3')
            if title_aux == None:
                title_aux = article.find('h2')
            title = title_aux.get_text()
            titles.append(title)
            url = article.find('a') 
            url = url['href']
            if url[0] == '/':
                url =  "https://www.publimetro.co/"+str(url)
            urls.append(url)
        except:
            pass

    columns = {'titles': titles, 'categories':categories, 'urls':urls}
    df = pd.DataFrame(columns)
    df.to_csv('/tmp/publimetro.csv', index=False, encoding='utf-8', sep=';')
    

def downloadNewsHtmlS3(newspeaper):
    """
    La siguiente función descarga un archivo HTML desde un bucket en S3.

    Args:
        newspeaper: Parametro que hace referencia al nombre del periorico, que a 
        su vez es el nombre del archivo alojado en S3 y el que se guarda en el lambda.
    """
    s3 = boto3.client('s3')
    s3Path = f'headlines/raw/periodico={newspeaper}/year={date.year}/month={date.month}/day={date.day}/{newspeaper}.html'
    localPath = f'/tmp/{newspeaper}.html'
    s3.download_file('parcial-3er-corte',s3Path,localPath)

def uploadNewscsvS3(newspeaper):
    """
    La siguiente función sube un archivo CSV con destino bucket en S3.

    Args:
        newspeaper: Parametro que hace referencia al nombre del periorico, que a 
        su vez es el nombre del archivo que será alojado en S3.
    """
    s3 = boto3.resource('s3')
    s3Path = f'headlines/final/periodico={newspeaper}/year={date.year}/month={date.month}/day={date.day}/{newspeaper}.csv'
    localPath = f'/tmp/{newspeaper}.csv'
    s3.meta.client.upload_file(localPath, 'parcial-3er-corte', s3Path)


def downloadNewscsvS3(newspeaper):
    """
    La siguiente función descarga un archivo CSV desde un bucket en S3.

    Args:
        newspeaper: Parametro que hace referencia al nombre del periorico, que a 
        su vez es el nombre del archivo alojado en S3 y el que se guarda en el lambda.
    """
    s3c = boto3.client('s3')
    s3Path = f'headlines/final/periodico={newspeaper}/year={date.year}/month={date.month}/day={date.day}/{newspeaper}.csv'
    localPath = f'/tmp/{newspeaper}.csv'
    s3c.download_file('parcial-3er-corte',s3Path,localPath)

def uploadNewscsvS3RAW(newspeaper):
    """
    La siguiente función sube un archivo CSV con destino bucket en S3.

    Args:
        newspeaper: Parametro que hace referencia al nombre del periorico, que a 
        su vez es el nombre del archivo que será alojado en S3.
    """
    s3 = boto3.resource('s3')
    s3Path = f'headlines/final/periodico={newspeaper}/year={date.year}/month={date.month}/day={date.day}/{newspeaper}.csv'
    localPath = f'/tmp/{newspeaper}.csv'
    s3.meta.client.upload_file(localPath, 'parcial-3er-corte', s3Path)


if __name__ == "__main__":
    uploadNewsHtmlS3('https://www.eltiempo.com/')
    uploadNewsHtmlS3('https://www.publimetro.co/')
    downloadNewsHtmlS3('eltiempo')
    downloadNewsHtmlS3('publimetro')
    getEltiempoNews()
    getPublimetroNews()
    uploadNewscsvS3('eltiempo')
    uploadNewscsvS3('publimetro')
    downloadNewscsvS3('eltiempo')
    downloadNewscsvS3('publimetro')
    uploadNewscsvS3RAW('eltiempo')
    uploadNewscsvS3RAW('publimetro')

