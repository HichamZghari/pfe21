from bs4 import BeautifulSoup, SoupStrainer
import requests
from sys import argv, exit
import configparser


## verification parameter
if len(argv) != 2 :
    print("error need an ini file as parameter")
    exit(1)

## get path finess ini file
path = argv[1]


## config for ini file
config = configparser.ConfigParser()
config.sections()
config.read(path)


url = "https://www.data.gouv.fr/fr/datasets/mon-reseau-mobile/#_"

page = requests.get(url)    
data = page.text
soup = BeautifulSoup(data,features="html.parser")


list_a = []
for link in soup.find_all('a'):
    list_a.append(str(link))

urls = []
for e in list_a:
    if e.find('sites-5g') != -1:
        urls.append(e)


url = urls[0]


url = url.split("')")[0]
url = url.split("url=")[1]

url = url.replace("%2F","/")
url = url.replace("%3A",":")

print(url)

config.set("Data","urls", url)


with open(path, 'w') as configfile:
    config.write(configfile)
