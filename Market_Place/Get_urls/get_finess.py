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


url = "https://www.data.gouv.fr/fr/datasets/finess-extraction-du-fichier-des-etablissements/#resource-9b81484a-0deb-42f7-a7c4-eb9869ea580a"

page = requests.get(url)    
data = page.text
soup = BeautifulSoup(data,features="html.parser")


list_a = []
for link in soup.find_all('a'):
    list_a.append(str(link))


for e in list_a:
    if e.find('cs1100502') != -1:
        url = e

url = url.split("')")[0]
url = url.split("url=")[1]

url = url.replace("%2F","/")
url = url.replace("%3A",":")


config.set("Data","urls", url)


with open(path, 'w') as configfile:
    config.write(configfile)

