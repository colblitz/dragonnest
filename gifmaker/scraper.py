from bs4 import BeautifulSoup
import requests
import re
import pafy
import os.path
from moviepy.editor import *

start_url = "dragonnest.gamepedia.com/Category:Skills_by_Class"
base = "dragonnest.gamepedia.com"
class_skill_regex = "/Category:([a-zA-Z]+[_])+Skills"
youtube_base = "http://www.youtube.com/watch?v="

class_skills = []
skills = {}
classes = set()

def get_links(url):
    links = []
    r = requests.get("http://" + url)
    data = r.text
    soup = BeautifulSoup(data, 'lxml')
    body = soup.find('div', class_="mw-content-ltr")
    for link in body.find_all('a'):
        links.append(link.get('href'))
    return links

def get_youtube(skill_url):
    r = requests.get("http://" + skill_url)
    data = r.text
    soup = BeautifulSoup(data, 'lxml')
    embed = soup.find_all('div', class_="thumb")
    if embed is None:
        return None
    for thing in embed:
        iframe = thing.find('iframe')
        if iframe:
            return iframe.get('src')
    return None

def extract_v(youtube_embed):
    if youtube_embed:
        return re.findall(r"[a-zA-Z0-9\-_]{11}", youtube_embed)[0]
    return ""

def download(v, classname, skill):
    filename = "%s - %s" % (classname, skill.replace("%2B", "EX").replace("%27", ""))
    video = pafy.new(youtube_base + v)
    best = video.getbest(preftype="mp4")
    myfilepath = filename + "." + best.extension
    if not os.path.isfile(myfilepath):
        best.download(filepath=myfilepath)
    return (filename, myfilepath)
    
def convert(filename, path):
    gifpath = filename + ".gif"
    if not os.path.isfile(gifpath):
        VideoFileClip(myfilepath).to_gif(gifpath)
    
for l in get_links(start_url):
    if l and re.match(class_skill_regex, l):
        class_skills.append(l)

for c in class_skills:
    classname = c.replace("/Category:", "").replace("_Skills", "")
    classes.add(classname)
    skills[classname] = []
    for l in get_links(base + c):
        skills[classname].append(l)

maxretry = 5
for c in skills:
    for s in skills[c]:
        if s[1:] not in classes:
            v = extract_v(get_youtube(base + s))
            retries = 0
            filename = ""
            myfilepath = ""
            if v != "":
                print "%s - %s [%s]" % (c, s, v)
                while retries < maxretry:
                    try:
                        (filename, myfilepath) = download(v, c, s[1:])
                        retries = maxretry
                    except:
                        retries += 1
                retries = 0
                while retries < maxretry:
                    try:
                        convert(filename, myfilepath)
                        retries = maxretry
                    except:
                        retries += 1
            else:
                print "%s - %s [   None    ]" % (c, s)
