from bs4 import BeautifulSoup
import requests
import re
import sys
import time
import networkx as nx
import math
import os
import cPickle as pickle
from datetime import date, timedelta

# Constants
base = "http://forum2.nexon.net/"
dn = "forumdisplay.php?62-Dragon-Nest&parenturl=http://dragonnest.nexon.net/community/forum"

# Globals
thread_count = {}
threads_info = {}
threads_left = set()
threads_done = set()
user_posts = {}
user_postcount = {}
year_postcount = {}
mont_postcount = {}
user_relations = {}
user_influence = {}
subforums_done = {}
in_same_thread = 1
following_post = 1
post_has_quote = 1
maxpage_forum = 50

def add_relation(u1, u2, i):
    global user_relations, user_influence
    if u1 == u2:
        return
    key = (min(u1, u2), max(u1, u2))
    if not key in user_relations:
        user_relations[key] = 0
    if not u1 in user_influence:
        user_influence[u1] = 0
    if not u2 in user_influence:
        user_influence[u2] = 0
    user_relations[key] += i
    user_influence[u1] += i
    user_influence[u2] += i

def get_soup(l):
    return BeautifulSoup(requests.get(base + l).text, 'lxml')

def get_next_page(soup, is_forum):
    navclass = "threadpagenav" if is_forum else "pagination_top"
    try:
        prevnext = soup.find('div', class_=navclass).find_all('span', class_="prev_next")
        for pn in prevnext:
            nextbutton = pn.find('a', rel='next')
            if nextbutton:
                return nextbutton.get('href')
    except:
        pass
    return None
                                   
def get_threads(f):
    threads = []
    soup = get_soup(f)
    for t in soup.find_all('li', class_='threadbit'):
        try:
            threadurl = t.find('h3', class_='threadtitle').find('a').get('href')
            postcount = int(t.find('ul', class_='threadstats').find('a').text.replace(',', ''))
            threads.append((threadurl, postcount))
        except:
            pass
    return (threads, get_next_page(soup, True))

def get_all_threads(f, done, maxpage = sys.maxint):
    global thread_count, threads_left
    page = f
    pagecount = 0
    while page and pagecount < maxpage:
        (threads, nextpage) = get_threads(page)
        for thread in threads:
            (threadurl, postcount) = thread
            if threadurl not in threads_info or postcount > thread_count[threadurl]:
                threads_left.add(threadurl)
                thread_count[threadurl] = postcount
            else:
                if done:
                    pagecount = maxpage - 1
        page = nextpage
        pagecount += 1


##def process_post(p, users_in_thread, quotes_list):
##    global user_posts
##    userinfo = p.find('div', class_='userinfo')
##    username = str(userinfo.find('a', class_='username').text)
##    content = str(p.find('blockquote', class_='postcontent').text.encode('ascii', 'ignore'))
##    if username not in user_posts:
##        user_posts[username] = []
##    user_posts[username].append(content)
##
##    users_in_thread.add(username)
##    postcount = int(userinfo.find('dl').find_all('dd')[1].text.replace(",", ""))
##    if username not in user_postcount:
##        user_postcount[username] = postcount
##    else:
##        if user_postcount[username] != postcount:
##            user_postcount[username] = max(postcount, user_postcount[username])
##    quotes = p.find_all('div', class_='quote_container')
##    for q in quotes:
##        try :
##            quoted = str(q.find('div', class_='bbcode_postedby').find('strong').text)
##            quotes_list.append((username, quoted))
##        except:
##            pass
##    return username

def process_post(p):
    rawpostdate = p.find('span', class_='date').text
    postdate = date.today()
    if 'Today' in rawpostdate:
        pass
    elif 'Yesterday' in rawpostdate:
        postdate = date.today() - timedelta(days=1)
    else:
        try:
            month = int(rawpostdate[0:2])
            day = int(rawpostdate[3:5])
            year = int(rawpostdate[6:10])
        except ValueError:
            print rawpostdate
        postdate = date(year, month, day)

    userinfo = p.find('div', class_='userinfo')
    username = str(userinfo.find('a', class_='username').text)
    postcount = int(userinfo.find('dl').find_all('dd')[1].text.replace(",", ""))
    if username not in user_postcount:
        user_postcount[username] = postcount
    else:
        if user_postcount[username] != postcount:
            user_postcount[username] = max(postcount, user_postcount[username])

    if postdate > date.today() - timedelta(days=365):
        if username not in year_postcount:
            year_postcount[username] = postcount
        else:
            if year_postcount[username] != postcount:
                year_postcount[username] = max(postcount, year_postcount[username])

    if postdate > date.today() - timedelta(days=30):
        if username not in mont_postcount:
            mont_postcount[username] = postcount
        else:
            if mont_postcount[username] != postcount:
                mont_postcount[username] = max(postcount, mont_postcount[username])
    
##def process_posts(page, users_in_thread, quotes, post_order):
##    posts = []
##    soup = get_soup(page)
##    for p in soup.find_all('li', class_='postcontainer'):
##        post_order.append(process_post(p, users_in_thread, quotes))
##    return get_next_page(soup, False)

def process_posts(page):
    soup = get_soup(page)
    for p in soup.find_all('li', class_='postcontainer'):
        process_post(p)
    return get_next_page(soup, False)

##def process_thread(t, maxpage = sys.maxint):
##    page = t
##    pagecount = 0
##    users_in_thread = set()
##    quotes = []
##    post_order = []
##    while page and pagecount < maxpage:
##        page = process_posts(page, users_in_thread, quotes, post_order)
##        pagecount += 1
##    # all thread info gotten
##    threads_info[t] = (users_in_thread, post_order, quotes)

def process_thread(t, maxpage = sys.maxint):
    page = t
    pagecount = 0
    while page and pagecount < maxpage:
        page = process_posts(page)
        pagecount += 1
    threads_done.add(t)
    
def calculate_thread(t):
    (users_in_thread, post_order, quotes) = threads_info[t]
    uit = list(users_in_thread)
    for i in xrange(len(uit)):
        for j in xrange(i+1, len(uit)):
            add_relation(uit[i], uit[j], in_same_thread)
    for i in xrange(len(post_order)-1):
        add_relation(post_order[i+1], post_order[i], following_post)
    for q in quotes:
        add_relation(q[0], q[1], post_has_quote)

def get_pstring():
    return "%d-%d-%d-%d" % (in_same_thread, following_post, post_has_quote, maxpage_forum)

def save_state(done):
    all_values = {}
    all_values['thread_count'] = thread_count
    all_values['threads_info'] = threads_info
    all_values['threads_left'] = threads_left
    all_values['user_posts'] = user_posts
    all_values['user_postcount'] = user_postcount
    all_values['user_relations'] = user_relations
    all_values['user_influence'] = user_influence
    all_values['subforums_done'] = subforums_done
    all_values['in_same_thread'] = in_same_thread
    all_values['following_post'] = following_post
    all_values['post_has_quote'] = post_has_quote
    all_values['done'] = done
    save_path = get_pstring() + "-state.p"
    pickle.dump(all_values, open(save_path, 'wb'))

def read_state():
    global thread_count, threads_info, threads_left, user_postcount, user_relations, user_influence
    global subforums_done, in_same_thread, following_post, post_has_quote, user_posts
    try:
        save_path = get_pstring() + "-state.p"
        all_values = pickle.load(open(save_path, 'rb'))
        thread_count = all_values['thread_count']
        threads_info = all_values['threads_info']
        threads_left = all_values['threads_left']
        user_posts = all_values['user_posts']
        user_postcount = all_values['user_postcount']
        user_relations = all_values['user_relations']
        user_influence = all_values['user_influence']
        subforums_done = all_values['subforums_done']
        in_same_thread = all_values['in_same_thread']
        following_post = all_values['following_post']
        post_has_quote = all_values['post_has_quote']
        print all_values['done']
        return all_values['done']
    except:
        pass
    return False

def produce_graph(cutoff, transform):
    G = nx.Graph()
    allusers = set()
    for c in user_relations:
        allusers.add(c[0])
        allusers.add(c[1])
    for u in allusers:
        G.add_node(u, weight = user_influence[u])
    user_top = {}
    for c in user_relations:
        user1 = c[0]
        user2 = c[1]
        score = user_relations[c]
        if user1 not in user_top:
            user_top[user1] = {}
        if user2 not in user_top:
            user_top[user2] = {}
        user_top[user1][user2] = score
    for u in user_top:
        top5 = dict(sorted(user_top[u].items(), key=lambda x: x[1], reverse=True)[:5])
        for j in top5:
            G.add_edge(u, j, weight = user_top[u][j])
            
##        user_top[c[0]]
##        if user_relations[c] > cutoff:
##            G.add_edge(c[0], c[1], weight = transform(user_relations[c]))
    nx.write_graphml(G, 'graph' + time.strftime("%Y-%m-%d-%H%M%S", time.gmtime()) + '.graphml')

#cutoffs = [1, 5, 10, 50, 100, 500, 1000, 5000, 10000, 50000, 100000, 500000]
cutoffs = [1, 10, 100, 1000, 10000, 100000, 1000000]
def find_post_index(n):
    i = 0
    while n > cutoffs[i]:
        i += 1
    return i

user_eyvn = {}
def main():
    # check for current state
    if not read_state():
        start = time.time()
        subforums = []
        soup = BeautifulSoup(requests.get(base + dn).text, 'lxml')
        for f in soup.find_all('h2', class_="forumtitle"):
            subforums.append(f.find('a').get('href'))
        print "time to get subforums: ", time.time() - start

        start = time.time()
        for f in subforums:
            subforums_done[f] = False
            get_all_threads(f, subforums_done[f], maxpage_forum)
        print "time to get threads from subforums: ", time.time() - start

        print "threads left: " + str(len(threads_left))

        start = time.time()
        threads_processed = 0
        while threads_left:
            thread = threads_left.pop()
            if thread not in threads_done:
                process_thread(thread)
                threads_processed += 1
                if threads_processed % 5 == 0:
                    print ".",
                    time.sleep(2)
                    if threads_processed % 50 == 0:
                        save_state(False)
                        elapsed = time.time() - start
                        speed = elapsed / threads_processed
                        estimate = len(threads_left) * speed
                        print " eta: " + str(estimate)
                        
        print "time to process " + str(threads_processed) + " threads: ", time.time() - start
        all_done = True
        save_state(True)

    postcounts = [[] for i in xrange(len(cutoffs))]
    yearcounts = [[] for i in xrange(len(cutoffs))]
    montcounts = [[] for i in xrange(len(cutoffs))]
    
    total = 0.0
    users = 0.0
    for u in user_postcount:
        c = user_postcount[u]
        total += c
        users += 1
        postcounts[find_post_index(c)].append(c)

    yeartotal = 0.0
    yearusers = 0.0
    for u in year_postcount:
        c = year_postcount[u]
        yeartotal += c
        yearusers += 1
        yearcounts[find_post_index(c)].append(c)

    monttotal = 0.0
    montusers = 0.0
    for u in mont_postcount:
        c = mont_postcount[u]
        monttotal += c
        montusers += 1
        montcounts[find_post_index(c)].append(c)

        
    results = open("results.txt", 'w')
    results.write("All:\n")
    for i in xrange(len(cutoffs)):
        results.write("%7d: (%4d - %f) %7d (%f)\n" % (cutoffs[i], len(postcounts[i]), len(postcounts[i])/users, sum(postcounts[i]), sum(postcounts[i])/total))
    results.write("Year:\n")
    for i in xrange(len(cutoffs)):
        results.write("%7d: (%4d - %f) %7d (%f)\n" % (cutoffs[i], len(yearcounts[i]), len(yearcounts[i])/users, sum(yearcounts[i]), sum(yearcounts[i])/total))
    results.write("Mont:\n")
    for i in xrange(len(cutoffs)):
        results.write("%7d: (%4d - %f) %7d (%f)\n" % (cutoffs[i], len(montcounts[i]), len(montcounts[i])/users, sum(montcounts[i]), sum(montcounts[i])/total))
    results.close()
        
        
    
##    start = time.time()
##    for t in threads_info:
##        calculate_thread(t)
##    print "time to calculate " + str(len(threads_info)) + " threads: ", time.time() - start
##
##    sortedu = sorted([(str(x[0]), x[1]) for x in user_postcount.items()], key=lambda x: x[1], reverse=True)
##
##    for u in sortedu[:50]:
##        pass
##        #print "%5d - %s" % (u[1], u[0])
##    #print ""
##
##    sortedr = sorted(user_relations.items(), key=lambda x: x[1], reverse=True)
##    for c in sortedr[:100]:
##        p1 = float(c[1])/user_influence[c[0][0]]
##        p2 = float(c[1])/user_influence[c[0][1]]
##        if p1 > 0.5 or p2 > 0.5:
##            print c
##            print "%5f, %5f" % (p1, p2)
##
##    produce_graph(1, lambda x: x)
##    #produce_graph(1, lambd x: math.log(x))
##
##    print len(user_posts)
##    # To find the number of times each person has mentioned eyvn
##    for u in user_posts:
##        user_eyvn[u] = 0
##        for p in user_posts[u]:
##            if p.__contains__('Eyvn') or p.__contains__('eyvn'):
##                user_eyvn[u] += 1
##
##    for u in user_eyvn:
##        if user_eyvn[u] > 10:
##            print user_eyvn[u], u
                
if __name__ == "__main__":
    main()
