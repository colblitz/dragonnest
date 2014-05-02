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
import threading

# Constants
base = "http://forum2.nexon.net/"
dn = "forumdisplay.php?62-Dragon-Nest&parenturl=http://dragonnest.nexon.net/community/forum"

# Globals
thread_count = {} #posts in each thread
subforums = set()
threads_left = set()
threads_done = set()
user_postcount = {}
year_postcount = {}
mont_postcount = {}
maxpages_forum = 50
maxpages_thread = sys.maxint #sys.maxint
thread_threads = 15
subforum_threads = 5
threads_start = None
threads_processed = 0

thread_count_lock = threading.Lock()
threads_left_lock = threading.Lock()
threads_done_lock = threading.Lock()
user_postcount_lock = threading.Lock()
year_postcount_lock = threading.Lock()
mont_postcount_lock = threading.Lock()
subforums_lock = threading.Lock()

def get_next_subforum():
    subforums_lock.acquire()
    try:
        if subforums:
            return subforums.pop()
        return None
    finally:
        subforums_lock.release()

def add_thread(t):
    threads_left_lock.acquire()
    try:
        threads_left.add(t)
    finally:
        threads_left_lock.release()

def get_next_thread():
    threads_left_lock.acquire()
    try:
        if threads_left:
            return threads_left.pop()
        return None
    finally:
        threads_left_lock.release()

def is_thread_done(t):
    threads_done_lock.acquire()
    try:
        return t in threads_done
    finally:
        threads_done_lock.release()

def finish_thread(t):
    global threads_processed, threads_start
    threads_done_lock.acquire()
    try:
        threads_done.add(t)
        threads_processed += 1
        if threads_processed % 5 == 0:
            print ".",
            time.sleep(2)
            if threads_processed % 50 == 0:
                save_state(False)
                elapsed = time.time() - threads_start
                speed = elapsed / threads_processed
                estimate = len(threads_left) * speed
                print " eta: " + str(estimate)
    finally:
        threads_done_lock.release()

def set_thread_count(t, c):
    thread_count_lock.acquire()
    try:
        thread_count[t] = c
    finally:
        thread_count_lock.release()

def get_thread_count(t):
    thread_count_lock.acquire()
    try:
        if t in thread_count:
            return thread_count[t]
        return 0
    finally:
        thread_count_lock.release()

def add_to_user(u, c):
    user_postcount_lock.acquire()
    try:
        if u not in user_postcount:
            user_postcount[u] = c
        else:
            if user_postcount[u] != c:
                user_postcount[u] = max(c, user_postcount[u])
    finally:
        user_postcount_lock.release()

def add_to_year(u, c):
    year_postcount_lock.acquire()
    try:
        if u not in year_postcount:
            year_postcount[u] = c
        else:
            if year_postcount[u] != c:
                year_postcount[u] = max(c, year_postcount[u])
    finally:
        year_postcount_lock.release()

def add_to_mont(u, c):
    mont_postcount_lock.acquire()
    try:
        if u not in mont_postcount:
            mont_postcount[u] = c
        else:
            if mont_postcount[u] != c:
                mont_postcount[u] = max(c, mont_postcount[u])
    finally:
        mont_postcount_lock.release()

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

class subforumThread(threading.Thread):
    def __init__(self, threadId):
        threading.Thread.__init__(self)
        self.threadId = threadId
    def run(self):
        print "Starting subforum thread " + str(self.threadId) + "\n"
        subforum = get_next_subforum()
        while (subforum):
            print ("%d processing subforum " % self.threadId) + subforum
            get_all_threads(subforum, maxpages_forum)
            subforum = get_next_subforum()
        print "Subforum thread %d done" % self.threadId + "\n"

def get_all_threads(f, maxpage = sys.maxint):
    page = f
    pagecount = 0
    while page and pagecount < maxpage:
        (threads, nextpage) = get_threads(page)
        for thread in threads:
            (threadurl, postcount) = thread
            if not is_thread_done(threadurl) or postcount > get_thread_count(threadurl):
                add_thread(threadurl)
                set_thread_count(threadurl, postcount)
            else:
                if done:
                    pagecount = maxpage - 1
        page = nextpage
        pagecount += 1

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

class threadThread(threading.Thread):
    def __init__(self, threadId):
        threading.Thread.__init__(self)
        self.threadId = threadId
    def run(self):
        print "Starting thread thread " + str(self.threadId) + "\n"
        thread = get_next_thread()
        while (thread):
            if not is_thread_done(thread):
                process_thread(thread, maxpages_thread)
            thread = get_next_thread()
        print "Thread thread %d done" % self.threadId + "\n"

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

    add_to_user(username, postcount)
    if postdate > date.today() - timedelta(days=365):
        add_to_year(username, postcount)

    if postdate > date.today() - timedelta(days=30):
        add_to_mont(username, postcount)

def process_posts(page):
    soup = get_soup(page)
    for p in soup.find_all('li', class_='postcontainer'):
        process_post(p)
    return get_next_page(soup, False)

def process_thread(t, maxpage = sys.maxint):
    page = t
    pagecount = 0
    while page and pagecount < maxpage:
        page = process_posts(page)
        pagecount += 1
    finish_thread(t)
    
def get_pstring():
    return "%d-%d-%d-%d" % (maxpages_forum, maxpages_thread, subforum_threads, thread_threads)

def save_state(done):
    all_values = {}
    all_values['thread_count'] = thread_count
    all_values['threads_done'] = threads_done
    all_values['threads_left'] = threads_left
    all_values['subforums'] = subforums
    all_values['user_postcount'] = user_postcount
    all_values['year_postcount'] = year_postcount
    all_values['mont_postcount'] = mont_postcount
    all_values['done'] = done
    save_path = get_pstring() + "-pc-state.p"
    pickle.dump(all_values, open(save_path, 'wb'))

def read_state():
    global thread_count, threads_done, threads_left, subforums, user_postcount, year_postcount, mont_postcount
    try:
        save_path = get_pstring() + "-pc-state.p"
        all_values = pickle.load(open(save_path, 'rb'))
        thread_count = all_values['thread_count']
        threads_done = all_values['threads_done']
        threads_left = all_values['threads_left']
        subforums = all_values['subforums']
        user_postcount = all_values['user_postcount']
        year_postcount = all_values['year_postcount']
        mont_postcount = all_values['mont_postcount']
        print all_values['done']
        return all_values['done']
    except:
        pass
    return False

def main():
    global threads_start
    # check for current state
    if not read_state():
        start = time.time()
        soup = BeautifulSoup(requests.get(base + dn).text, 'lxml')
        for f in soup.find_all('h2', class_="forumtitle"):
            subforums.add(f.find('a').get('href'))
        print "time to get subforums: ", time.time() - start

        # make threads to go through subforums
        start = time.time()
        s_threads = []
        for i in xrange(subforum_threads):
            s_threads.append(subforumThread(i))
        for t in s_threads:
            t.start()
        for t in s_threads:
            t.join()
        print "time to get threads from subforums: ", time.time() - start
        print "threads left: " + str(len(threads_left))

        # make threads to go through threads
        threads_start = time.time()
        t_threads = []
        for i in xrange(thread_threads):
            t_threads.append(threadThread(i))
        for t in t_threads:
            t.start()
        for t in t_threads:
            t.join()
        print "time to process " + str(threads_processed) + " threads: ", time.time() - threads_start
        all_done = True
        save_state(True)

    write_results()


cutoffs = [1, 10, 100, 1000, 10000, 100000, 1000000]
def find_post_index(n):
    i = 0
    while n > cutoffs[i]:
        i += 1
    return i

def write_results():
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

        
    results = open(get_pstring() + "-pc-results.txt", 'w')
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
        
if __name__ == "__main__":
    main()
