# coding=utf-8
import datetime
import time
import os
from threading import current_thread
from concurrent import futures
import requests
from scraperpractice.util import convert_to1970s, convert_from1970s
from retrying import retry


# TODO ERROR HADDING & CONTINUE DOWNLOAIDNIG
class MaoYan():
    '''
    A scrapy for ManYan movie comment , save the comment in file

    eg..
    maoyan = Maoyan(1111)
    maoyan.getcomment()
    '''

    def __init__(self, movieid, starttime, backendtime, peroidhour):
        '''
        init the MaoYan class , default movie id , url ,path , starttime,limit property
        :param movieid:
        :param starttime
        :param backendtime
        :param peroid how long of peroid to split the start and end time
        '''
        self.movieid = movieid
        self.onshowurl = 'http://m.maoyan.com/ajax/movieOnInfoList'
        self.movielist = []
        self.baseurl = 'http://m.maoyan.com/review/v2/comments.json?movieId={movieid}&offset={offset}&limit={limit}&type={type}&ts={ts}'
        self.folder = 'maoyan'
        self.filename = '{0}_comment{1}.txt'.format(self.movieid, datetime.datetime.now().strftime("%Y%m%d%H%M%S"))
        self.fullpath = os.path.join(self.folder, self.filename)
        self.starttime = convert_to1970s(starttime)
        self.backendtime = convert_to1970s(backendtime)
        self.peroidhour = peroidhour
        self.LIMIT = 30  # maxlimit = 30
        self.TYPE = 3  # not sure what is number of type stand for in url
        self.OFFSET = 0
        self.MAX_WORKER = 10
        print('save file in ', self.fullpath)

    def formaturl(self, movieid, offset, limit, type, st):
        '''
        format the maoyan comment url
        :param movieid: id of movie
        :param offset: offset , max is 1000
        :param limit: limit , max is 30
        :param type: always 3
        :param st: start time
        :return: formatted url
        '''
        return self.baseurl.format(movieid=movieid, offset=offset, limit=limit, type=type, ts=st)

    def getOninfolist(self):
        '''
        Get all the on-showing movie id
        :return: list of movie id
        '''
        resp = requests.get(self.onshowurl)
        assert resp.status_code == '200'
        return resp.json()['movieList']
        # print(resp.cookies)



    @retry(stop_max_attempt_number=5, wait_random_min=1000, wait_random_max=5000)
    def getComment(self, slicestart, slicebackend):
        '''
        get the comment from json file, in this format -> time|name|gender|comment|score|replycount

        logic is below:
            baseurl+offset+limit+start time
        offset has the limitation in 1000 , so you need to get the last start time which get from json file ,
        and assign it to start time, repeat it , until we the start time before slicebackend time or has-more
        value is equal to 0 or no comment return

        ALSO FIND ANOTHER LINK :
        http://m.maoyan.com/mmdb/comments/movie/344869.json?v=yes&offset=0&startTime=2019-02-12%2021%3A09%3A31

        ALMOST SAME LOGIC , IT HAS CITY VALUE , %2021%3A09%3A31 means " 21:09:31" , change it contiusely

        :param slicestart: slice begin time eg.2019-02-13 08:00:00
        :param slicebackend: slice back end time eg.. 2019-02-10 08:00:00
        :return:
        '''
        # TODO(me): error handler & retry if needed
        # assert self.endtime is not None and  self.starttime is not None and self.endtime < self.starttime

        offset = self.OFFSET
        limit = self.LIMIT
        st = slicestart
        totallength = 0

        # get the url, as api has limit on 1000 records,
        # we would set the st to min start time and reset the offset to 0 , when offset+limit > 1000
        try:
            while True:
                url = self.formaturl(self.movieid, offset, limit, self.TYPE, st)
                content = []
                resp = requests.get(url)
                json = resp.json()
                comments = json['data']['comments']
                # TODO(me): write more elegent for logging
                print("Thread:{0}|URL:{1}|{2} records ".format(current_thread().name, url, len(comments)))
                totallength += len(comments)
                # TODO(me): write more elegent in progress
                print('[' + '>' * int(30 * totallength / 18895),
                      '-' * (30 - int(30 * totallength / 18895)) + ']' + '%.2f' % (totallength / 18895 * 100) + '%')

                content += ["\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\"" % tuple(
                    map(lambda x: x and x or "", (convert_from1970s(comment['time']), comment['nick'], comment['gender'],
                        comment['content'], comment['score'], comment['replyCount']))
                ) for comment in comments]
                self.savecomment(content)
                # Using the implicit booleanness of the empty list is more pythonic
                # re-set offset logic
                if not comments:
                    break
                if offset + limit > 1000:
                    offset = 0
                    for comment in comments:
                        if comment['time'] < st:
                            st = comment['time']
                    if st < slicebackend:
                        print('pick data from {0} to {1} , which met the defined end time {2}'.format(
                            convert_from1970s(self.starttime), convert_from1970s(st),
                            convert_from1970s(self.backendtime)))
                        break
                    time.sleep(1)
                    continue
                offset += limit
                # in case hasmore element is false , break the loop
                if not resp.json()['paging']['hasMore']:
                    break
        except requests.HTTPError as exc:
            if resp.status_code == 404:
                print("404 error not found ")
            else:
                raise

    def savecomment(self, content):
        '''
        save comment in files
        :param content: data to be saved
        :return:
        '''
        with open(self.fullpath, 'a', encoding='utf-8') as f:
            f.writelines(map(lambda x: x.replace('\r', '').replace('\n', '').strip() + '\n', content))

    def createfolder(self):
        if not os.path.exists(self.folder):
            os.makedirs(self.folder)
            print('create folder on %s' % self.folder)
        # TODO(me): if clean folder function should be write here or not ????
        # if cleanfolder:
        #     for f in os.listdir(self.folder):
        #         print(f)
        #         os.remove(os.path.join(self.folder,f))

    def getcomment_many(self, slices):
        workers = min(len(slices), self.MAX_WORKER)
        with futures.ThreadPoolExecutor(workers) as executor:
            to_do = []
            for slicestart, slicebackend in slices:
                future = executor.submit(self.getComment, slicestart, slicebackend)
                to_do.append(future)
                print(slicestart, '-', slicebackend, future)
            result = []
            for future in futures.as_completed(to_do):
                res = future.result()
                print(future, res)

    def splitslice(self):
        if self.peroidhour == 0:
            return [(self.starttime, self.backendtime)]
        peroid_ms = self.peroidhour * 60 * 60 * 1000
        n = int((self.starttime - self.backendtime) / peroid_ms)
        slices = [(self.starttime - peroid_ms * (i - 1), self.starttime - peroid_ms * i) for i in range(1, n)]
        if (self.starttime - self.backendtime) % peroid_ms != 0:
            slices.append((self.starttime - n * peroid_ms, self.backendtime))
        return slices

    def main(self):
        begintime = datetime.datetime.now()
        self.createfolder()
        self.getcomment_many(self.splitslice())
        endtime = datetime.datetime.now()
        print('it take %s to finish the job !!' % (endtime - begintime))


if __name__ == '__main__':
    # TODO slice end time check !!!
    # TODO process line ...  better to change it into command line function 1.list on-showing id first 2.run command to get & save result

    maoyan = MaoYan(248906, datetime.datetime.now(), datetime.datetime(2019, 2, 5, 0, 0, 0), 4)
    maoyan.main()
    # slices = maoyan.splitslice()
    # for item in map(convert_from1970s,map(lambda x : x[0],slices)):
    #     print(item)
    # for item in map(convert_from1970s,map(lambda x : x[1],slices)):
    #     print(item)
