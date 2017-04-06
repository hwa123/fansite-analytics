# your Python code to implement the features could be placed here
# note that you may use any language, there is no preference towards Python
import sys
import os
import operator
import re
from collections import deque, defaultdict
from datetime import datetime, timedelta, time


def reading_data(filepath):
    with open(filepath, "rb") as data:
        for row in data:
            yield row


def writing_data(filepath, data, feature_type=None):
    if feature_type in ['hosts', 'hours']:
        with open(filepath, "wb") as writer:
            for k, v in data:
                writer.write("{},{}\n".format(k, v))


    if feature_type == 'resources':
        with open(filepath, "wb") as writer:
            for k, v in data:
                writer.write("%s\n" % k)

    if feature_type == 'blocks':
        with open(filepath, "ab") as writer:
            writer.write("%s" % data)



def checkblocks(url, res=set()):
    activity_map = sorted(floodlog[url].iteritems())
    failed_attempts = defaultdict(list)
    blocked = False
    thres = datetime.utcfromtimestamp(0)

    for idx, (k, v) in enumerate(activity_map):

        if k > thres and not failed_attempts[url]:
            blocked = False

        if blocked and k <= thres:
            res.add((url, k))

        if k > thres:
            if v != '200':
                failed_attempts[url].append((k, idx))
                if len(failed_attempts[url]) >= 3:
                    earliest_time, earliest_idx = failed_attempts[url].pop(0)
                    timegap = k - earliest_time
                    # 3 failed attempts in a row within a 20 seconds window
                    if idx - earliest_idx == 2 and timegap.seconds < 20:
                        thres = failed_attempts[url][-1][0] + \
                            timedelta(seconds=300)
                        blocked = True
                    else:
                        # leave only the lastest failed attempt
                        failed_attempts[url].pop(0)
        else:
            # reset current attempt
            for _ in range(len(failed_attempts[url])):
                failed_attempts[url].pop()
    return res


def main(logpath, *outputpath):
    for idx, rawrow in enumerate(reading_data(logpath)):
        row = re.findall(r'(?:"[^"]*"|[^[]*]|[^\s"])+', rawrow)
        host, t, response, bts = row[0], row[3], row[5], row[-1]
        request_type, url = row[4][1:-1].split()[:2]

        # Feature 1:
        # descending order the top 10 most active hosts/IP addresses
        hosts[host] += 1

        # Feature 2:
        # top 10 resources on the site that consume the most bandwidth
        if not bts.isdigit():
            bts = '0'
        if url in resources:
            cnt, total_bts = resources[url].pop()
            cnt += 1
            total_bts += int(bts)
            resources[url].append((cnt, total_bts))
        else:
            resources[url].append((1, int(bts)))

        # Feature 3:
        # site's 10 busiest (i.e. most frequently visited) 60-minute period
        dt = datetime.strptime(t[1:-7], '%d/%b/%Y:%H:%M:%S')
        dt_floor = dt - timedelta(seconds=3600)
        if dt not in datedata:
            datedata[dt] = (0, 0)
        for k, _ in datedata.iteritems():
            freq, cnt = datedata[k]
            if k >= dt_floor:
                freq += 1
                # datedata[k] += 1
            if k == dt:
                cnt += 1
            datedata[k] = (freq, cnt)

        # Feature 4:
        # insert records into block list if an IP has 3 failed logins in a row,
        # within a 20-second window
        hostkey = (host, url)
        tvalue = floodlog[hostkey]
        tvalue[dt] = response

        # skip any hostkey pair with log records less than 3, because it won't
        # fit block condition anyway
        if len(floodlog[hostkey]) > 3:
            if (hostkey, dt) in checkblocks(hostkey):
                writing_data(outputpath[-1], rawrow, 'blocks')

    # feature 1 results
    hosts_output = sorted(
        hosts.iteritems(), key=operator.itemgetter(1), reverse=True)[:10]
    writing_data(outputpath[0], hosts_output, 'hosts')

    # feature 2 results
    resources_output = sorted(resources.iteritems(),
                              key=operator.itemgetter(1), reverse=True)[:10]
    writing_data(outputpath[1], resources_output, 'resources')

    # feature 3 results
    max_dt = max(datedata.iteritems(), key=operator.itemgetter(1))[0]
    hours_output = set()

    # suppose the top buiest 60 mins window occurs after top frequent one
    # within 10s window.
    window = timedelta(seconds=10)
    tmp_max_dt = max_dt

    for i in range(10):
        new_dt = max_dt + timedelta(seconds=i)
        if new_dt not in datedata:
            datedata[new_dt] = (datedata[tmp_max_dt][0] -
                                datedata[tmp_max_dt][1], 0)
        else:
            tmp_max_dt = new_dt

        # append other times if frequency happens to be same
        for i in datedata.keys():
            if datedata[i][0] == datedata[new_dt][0]:
                hours_output.add(
                    (new_dt.strftime('%d/%b/%Y:%H:%M:%S') + ' -0004', datedata[i][0]))
                if len(hours_output) > 9:
                    break

    writing_data(outputpath[2], sorted(list(hours_output)), 'hours')


if __name__ == "__main__":

    if len(sys.argv) != 6:
    	print "usage: python ./src/process_log.py ./log_input/log.txt ./log_output/hosts.txt ./log_output/hours.txt ./log_output/resources.txt ./log_output/blocked.txt"
 		exit(0)

 	logpath = sys.argv[1]
 	hostspath = sys.argv[2]
 	resourcespath = sys.argv[3]
 	hourspath = sys.argv[4]
 	blockspath = sys.argv[5]

    hosts = defaultdict(int)
    resources = defaultdict(list)
    datedata = defaultdict(tuple)
    floodlog = defaultdict(dict)

    main(logpath, hostspath, resourcespath, hourspath, blockspath)

    exit(0)
