#!/usr/bin/python

"""Migrate indices from fast storage to slow storage, as they get older."""

__author__ = "Avleen Vig"
__copyright__ = "Copyright (C) Avleen Vig 2014"
__license__ = "GPL v2"
__version__ = "1.0"


import json
import sys
import requests



def main():
    """Main function"""

    # Some useful variables
    es_url = 'http://localhost:9200'

    # Indices 4 days old get moved to mid-term storage.
    # Indices 10 days old get moved to the slowest, largest storage.
    neulich_age = 4
    old_age = 10

    # Get the list of indexes
    es_stats = requests.get(es_url + '/_stats').json()

    # Get the indices which logstash uses
    ls_indices = [x for x in es_stats['indices'].keys()
                  if x.startswith('logstash-')]

    # Sort the list so oldest indices are first
    ls_indices.sort()

    # Some safety logic. If the number of indices is less than neulich_age, give
    # up.
    if len(ls_indices) < neulich_age:
        print 'Not enough indices to move to neulich. Exiting.'
        sys.exit()

    # Get the indices which should be neulich. Because of the way array math
    # works, we want to get all of the elements between old_age age neulich_age,
    # so we need to shift our range once to the right.
    # Eg, list[-1] is the last element, but list[-3:-1] returns the items at -3
    # and -2.
    # old_age-1 means we exclude the most recent index in the old tier.
    # neulich_age-1 means we include the oldest index on the recent tier.
    neulich_indices = ls_indices[-(old_age - 1):-(neulich_age - 1)]
    # Get all indices which should be old. Again, funny math here.
    # old_age-1 includes the oldest index from the mid tier
    old_indices = ls_indices[0:-(old_age - 1)]

    # Disable shard allocation temporarily. This stops the ES auto-rebalancing.
    print 'Disabling ES shard re-allocation'
    put_data = {"transient":
                 {"cluster.routing.allocation.disable_allocation": 'true'}
               }
    req = requests.put(es_url + '/_cluster/settings', data=json.dumps(put_data))
    print '    response: ' + req.text

    # Assign the indices to the correct tiers
    for index in neulich_indices:
        print 'Re-routing ' + index + ' to neulich tier'
        put_data = {"index.routing.allocation.require.tag": "neulich",
                     "index": {"number_of_replicas": 1}
                   }
        req = requests.put(es_url + '/' + index + '/_settings',
                           data=json.dumps(put_data))
        print '    response: ' + req.text

        # At this point, the index shouldn't change much (unless an unusual
        # backfill happens), so we should be safe to optimize and merge
        # segments.
        print 'Optimizing index ' + index
        req = requests.post(es_url + '/' + index + '/_optimize')
        print '    response: ' + req.text
        # TODO(avleen): Implement merging segments based on
        # http://www.elasticsearch.org/guide/reference/index-modules/merge/

    for index in old_indices:
        print 'Re-routing ' + index + ' to archive tier'
        put_data = {"index.routing.allocation.require.tag": "archive",
                     "index": {"number_of_replicas": 1}
                   }
        req = requests.put(es_url + '/' + index + '/_settings',
                           data=json.dumps(put_data))
        print '    response: ' + req.text

    # Re-enable the allocation routing, and let ES move the shards
    print 'Enabling ES shard re-allocation'
    put_data = {"transient":
                 {"cluster.routing.allocation.disable_allocation": 'false'}
               }
    req = requests.put(es_url + '/_cluster/settings', data=json.dumps(put_data))
    print '    response: ' + req.text

if __name__ == '__main__':
    main()
