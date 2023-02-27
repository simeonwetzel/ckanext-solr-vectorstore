import pysolr
import six
from ckan.common import asbool
from werkzeug.datastructures import MultiDict
from ckan.lib.search import PackageSearchQuery

import ckan.logic as logic
import ckan.model as model
import logging
from ckan.common import config
#from ckan.lib.search import PackageSearchQuery
from ckan.lib.search.common import (
    make_connection, SearchError, SearchQueryError
)

from ckan.lib.search.query import solr_literal, SearchQuery

log = logging.getLogger(__name__)

_open_licenses = None

VALID_SOLR_PARAMETERS = set([
    'q', 'fl', 'fq', 'rows', 'sort', 'start', 'wt', 'qf', 'bf', 'boost',
    'facet', 'facet.mincount', 'facet.limit', 'facet.field',
    'extras', 'fq_list', 'tie', 'defType', 'mm', 'df'
])

def custom_query_for(model, **kwargs):
    return CustomSearchQuery()

class CustomSearchQuery(PackageSearchQuery):
    def run(self, query, permission_labels=None, **kwargs):
        '''
        Performs a dataset search using the given query.

        :param query: dictionary with keys like: q, fq, sort, rows, facet
        :type query: dict
        :param permission_labels: filter results to those that include at
            least one of these labels. None to not filter (return everything)
        :type permission_labels: list of unicode strings; or None

        :returns: dictionary with keys results and count

        May raise SearchQueryError or SearchError.
        '''
        assert isinstance(query, (dict, MultiDict))
        # check that query keys are valid
        if not set(query.keys()) <= VALID_SOLR_PARAMETERS:
            invalid_params = [s for s in set(query.keys()) - VALID_SOLR_PARAMETERS]
            raise SearchQueryError("Invalid search parameters: %s" % invalid_params)

        # default query is to return all documents
        q = query.get('q')
        if not q or q == '""' or q == "''":
            query['q'] = "*:*"

        # number of results
        rows_to_return = int(query.get('rows', 10))
    
        # query['rows'] should be a defaulted int, due to schema, but make
        # certain, for legacy tests
        if rows_to_return > 0:
            # #1683 Work around problem of last result being out of order
            #       in SOLR 1.4
            rows_to_query = rows_to_return + 1
        else:
            rows_to_query = rows_to_return
        #query['rows'] = rows_to_query
        query['rows'] = 1000


        fq = []
        if 'fq' in query:
            # log.debug("WIR SIND IN DER SEARCH-FILE")
            # log.warn(query['fq'])
            fq.append(query['fq'])
        fq.extend(query.get('fq_list', []))

        # show only results from this CKAN instance
        fq.append('+site_id:%s' % solr_literal(config.get('ckan.site_id')))

        # filter for package status
        if not '+state:' in query.get('fq', ''):
            fq.append('+state:active')

        # only return things we should be able to see
        if permission_labels is not None:
            fq.append('+permission_labels:(%s)' % ' OR '.join(
                solr_literal(p) for p in permission_labels))
        query['fq'] = fq

        # faceting
        query['facet'] = query.get('facet', 'true')
        query['facet.limit'] = query.get('facet.limit', config.get('search.facets.limit', '50'))
        query['facet.mincount'] = query.get('facet.mincount', 1)

        # return the package ID and search scores
        # Add score field to results
        query['fl'] = '{},score'.format(query['fl'])
        # return results as json encoded string
        query['wt'] = query.get('wt', 'json')

        # If the query has a colon in it then consider it a fielded search and do use dismax.
        #defType = query.get('defType', 'dismax')
        #if ':' not in query['q'] or defType == 'edismax':
            # query['defType'] = defType
            # query['tie'] = query.get('tie', '0.1')
            # this minimum match is explained
            # http://wiki.apache.org/solr/DisMaxQParserPlugin#mm_.28Minimum_.27Should.27_Match.29
            # query['mm'] = query.get('mm', '2<-1 5<80%')
            # query['qf'] = query.get('qf', QUERY_FIELDS)

        query.setdefault("df", "text")
        query.setdefault("q.op", "AND")
        
        
        """
        try:
            if query['q'].startswith('{!'):
                raise SearchError('Local parameters are not supported.')
        except KeyError:
            pass
        """
        #query['min_score'] = query.get('min_score', 0.5)
        conn = make_connection(decode_dates=False)
        try:
            solr_response = conn.search(**query)
        except pysolr.SolrError as e:
            # Error with the sort parameter.  You see slightly different
            # error messages depending on whether the SOLR JSON comes back
            # or Jetty gets in the way converting it to HTML - not sure why
            #
            if e.args and isinstance(e.args[0], str):
                if "Can't determine a Sort Order" in e.args[0] or \
                        "Can't determine Sort Order" in e.args[0] or \
                        'Unknown sort order' in e.args[0]:
                    raise SearchQueryError('Invalid "sort" parameter')
            raise SearchError('SOLR returned an error running query: %r Error: %r' %
                                (query, e))
        #self.count = solr_response.hits
        self.results = solr_response.docs

        # filter out results that have a vector score below a certain threshold
        # TODO: find out best threshold        
        self.results = [r for r in self.results if r['score'] >= 0.6]
        self.count = len(self.results)
        # #1683 Filter out the last row that is sometimes out of order
        self.results = self.results[:rows_to_return]

        # get any extras and add to 'extras' dict
        for result in self.results:
            extra_keys = filter(lambda x: x.startswith('extras_'), result.keys())
            extras = {}
            for extra_key in list(extra_keys):
                value = result.pop(extra_key)
                extras[extra_key[len('extras_'):]] = value
            if extra_keys:
                result['extras'] = extras

        # if just fetching the id or name, return a list instead of a dict
        if query.get('fl') in ['id', 'name']:
            self.results = [r.get(query.get('fl')) for r in self.results]

        # get facets and convert facets list to a dict
        self.facets = solr_response.facets.get('facet_fields', {})
        for field, values in six.iteritems(self.facets):
            self.facets[field] = dict(zip(values[0::2], values[1::2]))
        
        return {'results': self.results, 'count': self.count}
