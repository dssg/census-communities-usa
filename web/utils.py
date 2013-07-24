import os

class QueryError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

class Query(object):
    def __init__(self, coll):
        """ Expects a pymongo.collection to initialize """
        self.coll = coll

    def filter(self, limit, **kwargs):
        """ 
        `limit` is passed into query to limit the number of results returned
        Keyword arguments take the form of `field__filter` where `filter` is optional
        """
        query = {}
        for field_filter, value in kwargs.items():
            fil_name, field = self._validate_query({field_filter: value})
            if fil_name:
                query.update({field: {'$%s' % fil_name: value}})
            else:
                query.update({field: value})
        results = [r for r in self.coll.find(query, limit=limit)]
        return results
        
    def _validate_query(self, query):
        """ 
        Validates the values against the filters to make sure that they make sense.
        This could be extended to validate the fields as well.
        """
        if len(query.keys()) == 0:
            raise QueryError('Need at least one query, please')
        try:
            field, fil_name = query.keys()[0].split('__')
        except ValueError:
            field = query.keys()[0]
            fil_name = None
        query_value = query.values()[0]
        if fil_name and fil_name not in ['lte', 'gte', 'lt', 'gt', 'in', 'nin', 'ne']:
            raise QueryError('%s is not a valid filter' % fil_name)
        elif fil_name and fil_name in ['lte', 'gte', 'lt', 'gt'] and type(query_value) not in [int, datetime]:
            raise QueryError('Queries using "%s" as a filter must provide either an integer or a datetime as the query value' % fil_name)
        elif fil_name and fil_name in ['in', 'nin'] and type(query_value) != list:
            raise QueryError('Queries using "%s" as a filter must provide a list as the query value' % fil_name)
        return fil_name, field

