from query_handlers import QueryHandler


class ScoringHandler(QueryHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
