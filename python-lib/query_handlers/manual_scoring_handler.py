from query_handlers import ScoringHandler


class ManualScoringHandler(ScoringHandler):
    def __init__(self):
        super().__init__(**kwargs)