class Context(object):
    def __init__(self, token, client, request_id):
        self.token = token
        self.client = client
        self.request_id = request_id
