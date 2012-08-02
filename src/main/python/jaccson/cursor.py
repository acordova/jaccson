

class Cursor(object):

    def __init__(self, label, conn):
        self.label = label
        self.conn = conn
        self.batch = []
        self.done = False
    
    def __iter__(self):
        return self
        
    def next(self):
        
        if self.done:
            raise StopIteration()

        if len(self.batch) == 0:
            self.batch = self.conn.nextBatch(self.label)
            
            if len(self.batch) == 0:
                self.done = True
                raise StopIteration()

        s = self.batch[0]
        del self.batch[0]

        jo = json.loads(s)
        return jo
