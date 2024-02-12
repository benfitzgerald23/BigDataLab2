#https://github.com/astan54321/PA3/blob/44628868dcc7f00feec9e4c4bdb9391558391ac7/problem2_3.py

from mrjob.job import MRJob
from mrjob.step import MRStep
import re

DATA_RE = re.compile(r"[\w.-]+")


class MRProb2_3(MRJob):


    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_alc,
                   reducer=self.reducer_get_avg)
        ]

    def mapper_get_alc(self, _, line): 
        
        data = DATA_RE.findall(line)
        wine_class = int(data[0])  
        alcohol_content = float(data[1]) 
        if wine_class == 3:
            yield ("alcohol_content", alcohol_content)

    def reducer_get_avg(self, key, values):
        
        size, total = 0, 0
        for val in values:
            size += 1
            total += val
        yield ("average alchohol content of class 3", round(total,1) / size)
if __name__ == '__main__':
    MRProb2_3.run()
