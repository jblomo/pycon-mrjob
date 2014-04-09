from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol

import re

# Use this regular expresion to break up text via findall()
WORD_RE = re.compile(r"[\w']+")

class UniqueReview(MRJob):
    INPUT_PROTOCOL = JSONValueProtocol

    def extract_words(self, _, record):
        """Take in a record, filter by type=review, yield <word, review_id>
        records look like:
        {"votes": {"funny": 3, "useful": 3, "cool": 2},
         "user_id": "dYtkphUrU7S2_bjif6k2uA",
         "review_id": "gjtWdiEMMfoOTCfdd3hPmA",
         "stars": 4,
         "date": "2009-04-24",
         "text": "Man, if these guys were trying to replicate a gringo bar in Mexico...",
         "type": "review",
         "business_id": "RqbSeoeqXTwts5pfhw7nJg"}
        """
        if record['type'] == 'review':
            ###
            # TODO: for each word in the review, yield the correct key,value
            # pair:
            # for word in ____:
            #   yield [ ___ , ___ ]
            ##/

    def count_reviews(self, word, review_ids):
        """Count the number of reviews a word has appeared in.  If it is a
        unique word (ie it has only been used in 1 review), output that review
        and 1 (the number of words that were unique)."""

        unique_reviews = set(review_ids)  # set() uniques an iterator
        ###
        # TODO: yield the correct pair when the desired condition is met:
        # if ___:
        #     yield [ ___ , ___ ]
        ##/

    def count_unique_words(self, review_id, unique_word_counts):
        """Output the number of unique words for a given review_id"""
        ###
        # TODO: summarize unique_word_counts and output the result
        # 
        ##/

    def steps(self):
        """TODO: Document what you expect each mapper and reducer to produce:
        mapper1: <line, record> => <key, value>
        reducer1: <key, [values]>
        mapper2: ...
        """
        return [self.mr(mapper=self.extract_words, reducer=self.count_reviews),
                self.mr(reducer=self.count_unique_words)]

if __name__ == '__main__':
    UniqueReview.run()
