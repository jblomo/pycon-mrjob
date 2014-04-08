"""Find the titles of the top visited Vroots.

This program will take a CSV data file and output lines of

    Vroot Title

To run:

    python top_titles.py anonymous-msweb.data
"""

from mrjob.job import MRJob
import csv

def csv_readline(line):
    """Given a sting CSV line, return a list of strings."""
    for row in csv.reader([line]):
        return row

class TopTitles(MRJob):

    def mapper(self, line_no, line):
        """Extracts the Vroot that was visited"""
        cell = csv_readline(line)
        if cell[0] == 'V':
            yield ### FILL IN
                  # How to "tag" this value for a given Key?
        elif cell[0] == 'A'
            yield ### FILL IN
                  # How to "tag" this value for a given Key?

    def reducer(self, vroot, visit_counts_and_title):
        """Sumarizes the visit counts by adding them together.  Extracts title,
        sums visit counts, returns both."""
        total = 0
        title = ''

        for value in visit_counts_and_title:
            if # FILL IN: value is a visit type:
                total += # FILL IN: extract untagged value
            elif # FILL IN: value is a attribute type:
                title = # FILL IN: extract untagged title

        yield title, total
        
if __name__ == '__main__':
    mr_job = TopTitles(args=['-r', 'local'])
    results = []
    with mr_job.make_runner() as runner:
        runner.run()
        title_counts = [mr_job.parse_output_line(line) for line in
                runner.stream_output()]
        results = sorted(title_counts, key=lambda (k,v): v)
    return results[:10]
