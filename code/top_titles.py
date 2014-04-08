"""Run an MRJob to generate the counts for all titles, then sort the results
and show the top 10.
"""

from count_titles import CountTitles

mr_job = CountTitles(args=['-r', 'local', 'msanon/anonymous-msweb.data.gz'])
with mr_job.make_runner() as runner:
    runner.run()
    title_counts = [mr_job.parse_output_line(line) for line in
            runner.stream_output()]
    results = sorted(title_counts, key=lambda (k,v): v, reverse=True)
    print results[:10]
