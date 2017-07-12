import sys
import pstats
p = pstats.Stats(sys.argv[1] + '.profile')
p.sort_stats(sys.argv[2]).print_stats()
