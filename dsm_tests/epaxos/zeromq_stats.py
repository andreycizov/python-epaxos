import sys
import pstats
p = pstats.Stats('1.profile')
p.sort_stats(sys.argv[1]).print_stats()
