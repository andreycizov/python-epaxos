import pstats
p = pstats.Stats('1.profile')
p.sort_stats('tottime').print_stats()
