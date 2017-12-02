from tempfile import mkstemp, mktemp

from graphviz import Source
import time

SOURCE = """
digraph ladder { ranksep=".1"; nodesep=".1";

# Define the defaults
  node [shape=point fontsize=10]
  edge [dir=none fontsize=10]

# Column labels
  a [shape=none]
  b [shape=none]
  c [shape=none]
  d [shape=none]

# Draw the 4 column headings, no line
  { rank=same; edge[style=invis] a -> b -> c -> d   }

# Draw the columns
  node [style=invis]
  a -> a1 [style=invis]
  b -> b1 [style=invis]
  c -> c1 [style=invis]
  d -> d1 [style=invis]
  a1 -> a2 -> a3 -> a4 [style=dotted weight=1000 label="   "]
  b1 -> b2 -> b3 -> b4 [style=dotted weight=1000 label="   "]
  c1 -> c2 -> c3 -> c4 [style=dotted weight=1000 label="   "]
  d1 -> d2 -> d3 -> d4 [style=dotted weight=1000 label="   "]

# Now each step in the ladder
  { rank=same; a1 -> b1 [dir=forward label="Flow1"]  }
  { rank=same; b2 -> c2 [dir=forward label="Flow2"]  }
  {
    rank=same;
    b3 -> c3 [dir=none]
    c3 -> d3 [dir=forward label="Flow3"]
  }
  { rank=same; c4 -> d4 [dir=back label="Flow4"]  }

}

"""

def visualize(source):
    tmp = mktemp(suffix='.png')
    src = Source(source)

    # src.save('test-output/holy-grenade.png')
    src.view(tmp)

    time.sleep(1)


def main():
    visualize(SOURCE)


if __name__ == '__main__':
    main()
