#!/usr/bin/env python
import ConfigParser as cp
import json
import pydot as pd
from itertools import product
import sys

edge_fn = sys.argv[1]
vertex_fn = sys.argv[2]

creader= cp.SafeConfigParser()
creader.read(edge_fn)
node_in_config = dict(
    [ (s.split(":")[1], int(creader.get(s, "numprocesses"))) for s
    in creader.sections() if s.startswith("watcher") ]
)
vertex_in_json = json.load(open(vertex_fn))
#del( vertex_in_json["db"])

node = {0}^{0}
vertex = [  v.split("_")  for v in vertex_in_json["cnx"]  ]
for el,n in node_in_config.items():
    if n>1:
        node ^= {el}
        node |= set([ "%s_%d" %(el, i) for i in range(1,n+1) ])
    else:
        node |= {el}
print "nodes"
print node
#node |= {"any"}
node = set(node)
graph = pd.Dot(graph_type='digraph', 
    rankdir = "LR",
    labelloc='b',
    labeljust='t', ranksep=.5,
    size="18x10",
    label = "DSAT connexion graph",
    fontsize = 14,

)
node_defaults =  dict(shape = "rectangle", style="filled",fixedsize=True,
   width=2,     fillcolor= "lightgray")
for v in vertex:
    vert = [] + v
    for ind in [1,2]:
        print "%r => %r" %  (vert[ind] ,{vert[ind]} & node)
        if { vert[ind] } not in  node: #node.index( vert[ind] ):
            if  vert[ind]  not in node_in_config:
                print("EXTERNAL or ANY")
                vert[ind] = [  v[ind]  ]
                #graph.add_node(pd.Node(vert[ind], **node_defaults))
            else:
                print "PROD"
                print vert[ind]
                nproc = node_in_config[v[ind]]
                if nproc - 1:
                    node ^= { vert[ind] }
                    vert[ind] =  [ "%s_%d" %(v[ind], i) for i in range(1,nproc+1) ]
                    for n in vert[ind]:
                        graph.add_node(pd.Node(n, **node_defaults))

                else:
                    this = node_defaults.copy()
                    this["fillcolor"] = "lightblue"
                    graph.add_node(pd.Node(vert[ind], **this))
                    vert[ind] = [  v[ind]  ]
                    #pass
        else:
            print "KKK"
            print vert[ind]
    print "V%r, v%r" %( vert, v)
    for src, dst in product(vert[1],vert[2]):
        print "%r // % r l:%r" % (src , dst, vert[0])
        graph.add_edge(pd.Edge( src,dst, arrowhead="normal", label = vert[0]))


graph.write_dot("out.dot")
graph.write_jpeg("out.jpeg")
