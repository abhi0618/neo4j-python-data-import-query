from pickle import TRUE
from neo4j import GraphDatabase
from numpy import true_divide
from tabulate import tabulate
uri = "neo4j://localhost:7687"
driver = GraphDatabase.driver(uri)

def search(tx, keyword):
    results = []
    result = tx.run("""
    CALL db.index.fulltext.queryNodes("keywordAndPaperAndAuthor", "keyword") YIELD node, score
OPTIONAL MATCH ak=(node)-[:Auther_Keyword]->(p:paper)
WITH node,ak
OPTIONAL MATCH ap=(node)-[:Written]->(p:paper)
WITH node,ak,ap
OPTIONAL MATCH pp=(node) where ["paper"] in  LABELS(node)
WITH COLLECT([node in nodes(ak)  WHERE  node:paper])+COLLECT([node in nodes(ap)  WHERE  node:paper])+COLLECT([node in nodes(pp)  WHERE  node:paper]) AS output
UNWIND apoc.coll.flatten(output) as onnode
MATCH (onnode)-[:Published_in]->(j:journal)
MATCH (onnode)<-[:Written]-(a:author)
MATCH (onnode)<-[:Reference_Paper]-(rp:referencePaper)
RETURN onnode.title as title,j.name as journal,a.name as author,rp.title as referencePaper 
    """, keyword=keyword)
    for record in result:
        results.append(record)
    return results

print("Please enter search keyword and press enter")
input = input()
with driver.session() as session:
    search_results = session.read_transaction(search, input)
    print(tabulate(search_results))
# for item in search_results:
#     print(item)

driver.close()