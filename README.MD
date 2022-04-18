CREATE CONSTRAINT ON (n:author) ASSERT n.name IS UNIQUE;

CREATE CONSTRAINT ON (n:journal) ASSERT n.name IS UNIQUE;

CREATE CONSTRAINT ON (n:keyword) ASSERT n.name IS UNIQUE;

CREATE CONSTRAINT ON (n:paper) ASSERT n.id IS UNIQUE;

CREATE CONSTRAINT ON (n:referencePaper) ASSERT n.id IS UNIQUE;



CALL db.index.fulltext.queryNodes("keywordAndPaperAndAuthor", "Personality") YIELD node, score
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
RETURN onnode.title as title,j.name as journal,a.name as author,rp.title as referencePaper limit 150


call db.schema.visualization()

docker-compose up -d

remove 

docker-compose down