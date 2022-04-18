# %%


# %%
import xxhash

# %%
import pandas as pd
dfs = pd.read_excel('data.xlsx',sheet_name="Sheet1")

# %%
from neo4j import GraphDatabase

uri = "neo4j://localhost:7687"
driver = GraphDatabase.driver(uri)

# %%
def create_authors(tx, authors):
    for name in  authors.split(','):
        tx.run("MERGE(:author {name: $name})",
            name=name)

# %%
def create_keywords(tx, keywords):
    for keyword in  keywords.split(';'):
        tx.run("MERGE(:keyword {name: $name})",
            name=keyword)

# %%
def create_journal(tx, name):
    tx.run("MERGE(:journal {name: $name})",
            name=name)

# %%
def create_paper(tx,id, title,year,doi,link,abstract):
    tx.run("""
        MERGE(p:paper{id:$id})
                ON CREATE
                set  p.id=$id,
                p.title=$title,
                p.year=$year,
                    p.doi=$doi,
                    p.link=$link,
                    p.abstract=$abstract  
                ON MATCH 
                set  
                p.title=$title,
                p.year=$year,
                    p.doi=$doi,
                    p.link=$link,
                    p.abstract=$abstract
                    RETURN p
    """,
        id=id,title=title,year=year,doi=doi,link=link,abstract=abstract)

# %%
def create_references(tx,id, reference):
    tx.run("""
        MERGE(p:referencePaper{id:$id})
                ON CREATE
                set  p.id=$id,
                        p.title=$reference
                ON MATCH 
                set  
                   p.title=$reference
                RETURN p
    """,
        id=id,reference=reference)

# %%
def create_paper_to_auther_relation(tx,id, authors):
     for author in  authors.split(','):
        tx.run("""MATCH(p:paper {id: $id})
                        MATCH(a:author{name:$author})
                        MERGE  (p)<-[:Written]-(a)  
                    """,id=id,author=author)

# %%
def create_paper_to_keyword_relation(tx,id, keywords):
     for keyword in  keywords.split(';'):
        tx.run("""MATCH(p:paper {id: $id})
                        MATCH(k:keyword{name:$keyword})
                        MERGE  (p)<-[:Auther_Keyword]-(k)  
                    """,id=id,keyword=keyword)

# %%
def create_paper_to_published_relation(tx,id, journal):
    tx.run("""MATCH(p:paper {id: $id})
                    MATCH(j:journal{name:$journal})
                    MERGE  (p)-[:Published_in]->(j)  
                """,id=id,journal=journal)

# %%

def create_paper_to_reference_paper_relation(tx,id, id_references):
    tx.run("""MATCH(p:paper {id: $id})
                    MATCH(rp:referencePaper{id:$id_references})
                    MERGE  (p)<-[:Reference_Paper]-(rp)  
                """,id=id,id_references=id_references)

# %%
with driver.session() as session:
    for indices, row in dfs.iterrows():
        authors =row["Authors"]
        title =str(row["Title"])
        year =row["Year"]
        doi =row["DOI"]
        cited_by =row["Cited by"]
        source_title =row["Source title"]
        link =row["Link"]
        abstract =row["Abstract"]
        author_keywords=str( row["Author Keywords"])  if row["Author Keywords"] is not None else "" 
        references=str(row["References"])
        id=xxhash.xxh64(title).hexdigest()
        id_references=xxhash.xxh64(references).hexdigest()
        # Nodes
        session.write_transaction(create_authors, authors)
        session.write_transaction(create_journal, source_title)
        session.write_transaction(create_keywords, author_keywords)
        session.write_transaction(create_paper, id,title,year,doi,link,abstract)
        session.write_transaction(create_references, id_references,references)

        # Relations
        session.write_transaction(create_paper_to_auther_relation, id,authors)
        session.write_transaction(create_paper_to_keyword_relation, id,author_keywords)
        session.write_transaction(create_paper_to_published_relation, id,source_title)
        session.write_transaction(create_paper_to_reference_paper_relation, id,id_references)
        
        
driver.close()


