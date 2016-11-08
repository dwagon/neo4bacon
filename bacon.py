#!/usr/bin/env python

from neo4j.v1 import GraphDatabase, basic_auth
from imdb import IMDb
import sys
import time
import pickle
import os
import random

stats = {'actors': 0, 'movies': 0, 'links': 0}


###############################################################################
class Parent(object):
    ###########################################################################
    def sanitise(self, s):
        s = s.replace("'", "")
        return s

    ###########################################################################
    def __getitem__(self, key):
        try:
            return self.data[key]
        except KeyError:
            sys.stderr.write("Unknown value for %s\n" % key)
            return ''

    ###########################################################################
    def __contains__(self, key):
        return key in self.data

    ###########################################################################
    def save(self):
        try:
            os.makedirs(self.__class__.__name__)
        except OSError:
            pass
        idpath = os.path.join(self.__class__.__name__, self.id)
        f = open(idpath, 'w')
        pickle.dump(self.data, f)
        f.close()
        namepath = os.path.join(self.__class__.__name__, self.name)
        try:
            os.symlink(self.id, namepath)
        except OSError:
            pass

    ###########################################################################
    def load(self, **kwargs):
        if 'name' in kwargs:
            path = os.path.join(self.__class__.__name__, kwargs['name'])
        elif 'id' in kwargs:
            path = os.path.join(self.__class__.__name__, kwargs['id'])
        if os.path.exists():
            f = open(path)
            self.data = pickle.load(f)
            f.close()
            return True
        return False


###############################################################################
class Person(Parent):
    def __init__(self, **kwargs):
        self.ia = IMDb()
        self.data = {}
        if 'name' in kwargs:
            act = self.getActorByName(kwargs['name'])
            self.id = act.getID()
        if 'id' in kwargs:
            self.id = kwargs['id']
        self.data = {i[0]: i[1] for i in self.getActorById(self.id).items()}
        self.name = self['name']
        self.graph()

    ######################################################################
    def getActorByName(self, name=None):
        sys.stderr.write("search_person(%s)\n" % name)
        start = time.time()
        search = self.ia.search_person(name)
        end = time.time()
        sys.stderr.write(" in %fsec (%d choices)\n" % (end-start, len(search)))
        return search[0]

    ######################################################################
    def getActorById(self, id):
        start = time.time()
        sys.stderr.write("get_person(%s)" % id)
        results = self.ia.get_person(id)
        end = time.time()
        sys.stderr.write(" in %fsec = %s\n" % (end-start, results['name']))
        return results

    ######################################################################
    def graph(self):
        session.run("MERGE (a:Actor {name:'%s', id:'%s'})" % (self.sanitise(self['name']), self.id))
        stats['actors'] += 1

    ######################################################################
    def add_role(self, rel, *args):
        roles = []
        for rtype in args:
            if rtype in self:
                roles.extend(self[rtype])

        for mov in roles:
            session.run("MERGE (m:Movie {name:'%s', id:'%s'})" % (self.sanitise(mov['title']), mov.movieID))
            stats['movies'] += 1
            cmd = "MATCH (a:Actor),(m:Movie) WHERE a.id = '%s' AND m.id = '%s' MERGE (a)-[r:%s]->(m) RETURN *" % (self.id, mov.movieID, rel)
            session.run(cmd)
            stats['links'] += 1
            sys.stderr.write("Linking %s-[%s]->%s\n" % (self.name, rel, mov['title']))

    ######################################################################
    def add_filmography(self):
        self.add_role('ACTED_IN', 'actor', 'actress')
        self.add_role('WROTE', 'writer movie', 'writer tv')
        self.add_role('PRODUCED', 'producer movie', 'producer tv')
        session.run('MATCH (a:Actor) WHERE a.id="%s" SET a.finished=true' % self.id)


###############################################################################
class Movie(Parent):
    def __init__(self, **kwargs):
        self.ia = IMDb()
        self.data = {}
        if 'name' in kwargs:
            act = self.getMovieByName(kwargs['name'])
            self.id = act.getID()
        if 'id' in kwargs:
            self.id = kwargs['id']
        self.data = {i[0]: i[1] for i in self.getMovieById(self.id).items()}
        self.name = self['title']
        self.graph()

    ######################################################################
    def getMovieByName(self, name=None):
        sys.stderr.write("search_movie(%s)" % name)
        start = time.time()
        search = self.ia.search_movie(name)
        end = time.time()
        sys.stderr.write(" in %fsec\n" % (end-start))
        return search[0]

    ######################################################################
    def getMovieById(self, id):
        sys.stderr.write("get_movie(%s)" % id)
        start = time.time()
        results = self.ia.get_movie(id)
        end = time.time()
        sys.stderr.write(" in %fsec = %s\n" % (end-start, results['title']))
        return results

    ######################################################################
    def graph(self):
        session.run("MERGE (m:Movie {name:'%s', id:'%s'})" % (self.sanitise(self['title']), self.id))
        stats['movies'] += 1

    ######################################################################
    def add_role(self, rel, *args):
        roles = []
        for rtype in args:
            if rtype in self:
                roles.extend(self[rtype])

        for act in roles:
            session.run("MERGE (a:Actor {name:'%s', id:'%s'})" % (self.sanitise(act['name']), act.personID))
            stats['actors'] += 1
            cmd = "MATCH (a:Actor), (m:Movie) WHERE a.id = '%s' AND m.id = '%s' MERGE (a)-[r:%s]->(m) RETURN *" % (act.personID, self.id, rel)
            results = session.run(cmd)
            stats['links'] += 1
            res = results.peek()
            sys.stderr.write("%s %s %s\n" % (res['a']['name'], rel, res['m']['name']))

    ######################################################################
    def add_cast_crew(self):
        self.add_role('ACTED_IN', 'cast')
        self.add_role('DIRECTED', 'director')
        self.add_role('WROTE', 'writer')
        self.add_role('PRODUCED', 'producer')
        self.add_role('CREATED', 'creator')
        session.run('MATCH (m:Movie) WHERE m.id="%s" SET m.finished=true' % self.id)


###############################################################################
def getSession():
    driver = GraphDatabase.driver("bolt://localhost", auth=basic_auth("neo4j", "admin"))
    session = driver.session()
    return session


###############################################################################
def getMovies(session, num=10):
    results = list(session.run("MATCH (m:Movie) WHERE m.finished IS NULL RETURN m"))
    sys.stderr.write("Pulling %d out of %d movies\n" % (num, len(results)))
    random.shuffle(results)
    for movie in results[:num]:
        sys.stderr.write("Adding cast to %s\n" % movie['m']['name'])
        m = Movie(id=movie['m']['id'])
        m.add_cast_crew()
        print_stats()


###############################################################################
def getPeople(session, num=10):
    results = list(session.run("MATCH (a:Actor) WHERE a.finished IS NULL RETURN a"))
    sys.stderr.write("Pulling %d out of %d people\n" % (num, len(results)))
    random.shuffle(results)
    for person in results[:num]:
        sys.stderr.write("Adding jobs to %s\n" % person['a']['name'])
        p = Person(id=person['a']['id'])
        p.add_filmography()
        print_stats()


###############################################################################
def print_stats():
    sys.stderr.write("Movies %(movies)d Actors %(actors)d Links %(links)d\n" % stats)


###############################################################################
def main(chase, option):
    global session
    session = getSession()
    if chase is None:
        while(True):
            getMovies(session, num=5)
            getPeople(session, num=5)
    else:
        sys.stderr.write("Chasing %s\n" % chase)
        if option == 'Person':
            try:
                int(chase)
            except ValueError:
                p = Person(name=chase)
            else:
                p = Person(id=chase)
            p.add_filmography()
        if option == 'Movie':
            try:
                int(chase)
            except ValueError:
                m = Movie(name=chase)
            else:
                m = Movie(id=chase)
            m.add_cast_crew()
    session.close()


###############################################################################
if __name__ == "__main__":
    chase = None
    option = None
    if len(sys.argv) > 1:
        if sys.argv[1] == "-m":
            option = 'Movie'
        elif sys.argv[1] == "-p":
            option = 'Person'
        chase = " ".join(sys.argv[2:])
    main(chase, option)

# EOF
